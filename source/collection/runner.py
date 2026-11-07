import asyncio
from itertools import count
from pathlib import Path
import re
import sqlite3
import time
from urllib.parse import urlsplit

from playwright.async_api import Error as BrowserError, async_playwright

from .capture import Capture
from .pacing import Pacer, PacingPause, stored_factor
from .protocol import Ledger
from .screening import profile_for, relevant_note
from .replay import chain_key, resume_seed
from .store import encode
from .request import navigation_url, request_url
from . import VERSION

ADAPTER = Path(__file__).resolve().parents[2] / "static" / "collection-page.js"
REQUEST_ADAPTER = ADAPTER.with_name("collection-request.js")


class Paused(Exception):
    pass


class Unavailable(Exception):
    pass


class PendingPage(Exception):
    """A page timeout or connection error; this post is retried after 30 s.

    Never used for refusals: any restriction stops the whole batch.
    """
    def __init__(self, reason):
        self.kind, self.until = 'page', time.time() + 30
        super().__init__(f'{reason}_retry_scheduled')


def gate_reason(state, url):
    """None, or why the page is refused: a refusal page only needs time, a captcha or login gate needs a person."""
    path = urlsplit(url).path.rstrip('/')
    login = path.startswith(('/website-login', '/login'))
    if not state.get('blocked') and not login:
        return None
    if state.get('blockKind') not in {'verification', 'login'} and (path == '/website-login/error' or not login):
        return 'access_restricted'
    return 'login_or_verification_required'


class NavigationGate:
    def __init__(self, store, clock=time.time, sleep=asyncio.sleep):
        self.store, self.clock, self.sleep = store, clock, sleep
        self.lock = asyncio.Lock()

    async def wait(self, paused):
        # Read the last navigation only after acquiring the shared gate.
        async with self.lock:
            last = float(self.store.control("last_navigation") or 0)
            # Post opens and search result pages share this gap; it slows down with the
            # same persisted factor as API requests.
            interval = self.store.config.open_interval * stored_factor(self.store)
            while (remaining := last + interval - self.clock()) > 0:
                if paused():
                    raise Paused("user_pause")
                await self.sleep(min(remaining, 0.25))
            if paused():
                raise Paused("user_pause")
            self.store.control("last_navigation", self.clock())


class Runner:
    def __init__(self, store, media=None, host=None):
        self.host = host
        self.store, self.config, self.media = store, store.config, media
        self.pause_requested = False
        self.paused = False
        self.running = False
        self.message = "等待开始"
        self.gate = NavigationGate(store)
        self.discovery_page = None
        self.pacer = None  # created in run(); tests may inject one
        self.structural_errors = 0
        self.post_workers = set()
        self.post_progress = asyncio.Event()
        self.active_captures = set()
        self.worker_failure = None
        self.pace_lock = asyncio.Lock()
        self.page_creation_lock = asyncio.Lock()

    async def open_note(self, page, url):
        # One route only (/explore/{id} with its token); a refused open stops
        # the batch rather than being retried through another route.
        return await page.goto(url, wait_until='domcontentloaded', timeout=30000)

    async def new_page(self, context):
        if self.host:
            return await self.host.new_page()
        # Playwright new_page() activates a foreground tab in the attached
        # desktop Chrome. Create a background target instead; serialize the
        # page event so concurrent workers cannot receive the same new page.
        async with self.page_creation_lock:
            if not context.pages:
                return await context.new_page()
            existing = await context.new_cdp_session(context.pages[0])
            try:
                existing_info = await existing.send('Target.getTargetInfo')
                context_id = existing_info['targetInfo'].get('browserContextId')
            finally:
                await existing.detach()
            cdp = await context.browser.new_browser_cdp_session()
            target = None
            try:
                async with context.expect_page() as created:
                    target = await cdp.send('Target.createTarget', {
                        'url': 'about:blank', 'background': True,
                        **({'browserContextId': context_id} if context_id else {}),
                    })
                page = await created.value
                session = await context.new_cdp_session(page)
                try:
                    info = await session.send('Target.getTargetInfo')
                    if info['targetInfo']['targetId'] != target['targetId']:
                        raise RuntimeError('background_page_identity_mismatch')
                finally:
                    await session.detach()
                return page
            except BaseException:
                if target:
                    await cdp.send('Target.closeTarget', {'targetId': target['targetId']})
                raise
            finally:
                await cdp.detach()

    def pause(self):
        self.pause_requested = True
        self.message = "正在暂停，等待已发出的响应写入"

    async def connect(self, playwright):
        if self.host:
            browser, context = await self.host.connect(playwright)
            self.store.control('browser_version', getattr(self.host, 'version', browser.version))
            self.store.control('active_cdp', self.host.endpoint)
            return browser, context
        from .cli import version_info
        info = await asyncio.to_thread(version_info, self.config.cdp)
        browser = await playwright.chromium.connect_over_cdp(info['webSocketDebuggerUrl'], no_defaults=True, timeout=5000)
        if not browser.contexts:
            raise RuntimeError("专用 Chrome 没有可用上下文")
        self.store.control("browser_version", browser.version)
        return browser, browser.contexts[0]

    async def discover(self, context):
        pages = ([await self.host.discovery_page()] if self.host else
                 [p for p in context.pages if re.match(r"https://www\.(xiaohongshu|rednote)\.com/(search_result|user/profile/|explore(?:\?|$))", p.url)])
        if len(pages) != 1:
            raise RuntimeError("请在专用 Chrome 中只保留一个搜索结果、推荐或作者主页标签")
        page = pages[0]
        self.discovery_page = page
        initial_url = page.url
        await page.add_script_tag(path=str(ADAPTER))
        capture = Capture(self.store, None, None, Ledger(None))
        await capture.start(page)
        # Persist discovery separately so GUI resume doesn't silently lose its scope.
        self.store.control("discovery_status", "running")
        # Already-loaded page: honestly label a snapshot; future search responses are captured.
        try:
            self.store.save(None, None, "page_state_snapshot", (await page.content()).encode(), {"url": initial_url, "scope": "discovery_dom", "pre_navigation_capture": False})
            if (await page.evaluate("window.__xhsCollect.state('')"))["blocked"]:
                raise Paused("login_or_verification_required")
            await page.evaluate("a => window.__xhsCollect.startDiscovery(a.count, a.interval, true)",
                                {"count": self.config.max_scrolls, "interval": self.config.page_interval * 1000})
            while True:
                if self.pause_requested:
                    await page.evaluate("window.__xhsCollect.cancelDiscovery()")
                if page.url != initial_url:
                    raise Paused("discovery_page_changed")
                progress = await page.evaluate("window.__xhsCollect.discoveryProgress()")
                capture.check()
                if capture.ledger.blocked or (await page.evaluate("window.__xhsCollect.state('')"))["blocked"]:
                    await page.evaluate("window.__xhsCollect.cancelDiscovery()")
                    raise Paused("access_restricted")
                if progress["links"]:
                    self.store.enqueue(progress["links"], {"url": initial_url, "filters": progress["filters"], "scrolls": progress["count"], "mode": "browser_discovery"})
                self.message = f"发现：滚动 {progress['count']} 次，已登记 {len(self.store.tasks())} 篇"
                if progress["done"]:
                    self.store.save(None, None, "discovery_result", encode(progress), {"url": initial_url})
                    if progress["reason"] in ("paused", "page_changed"):
                        raise Paused(progress["reason"])
                    self.store.control("discovery_status", "finished")
                    break
                await self.paced(capture)
                self.checkpoint(capture)
                await page.evaluate("window.__xhsCollect.stepDiscovery()")
                await asyncio.sleep(0.1)
        finally:
            try:
                await page.evaluate("window.__xhsCollect.cancelDiscovery()")
            finally:
                await capture.stop()
                self.discovery_page = None
                if self.store.control("discovery_status") == "running":
                    self.store.control("discovery_status", "paused")

    def checkpoint(self, capture=None):
        # Any refusal (HTTP 461/429…, business code, 安全限制 page) stops the
        # whole batch. It is never retried per post, per endpoint or by route.
        if self.config.post_concurrency > 1:
            if self.worker_failure is not None:
                raise self.worker_failure
            for active in self.active_captures:
                try:
                    active.check()
                    if active.ledger.blocked:
                        raise Paused('access_restricted')
                except Exception as exc:
                    self.worker_failure = exc
                    self.pause_requested = True
                    raise
        if capture:
            capture.check()
            if capture.ledger.blocked:
                raise Paused("access_restricted")
        if self.pause_requested or (self.store.root / '.pause-requested').exists():
            raise Paused("user_pause")

    async def page_state(self, page, id_, capture):
        self.checkpoint(capture)
        state = await page.evaluate("id => window.__xhsCollect?.state(id) ?? {blocked:false, unavailable:false, note:null}", id_)
        current = state.get('href', page.url)
        if reason := gate_reason(state, current):
            capture.ledger.blocked = True
            raise Paused(reason)
        if state["unavailable"]:
            raise Unavailable("note_unavailable")
        if re.search(r"/(?:explore|discovery/item|search_result)/" + re.escape(id_) + r"(?:[/?#]|$)", current) is None:
            raise PendingPage('page_changed_or_redirected')
        return state

    def seed(self, id_, strategy):
        """Resume only request-strategy chains rebuilt from verified raw responses."""
        if strategy != "request" or not self.config.comments:
            return Ledger(id_), [], None
        if self.store.control(chain_key(id_)) is None:
            # Batches from before resume chains existed: seed from the latest attempt.
            row = self.store.db.execute("SELECT id FROM attempts WHERE note_id=? AND status!='running' ORDER BY rowid DESC LIMIT 1", (id_,)).fetchone()
            if row:
                self.store.control(chain_key(id_), encode([row[0]]).decode())
        return resume_seed(self.store, id_)

    async def collect(self, context, task, strategy=None):
        from .media import MediaCollector
        from ..module.access import AccessRestricted
        id_, url = task["note_id"], navigation_url(task["url"])
        strategy = strategy or self.config.strategy
        strategy = "request" if strategy == "auto" else strategy
        ledger, chain, stats = self.seed(id_, strategy)
        attempt = self.store.begin(id_)
        chain = chain + [attempt]
        self.store.control(chain_key(id_), encode(chain).decode())
        if stats is not None:
            self.store.save(attempt, id_, "resume_seed", encode({"chain": chain, "resumed": len(chain) > 1, **stats}),
                            progress=ledger.record() if len(chain) > 1 else None)
        result = {"body": task.get("body", "partial"), "comments": "partial" if self.config.comments else "not_requested",
                  "media": ("success" if task.get("media") == "success" else "pending") if self.config.media else "disabled",
                  "comment_media": ("success" if task.get("comment_media") == "success" else "pending") if self.config.media and self.config.comments else "not_requested",
                  "reason": "unknown", "strategy": strategy, "resume_chain": len(chain)}
        status, page, pause_error = "finished", None, None
        capture = Capture(self.store, attempt, id_, ledger)
        self.active_captures.add(capture)
        assets = MediaCollector(self.store, self.checkpoint)
        try:
            note = self.store.note_object(id_)
            off_topic = note is not None and self.off_topic(id_, note)
            # A verified archived body and completed pagination allow media-only recovery offline.
            if note is None or self.config.comments and not off_topic and not self.comments_done(ledger):
                await self.gate.wait(lambda: self.pause_requested)
                await self.paced(None)
                page = await self.new_page(context)
                await page.add_init_script(path=str(ADAPTER))
                await page.add_init_script(path=str(REQUEST_ADAPTER))
                await capture.start(page)
                await self.open_note(page, url)
                deadline = time.monotonic() + 30
                while True:
                    state = await self.page_state(page, id_, capture)
                    if state["note"]:
                        break
                    if time.monotonic() > deadline:
                        raise TimeoutError("page_ready_timeout")
                    await asyncio.sleep(0.2)
                note = state["note"]
                self.store.save(attempt, id_, "page_state_snapshot", encode(state), {"url": url, "adapter": VERSION})
                if self.pacer:
                    self.pacer.succeeded()
                off_topic = self.off_topic(id_, note)
            if note is not None:
                self.store.save(attempt, id_, "note_object", encode(note), {"verified_note_id": id_})
                result["body"] = "saved"
                self.store.update_parts(id_, {"body": "saved"})
            if note is not None and off_topic:
                # Body saved for the record; no comment pages or media for a slang collision.
                result.update(comments="skipped", media="skipped", comment_media="skipped", reason="off_topic")
                self.store.save(attempt, id_, "topic_gate", encode({"keep": False, "reason": off_topic}))
            elif note is not None:
                async def save_body_media():
                    self.message = f"保存帖子媒体：{id_}"
                    try:
                        result["media"] = await (self.media(note, url) if self.media else assets(note, url))
                    except (Paused, AccessRestricted, OSError, sqlite3.Error):
                        raise
                    except Exception:
                        result["media"] = "failed"
                    self.store.update_parts(id_, {"media": result["media"]})
                async def save_comments():
                    nonlocal state
                    self.checkpoint(capture)
                    self.message = f"采集评论与回复：{id_}"
                    if self.comments_done(ledger):
                        result["reason"] = self.done_reason(ledger)
                    elif strategy == "request":
                        capability = await page.evaluate("window.__xhsReadOnly.probe()")
                        self.store.save(attempt, id_, "adapter_capability", encode(capability))
                        if capability["available"]:
                            result["reason"] = await self.request_comments(page, id_, capture, ledger, url)
                        else:
                            result["reason"] = "adapter_unavailable"
                    else:
                        deadline = time.monotonic() + 30
                        while not state["commentsReady"]:
                            if time.monotonic() > deadline:
                                raise TimeoutError("comments_container_timeout")
                            await asyncio.sleep(0.2)
                            state = await self.page_state(page, id_, capture)
                        result["reason"] = await self.comments(page, id_, capture, ledger)

                async def guarded(work):
                    try:
                        return await work
                    except (Paused, AccessRestricted, OSError, sqlite3.Error) as exc:
                        if self.worker_failure is None:
                            self.worker_failure = Paused('access_restricted') if isinstance(exc, AccessRestricted) else exc
                        self.pause_requested = True
                        raise

                jobs = []
                if self.config.media:
                    jobs.append(asyncio.create_task(guarded(save_body_media())))
                if self.config.comments:
                    jobs.append(asyncio.create_task(guarded(save_comments())))
                else:
                    result['reason'] = 'body_saved'
                try:
                    outcomes = await asyncio.gather(*jobs, return_exceptions=True)
                finally:
                    # Do not close the media client/page while a sibling job is
                    # still saving responses, including on cancellation.
                    await asyncio.gather(*jobs, return_exceptions=True)
                for outcome in outcomes:
                    if isinstance(outcome, BaseException):
                        raise outcome
        except PendingPage as e:
            status, result['reason'], pause_error = 'paused', str(e), e
        except AccessRestricted as e:
            status, result["reason"], pause_error = "paused", e.reason, Paused("access_restricted")
        except Paused as e:
            status, result["reason"], pause_error = "paused", str(e), e
        except Unavailable:
            result.update(body="unavailable", reason="note_unavailable")
        except (TimeoutError, asyncio.TimeoutError, BrowserError):
            # A redirect to a refusal or login page can interrupt a page read.
            if reason := page and not page.is_closed() and gate_reason({}, page.url):
                ledger.blocked = True
                pause_error = Paused(reason)
            else:
                pause_error = PendingPage('page_timeout_or_connection_error')
            status, result['reason'] = 'paused', str(pause_error)
        except (OSError, sqlite3.Error):
            status, result['reason'] = 'interrupted', 'storage_error'
            pause_error = Paused('storage_error')
            raise
        except asyncio.CancelledError:
            status, result['reason'] = 'interrupted', 'cancelled'
            pause_error = Paused('cancelled')
            raise
        except Exception as e:
            result["reason"] = type(e).__name__
        finally:
            try:
                await capture.stop()
                if ledger.blocked and str(pause_error) not in {"access_restricted", "login_or_verification_required"}:
                    pause_error = Paused("access_restricted")
                    status, result["reason"] = "paused", str(pause_error)
                if result['reason'] == 'comment_slice_yield' and not pause_error:
                    # A slice is unfinished work, never a completed/failed task.
                    # The next attempt rebuilds the same verified cursor chain.
                    status = 'pending'
                if self.config.comments and result["comments"] != "skipped":
                    result["comments"] = self.comment_status(ledger, result["body"])
                self.store.save(attempt, id_, "comment_progress", encode(ledger.record()), progress=ledger.record())
                self.store.update_parts(id_, {"comments": result["comments"]})
                if page and result["reason"] not in {"observed_end", "primary_end", "valid_comment_cap", "off_topic"}:
                    try:
                        html = await page.content()
                    except Exception:
                        html = None
                    if html:
                        self.store.save(attempt, id_, "page_state_snapshot", html.encode(), {"scope": "diagnostic_dom", "reason": result["reason"]})
                if self.config.media and self.config.comments and result["comments"] != "skipped" and (
                        not pause_error or isinstance(pause_error, PendingPage)):
                    self.message = f"保存评论图片：{id_}"
                    try:
                        result["comment_media"] = await assets.comments(id_)
                    except AccessRestricted:
                        status, result["reason"], pause_error = "paused", "access_restricted", Paused("access_restricted")
                    except Paused as e:
                        status, result["reason"], pause_error = "paused", str(e), e
                    except (OSError, sqlite3.Error):
                        raise
                    except Exception:
                        result["comment_media"] = "failed"
            except (OSError, sqlite3.Error):
                status, result['reason'] = 'interrupted', 'storage_error'
                raise
            finally:
                await assets.close()
                if page:
                    if self.host:
                        await self.host.close_page(page)
                    else:
                        await page.close()
                try:
                    self.store.finish(attempt, result, status)
                    if isinstance(pause_error, PendingPage):
                        self.store.defer_task(id_, pause_error.kind, pause_error.until, str(pause_error))
                    elif result['comments'] in {'complete_observed', 'sampled', 'skipped'}:
                        with self.store.db:
                            self.store.db.execute('DELETE FROM deferred_tasks WHERE note_id=?', (id_,))
                finally:
                    self.active_captures.discard(capture)
        if pause_error and not isinstance(pause_error, PendingPage):
            raise pause_error
        return result

    def off_topic(self, id_, note):
        """Reason to skip this post's comments and media, or None to collect them."""
        if not self.config.topic_gate:
            return None
        keep, reason = relevant_note(note, self.store.keywords(id_), profile_for(self.store.root))
        return None if keep else reason

    def comments_done(self, ledger):
        if self.config.comment_scope == "primary":
            return ledger.sampled(self.config.valid_comment_cap)
        return ledger.complete()

    @staticmethod
    def done_reason(ledger):
        return "observed_end" if ledger.complete() else "primary_end" if ledger.primary.complete() else "valid_comment_cap"

    def comment_status(self, ledger, body):
        if ledger.blocked:
            return "blocked"
        if body != "saved":
            return "partial"
        if ledger.complete():
            return "complete_observed"
        # Every primary page, or enough substantive comments, under the sampling rule.
        if self.config.comment_scope == "primary" and ledger.sampled(self.config.valid_comment_cap):
            return "sampled"
        return "partial"

    async def paced(self, capture):
        """Batch-wide pacing gate, then count the action. Interruptible."""
        async with self.pace_lock:
            self.checkpoint(capture)
            if self.pacer is None:
                self.pacer = Pacer(self.store)
            try:
                await self.pacer.wait(lambda: self.checkpoint(capture))
            except PacingPause as e:
                raise Paused(e.reason) from None
            # Reservation is atomic across all post workers and search requests.
            self.pacer.record()

    async def wait_page_interval(self, page, id_, capture):
        # Waiting remains interruptible; recheck the page before another action.
        await self.paced(capture)
        await self.page_state(page, id_, capture)

    async def request_comments(self, page, id_, capture, ledger, url):
        if not await capture.drain(timeout=self.config.operation_timeout):
            return "initial_capture_pending"
        primary_only = self.config.comment_scope == "primary"
        for index in (count() if self.config.max_operations is None else range(self.config.max_operations)):
            await self.page_state(page, id_, capture)
            if self.comments_done(ledger):
                return self.done_reason(ledger)
            if self.config.comment_page_quantum is not None and index >= self.config.comment_page_quantum:
                return 'comment_slice_yield'
            # Explicit pacing; never hide restrictions by switching request paths.
            await self.wait_page_interval(page, id_, capture)
            if self.comments_done(ledger):
                return self.done_reason(ledger)
            req = ledger.next_request(primary_only=primary_only)
            if req is None:
                return "pagination_evidence_invalid"
            target = request_url(req, capture.templates, url)
            op = capture.begin_operation(target)
            reason = "controlled_operation_incomplete"
            try:
                started = await page.evaluate("a => window.__xhsReadOnly.start(a.id, a.url)",
                                              {"id": op["id"], "url": target})
                if not started["started"]:
                    return "adapter_start_rejected"
                deadline = time.monotonic() + self.config.operation_timeout
                while True:
                    await self.page_state(page, id_, capture)
                    feedback = await page.evaluate("id => window.__xhsReadOnly.status(id)", op["id"])
                    if op["done"].done() and feedback and feedback["state"] != "pending":
                        op["done"].result()  # Only a durable CDP commit fulfills this future.
                        reason = None
                        break
                    if time.monotonic() >= deadline:
                        return "operation_response_timeout"
                    await asyncio.sleep(0.05)
            finally:
                capture.end_operation(reason)
            if not await capture.drain(timeout=self.config.operation_timeout):
                return "capture_pending"
            if self.pacer and not ledger.blocked and not ledger.gaps:
                self.pacer.succeeded()
        return self.done_reason(ledger) if self.comments_done(ledger) else "operation_limit"

    async def comments(self, page, id_, capture, ledger):
        idle = 0
        last_parent = None
        fingerprint = encode(ledger.record())
        for index in (count() if self.config.max_operations is None else range(self.config.max_operations)):
            await self.page_state(page, id_, capture)
            await capture.drain(timeout=0.5)
            if self.comments_done(ledger):
                return self.done_reason(ledger)
            if self.config.comment_page_quantum is not None and index >= self.config.comment_page_quantum:
                return 'comment_slice_yield'
            await self.wait_page_interval(page, id_, capture)
            if self.comments_done(ledger):
                return self.done_reason(ledger)
            # Primary-only sampling scrolls and never clicks "展开更多回复".
            incomplete = [] if self.config.comment_scope == "primary" else [
                id_ for id_, chain in ledger.replies.items() if not chain.complete()]
            version = ledger.version
            action = await page.evaluate("args => window.__xhsCollect.action(args.ids, args.parent)",
                                         {"ids": incomplete, "parent": last_parent if last_parent in incomplete else None})
            last_parent = action.get("parent")
            until = time.monotonic() + self.config.operation_timeout
            while ledger.version == version and time.monotonic() < until:
                await self.page_state(page, id_, capture)
                await asyncio.sleep(0.1)
            await capture.drain(timeout=self.config.operation_timeout)
            if ledger.version == version:
                idle += 1
            else:
                # Repeated pages are not progress. Only new IDs/edges change fingerprint.
                observed = encode(ledger.record())
                idle = idle + 1 if fingerprint == observed else 0
                fingerprint = observed
            if idle >= self.config.no_progress_limit:
                return "no_progress"
        return "operation_limit"

    async def collect_next(self, context):
        """Use the same complete-post flow between search pages and for the remainder."""
        self.checkpoint()
        task = self.store.next_task()
        if task is None:
            return False
        self.message = f"采集 {task['note_id']}"
        result = await self.collect(context, task)
        if self.config.strategy == "auto" and result["reason"] == "adapter_unavailable":
            self.checkpoint()
            # Capability incompatibility only, never an access-restriction fallback.
            result = await self.collect(context, task, strategy="ui")
        # Repeated page timeouts/redirects also stop the batch instead of cycling posts.
        failed = result["body"] == "partial" or result["reason"].endswith("_retry_scheduled")
        self.structural_errors = self.structural_errors + 1 if failed else 0
        if self.structural_errors >= 3:
            raise Paused("consecutive_page_failures")
        return True

    async def _post_worker(self, context):
        try:
            while await self.collect_next(context):
                self.post_progress.set()
                await asyncio.sleep(0)
            return False
        except Exception as exc:
            if self.worker_failure is None:
                self.worker_failure = exc
            self.pause_requested = True
            raise
        finally:
            self.post_progress.set()

    async def collect_turn(self, context, *, wait=True):
        """Refill the pool; search may keep progressing while posts are busy."""
        if self.config.post_concurrency == 1:
            return await self.collect_next(context)
        self.checkpoint()
        if wait:
            self.post_progress.clear()
        # Retrieve all completed exceptions, including when search was in flight.
        done = {worker for worker in self.post_workers if worker.done()}
        self.post_workers.difference_update(done)
        if done:
            results = await asyncio.gather(*done, return_exceptions=True)
            for result in results:
                if isinstance(result, BaseException):
                    raise result
        while len(self.post_workers) < self.config.post_concurrency and self.store.next_task() is not None:
            # collect_next re-reads and claims the task synchronously (begin)
            # before its first await; Store rejects a duplicate running claim.
            self.post_workers.add(asyncio.create_task(self._post_worker(context)))
        if not self.post_workers:
            return False
        if wait:
            await self.post_progress.wait()
        else:
            # Let new workers claim their posts before search continues. Waiting
            # for a whole post here serializes keyword paging behind its media
            # downloads and comment slice, even with multiple post workers.
            await asyncio.sleep(0)
        self.checkpoint()
        return True

    async def drain_workers(self):
        if self.post_workers:
            self.pause_requested = True
            # Keep Playwright connected while in-flight responses are committed.
            # Do not cancel successful requests merely because a peer stopped.
            await asyncio.gather(*self.post_workers, return_exceptions=True)
            self.post_workers.clear()

    async def run_connected(self, context, discover):
        from .search import add_keywords, queued_keywords, discover_keywords
        if self.store.control('search_progress') or queued_keywords(self.store):
            await self.collect_turn(context, wait=False)
            await discover_keywords(self, context, collect_posts=True)
        if discover:
            await self.discover(context)
        while True:
            self.checkpoint()
            requested = queued_keywords(self.store)
            if requested:
                before = self.store.control('search_progress')
                add_keywords(self.store, requested)
                if self.store.control('search_progress') != before:
                    await discover_keywords(self, context, collect_posts=True)
            if not await self.collect_turn(context):
                retry_at = self.store.next_retry_time()
                if retry_at is None:
                    break
                self.message = '等待加载超时的帖子重试；已保存的正文和媒体保留'
                while time.time() < retry_at:
                    self.checkpoint()
                    await asyncio.sleep(min(1, retry_at - time.time()))
        self.message = "本批次处理结束；请查看各篇完整性状态"

    async def run(self, discover=False, retry=False):
        if self.running:
            raise RuntimeError("采集已经运行")
        self.running, self.pause_requested, self.paused = True, False, False
        self.structural_errors = 0
        self.worker_failure = None
        try:
            self.pacer = Pacer(self.store)
            # Audit record of the configuration actually used by this session
            # (run.json may be edited later; events are append-only).
            self.store.save(None, None, "session_config", encode({**self.config.record(), "pacing": self.pacer.state()}))
            try:
                if self.store.control("workspace_blocked"):
                    raise Paused("access_restricted_unacknowledged")
                self.pacer.check_cooldown()
            except PacingPause as e:
                minutes = max(1, round((e.until - time.time()) / 60))
                raise Paused(f"{e.reason}_cooldown_{minutes}min") from None
            if retry:
                self.store.retry_incomplete()
            async with async_playwright() as p:
                _, context = await self.connect(p)
                try:
                    await self.run_connected(context, discover)
                finally:
                    await self.drain_workers()
        except Paused as e:
            self.paused = True
            if str(e) in {"access_restricted", "login_or_verification_required"}:
                self.store.control('workspace_blocked', str(e))
                # Persisted cooldown + slowdown; resuming early is refused by check_cooldown().
                until = self.pacer.restricted()
                self.message = f"已暂停：access_restricted；冷却至 {time.strftime('%m-%d %H:%M', time.localtime(until))}，之后自动降速为 {self.pacer.factor:g} 倍间隔"
            else:
                self.message = f"已暂停：{e}"
        except Exception as e:
            self.pause_requested = True
            self.message = f"已停止：{type(e).__name__}；请检查 Chrome 连接、存储和诊断"
            raise
        finally:
            self.running = False
            self.store.export()
