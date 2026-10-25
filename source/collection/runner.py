import asyncio
from pathlib import Path
import re
import sqlite3
import time

from playwright.async_api import async_playwright

from .capture import Capture
from .protocol import Ledger
from .store import encode
from .request import request_url
from . import VERSION

ADAPTER = Path(__file__).resolve().parents[2] / "static" / "collection-page.js"
REQUEST_ADAPTER = ADAPTER.with_name("collection-request.js")


class Paused(Exception):
    pass


class Unavailable(Exception):
    pass


class NavigationGate:
    def __init__(self, store, clock=time.time, sleep=asyncio.sleep):
        self.store, self.clock, self.sleep = store, clock, sleep

    async def wait(self, paused):
        last = float(self.store.control("last_navigation") or 0)
        while (remaining := last + self.store.config.open_interval - self.clock()) > 0:
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

    def pause(self):
        self.pause_requested = True
        self.message = "正在暂停，等待已发出的响应写入"

    async def connect(self, playwright):
        if self.host:
            browser, context = await self.host.connect(playwright)
            self.store.control('browser_version', getattr(self.host, 'version', browser.version))
            self.store.control('active_cdp', self.host.endpoint)
            return browser, context
        browser = await playwright.chromium.connect_over_cdp(self.config.cdp, timeout=5000)
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
            await page.evaluate("n => window.__xhsCollect.startDiscovery(n)", self.config.max_scrolls)
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
        if capture:
            capture.check()
            if capture.ledger.blocked:
                raise Paused("access_restricted")
        if self.pause_requested:
            raise Paused("user_pause")

    async def page_state(self, page, id_, capture):
        self.checkpoint(capture)
        current = page.url
        if re.search(r"/(?:explore|discovery/item)/" + re.escape(id_) + r"(?:[/?#]|$)", current) is None:
            raise Paused("controlled_page_changed_or_redirected")
        state = await page.evaluate("id => window.__xhsCollect.state(id)", id_)
        if state["blocked"]:
            raise Paused("login_or_verification_required")
        if state["unavailable"]:
            raise Unavailable("note_unavailable")
        return state

    async def collect(self, context, task, strategy=None):
        id_, url = task["note_id"], task["url"]
        attempt = self.store.begin(id_)
        ledger = Ledger(id_)
        strategy = strategy or self.config.strategy
        strategy = "request" if strategy == "auto" else strategy
        result = {"body": "partial", "comments": "partial" if self.config.comments else "not_requested", "media": "disabled", "reason": "unknown", "strategy": strategy}
        status = "finished"
        page = None
        capture = Capture(self.store, attempt, id_, ledger)
        note = None
        pause_error = None
        try:
            page = await self.host.new_page() if self.host else await context.new_page()
            await capture.start(page)
            await page.add_init_script(path=str(ADAPTER))
            await page.add_init_script(path=str(REQUEST_ADAPTER))
            await self.gate.wait(lambda: self.pause_requested)
            async with asyncio.timeout(self.config.note_timeout):
                await page.goto(url, wait_until="domcontentloaded", timeout=30000)
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
                self.store.save(attempt, id_, "note_object", encode(note), {"source": "page_state_snapshot", "verified_note_id": id_})
                result["body"] = "saved"
                if self.config.comments:
                    if strategy == "request":
                        capability = await page.evaluate("window.__xhsReadOnly.probe()")
                        self.store.save(attempt, id_, "adapter_capability", encode(capability))
                        if capability["available"]:
                            result["reason"] = await self.request_comments(page, id_, capture, ledger, url)
                        else:
                            result["reason"] = "adapter_unavailable"
                    else:
                        while not state["commentsReady"]:
                            if time.monotonic() > deadline:
                                raise TimeoutError("comments_container_timeout")
                            await asyncio.sleep(0.2)
                            state = await self.page_state(page, id_, capture)
                        result["reason"] = await self.comments(page, id_, capture, ledger)
                else:
                    result["reason"] = "body_saved"
        except Paused as e:
            status, result["reason"], pause_error = "paused", str(e), e
        except Unavailable:
            result.update(body="unavailable", reason="note_unavailable")
        except (TimeoutError, asyncio.TimeoutError):
            result["reason"] = "note_or_page_timeout"
        except (OSError, sqlite3.Error):
            raise  # storage failure must halt the batch
        except Exception as e:
            result["reason"] = type(e).__name__
            # No URLs, cookies, or arbitrary exception strings in ordinary UI logs.
        finally:
            try:
                await capture.stop()
                if ledger.blocked and not pause_error:
                    pause_error = Paused('access_restricted')
                    status, result['reason'] = 'paused', 'access_restricted'
                if self.config.comments:
                    result["comments"] = "blocked" if ledger.blocked else ("complete_observed" if ledger.complete() and result["body"] == "saved" and result["reason"] == "observed_end" and not pause_error else "partial")
                self.store.save(attempt, id_, "comment_progress", encode(ledger.record()), progress=ledger.record())
                if result["reason"] != "observed_end":
                    try:
                        html = await page.content()
                    except Exception:
                        html = None
                    if html:
                        self.store.save(attempt, id_, "page_state_snapshot", html.encode(), {"scope": "diagnostic_dom", "reason": result["reason"]})
            finally:
                if page:
                    if self.host:
                        await self.host.close_page(page)
                    else:
                        await page.close()
        if note and self.config.media and not pause_error and result["reason"] != "adapter_unavailable":
            try:
                self.checkpoint()
                result["media"] = await self.media(note, url)
            except Paused as e:
                status, result["reason"], pause_error = "paused", str(e), e
            except (OSError, sqlite3.Error):
                raise
            except Exception:
                result["media"] = "failed"
        self.store.finish(attempt, result, status)
        if pause_error:
            raise pause_error
        return result

    async def request_comments(self, page, id_, capture, ledger, url):
        if not await capture.drain(timeout=self.config.operation_timeout):
            return "initial_capture_pending"
        for _ in range(self.config.max_operations):
            await self.page_state(page, id_, capture)
            if ledger.complete():
                return "observed_end"
            req = ledger.next_request()
            if req is None:
                return "pagination_evidence_invalid"
            # Explicit pacing; never hide restrictions by switching request paths.
            until = time.monotonic() + self.config.page_interval
            while time.monotonic() < until:
                self.checkpoint(capture)
                await asyncio.sleep(min(0.1, max(0, until - time.monotonic())))
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
        return "observed_end" if ledger.complete() else "operation_limit"

    async def comments(self, page, id_, capture, ledger):
        idle = 0
        last_parent = None
        fingerprint = encode(ledger.record())
        for _ in range(self.config.max_operations):
            await self.page_state(page, id_, capture)
            await capture.drain(timeout=0.5)
            if ledger.complete():
                return "observed_end"
            incomplete = [id_ for id_, chain in ledger.replies.items() if not chain.complete()]
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

    async def run(self, discover=False, retry=False):
        if self.running:
            raise RuntimeError("采集已经运行")
        self.running, self.pause_requested, self.paused = True, False, False
        structural_errors = 0
        try:
            if retry:
                self.store.retry_incomplete()
            async with async_playwright() as p:
                _, context = await self.connect(p)
                if discover:
                    await self.discover(context)
                while task := self.store.next_task():
                    self.checkpoint()
                    self.message = f"采集 {task['note_id']}"
                    result = await self.collect(context, task)
                    if self.config.strategy == "auto" and result["reason"] == "adapter_unavailable":
                        self.checkpoint()
                        # Capability incompatibility only: new attempt, normal navigation gate.
                        # Access restrictions and failed API requests never trigger a switch.
                        result = await self.collect(context, task, strategy="ui")
                    structural_errors = structural_errors + 1 if result["body"] == "partial" else 0
                    if structural_errors >= 3:
                        raise Paused("consecutive_page_failures")
                self.message = "本批次处理结束；请查看各篇完整性状态"
        except Paused as e:
            self.paused = True
            self.message = f"已暂停：{e}"
        except Exception as e:
            self.pause_requested = True
            self.message = f"已停止：{type(e).__name__}；请检查 Chrome 连接、存储和诊断"
            raise
        finally:
            self.running = False
            self.store.export()
