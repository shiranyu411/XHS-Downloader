"""Passive CDP recording: no injected scripts, no site requests.

Raw responses are authoritative; cumulative and per-session reports are projections.
"""
import asyncio
import base64
from collections import deque
import time
from uuid import uuid4

from .observations import (Coverage as Coverage, ObservationIndex,
                           comment_count as comment_count, note_items as note_items)
from .protocol import classify, restricted
from .store import atomic_write, encode


class RateWatch:
    """Observed browsing speed, never an inferred platform threshold."""

    def __init__(self, warn_10s=10, warn_60s=30):
        self.warn_10s, self.warn_60s = warn_10s, warn_60s
        self.times = deque()
        self.warned = False

    def add(self, now=None):
        self.times.append(now if now is not None else time.monotonic())

    def counts(self, now=None):
        now = now if now is not None else time.monotonic()
        while self.times and self.times[0] <= now - 300:
            self.times.popleft()
        return (sum(t > now - 10 for t in self.times), sum(t > now - 60 for t in self.times))

    def state(self, now=None):
        ten, sixty = self.counts(now)
        self.warned = ten >= self.warn_10s or sixty >= self.warn_60s
        return {"last_10s": ten, "last_60s": sixty, "fast": self.warned,
                "warn_10s": self.warn_10s, "warn_60s": self.warn_60s,
                "advice": "当前浏览速度偏快，建议放慢滚动与展开；不是平台安全阈值" if self.warned else ""}


class PassiveCapture:
    def __init__(self, recorder, page):
        self.recorder, self.page, self.store = recorder, page, recorder.store
        self.requests, self.jobs = {}, set()
        self.cdp = None
        self.closed = self.ready = False
        self.start_task = None
        self.stop_task = None

    async def start(self):
        try:
            self.cdp = await self.page.context.new_cdp_session(self.page)
            self.cdp.on("Network.requestWillBeSent", self.on_request)
            self.cdp.on("Network.responseReceived", self.on_response)
            self.cdp.on("Network.loadingFinished", self.on_finished)
            self.cdp.on("Network.loadingFailed", self.on_failed)
            await self.cdp.send("Network.enable", {"maxTotalBufferSize": 100_000_000,
                                                   "maxResourceBufferSize": 20_000_000})
            self.ready = not self.closed
        except Exception as exc:
            self.recorder.fail(exc, "listener_start_failed")

    def metadata(self, item):
        req = item["request"]
        return {"kind": req.kind, "url": item["url"], "status": item["status"],
                "method": item["method"], "mode": "passive", "session_id": self.recorder.session_id,
                "note_id": req.note, "root_comment_id": req.parent, "cursor": req.cursor}

    def on_request(self, event):
        if self.closed or self.recorder.stopped:
            return
        request = event["request"]
        if request["method"] == "OPTIONS" or event.get("type") == "Preflight":
            return
        req = classify(request["url"], request.get("postData"), event.get("type"))
        if req is None:
            return
        key = event["requestId"]
        if key in self.requests:
            self.recorder.gap("redirected_relevant_request", self.metadata(self.requests.pop(key)))
        self.requests[key] = {"request": req, "url": request["url"], "method": request["method"], "status": None}
        self.recorder.rate.add()

    def on_response(self, event):
        item = self.requests.get(event["requestId"])
        if item:
            item["status"] = event["response"]["status"]
            if restricted(None, item["status"]):
                self.recorder.restriction(item["status"])

    def on_failed(self, event):
        item = self.requests.pop(event["requestId"], None)
        if item:
            self.recorder.gap("network_loading_failed", self.metadata(item))

    def on_finished(self, event):
        key = event["requestId"]
        if key in self.requests and not self.requests[key].get("reading"):
            self.requests[key]["reading"] = True
            task = asyncio.create_task(self.read_body(key))
            self.jobs.add(task)
            task.add_done_callback(self.jobs.discard)

    async def read_body(self, key):
        item = self.requests.get(key)
        if item is None:
            return
        try:
            try:
                response = await self.cdp.send("Network.getResponseBody", {"requestId": key})
                body = base64.b64decode(response["body"]) if response.get("base64Encoded") else response["body"].encode()
            except Exception:
                self.recorder.gap("response_body_unreadable", self.metadata(item))
                return
            req = item["request"]
            kind = "document_html" if req.kind == "document" else "network_json"
            self.store.save(None, req.note, kind, body, self.metadata(item))
            self.recorder.observe(req, body, item["status"])
        except Exception as exc:
            self.recorder.fail(exc, "archive_write_failed")
        finally:
            self.requests.pop(key, None)

    async def stop(self, timeout=10):
        if self.stop_task is None:
            self.closed = True  # Stop accepting, but keep response listeners while draining.
            self.stop_task = asyncio.create_task(self._stop(timeout))
        await asyncio.shield(self.stop_task)
        self.recorder.check()

    async def _stop(self, timeout):
        deadline = time.monotonic() + timeout
        try:
            if self.start_task and not self.start_task.done():
                try:
                    await asyncio.wait_for(asyncio.shield(self.start_task), max(0.01, deadline - time.monotonic()))
                except asyncio.TimeoutError:
                    self.start_task.cancel()
                    await asyncio.gather(self.start_task, return_exceptions=True)
                    self.recorder.gap("listener_start_timeout", {})
            while (self.requests or self.jobs) and time.monotonic() < deadline and not self.recorder.fatal:
                await asyncio.sleep(0.02)
            remaining = list(self.requests.values())
            for task in list(self.jobs):
                task.cancel()
            if self.jobs:
                await asyncio.gather(*list(self.jobs), return_exceptions=True)
            for item in remaining:
                self.recorder.gap("stop_drain_timeout", self.metadata(item))
            self.requests.clear()
        finally:
            self.ready = False
            if self.cdp:
                try:
                    await self.cdp.detach()
                except Exception:
                    pass  # Tab may already have closed; outstanding requests were counted above.


class PassiveRecorder:
    def __init__(self, store, cdp, on_event=None, on_restriction=None):
        self.store, self.cdp = store, cdp
        self.index = ObservationIndex.rebuild(store.db, store.root)
        self.index.persist(store)
        self.session = ObservationIndex()
        self.coverage = self.index.coverage
        self.discovered = self.index.discovered
        self.rate = RateWatch()
        self.captures, self.contexts = {}, []
        self.on_event = on_event or (lambda message: None)
        self.on_restriction = on_restriction
        self.started = time.time()
        self.session_id = uuid4().hex
        self.stopped = False
        self.fatal = None
        self.restriction_reported = False

    def stop(self):
        self.stopped = True

    def fail(self, exc, reason):
        if self.fatal is None:
            self.fatal = exc
            self.on_event(f"录制故障：{reason}（{type(exc).__name__}）；正在停止，不能视为正常完成。")
        self.stop()

    def check(self):
        if self.fatal:
            raise self.fatal

    def gap(self, reason, metadata):
        self.index.gaps += 1
        self.session.gaps += 1
        self.on_event("录制缺口：" + reason)
        try:
            self.store.save(None, metadata.get("note_id"), "passive_gap", encode({"reason": reason}),
                            {**metadata, "mode": "passive", "session_id": self.session_id})
        except Exception as exc:
            self.fail(exc, "gap_write_failed")

    def restriction(self, status=None, code=None):
        if self.restriction_reported:
            return
        self.restriction_reported = True
        self.index.access_restricted = self.session.access_restricted = True
        try:
            if self.on_restriction:
                self.on_restriction("access_restricted")
            else:
                from .pacing import Pacer
                Pacer(self.store).restricted(status, code)
                self.store.control("workspace_blocked", "access_restricted")
                self.store.control("workspace_restriction_ack", "no")
            self.on_event("检测到访问限制：自动作业已锁定；停止录制并保存已在途响应。")
        except Exception as exc:
            self.fail(exc, "restriction_save_failed")
        self.stop()

    def attach(self, page):
        if self.stopped or page in self.captures:
            return
        capture = PassiveCapture(self, page)
        self.captures[page] = capture
        capture.start_task = asyncio.create_task(capture.start())

    def detach_closed(self):
        # Keep ownership until final cleanup, including tabs closed during a write.
        for page, capture in self.captures.items():
            if page.is_closed() and capture.stop_task is None:
                capture.closed = True
                capture.stop_task = asyncio.create_task(capture._stop(0))

    def observe(self, req, body, status):
        found = self.index.observe(req, body, status)
        self.session.observe(req, body, status)
        if self.session.access_restricted:
            self.restriction(status)
        if found:
            self.store.enqueue([f"https://www.xiaohongshu.com/explore/{i}?xsec_token={t}" for i, t in found],
                               {"mode": "passive_discovery"})
            self.on_event(f"新发现 {len(found)} 篇；批次累计 {len(self.discovered)} 篇")
        self.index.persist(self.store)

    def summary(self):
        return {"mode": "passive", "scope": "batch_cumulative", "schema_version": 2,
                "started": self.started, "session_id": self.session_id,
                **self.index.summary(), "session": self.session.summary(),
                "pages_watched": sum(not c.closed for c in self.captures.values()),
                "pages_ready": sum(c.ready and not c.closed for c in self.captures.values()),
                "rate": self.rate.state(), "failed": self.fatal is not None, "stopped": self.stopped,
                "pages_seen": len(self.captures)}

    def report(self):
        return {**self.summary(), "notes": self.index.report()["notes"]}

    def export(self):
        self.index.persist(self.store)
        atomic_write(self.store.root / "passive_report.json", encode(self.report()))
        atomic_write(self.store.root / "passive_sessions" / f"{self.session_id}.json",
                     encode({**self.session.report(), "scope": "session", "session_id": self.session_id,
                             "started": self.started, "stopped": self.stopped, "failed": self.fatal is not None}))

    def watch(self, context):
        self.contexts.append(context)
        for page in context.pages:
            self.attach(page)
        context.on("page", self.attach)

    async def run(self, status_interval=2.0, until=None, context=None):
        if status_interval <= 0:
            raise ValueError("状态间隔必须大于零")
        if context is not None:
            self.watch(context)
            await self._loop(status_interval, until)
            return
        from playwright.async_api import async_playwright
        async with async_playwright() as playwright:
            browser = await playwright.chromium.connect_over_cdp(self.cdp, timeout=5000)
            for found in browser.contexts:
                self.watch(found)
            await self._loop(status_interval, until)

    async def _loop(self, status_interval, until):
        self.on_event("被动录制已启动：不注入、不主动请求；等待监听就绪再浏览。")
        announced, exported = -1, None
        try:
            while not self.stopped and (until is None or time.time() < until):
                self.detach_closed()
                self.check()
                ready = sum(c.ready and not c.closed for c in self.captures.values())
                if ready != announced:
                    announced = ready
                    self.on_event(f"正在监听 {ready} 个标签页；新标签页请等数字更新")
                current = (self.index.events, self.index.gaps, ready)
                if current != exported:
                    self.export()
                    exported = current
                await asyncio.sleep(status_interval)
        finally:
            self.stop()
            for context in self.contexts:
                context.remove_listener("page", self.attach)
            # All captures stop accepting immediately, then drain concurrently within 10s.
            for capture in self.captures.values():
                capture.closed = True
            results = await asyncio.gather(*(c.stop() for c in self.captures.values()), return_exceptions=True)
            for result in results:
                if isinstance(result, Exception):
                    self.fail(result, "capture_close_failed")
            self.export()
        self.check()
