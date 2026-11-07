"""Synthetic passive-recording tests; browser traffic is intercepted locally."""
import asyncio
import json
from pathlib import Path
import socket
import subprocess
import tempfile
import unittest

from source.collection.cli import version_info
from source.collection.config import Config
from source.collection.passive import (Coverage, PassiveCapture, PassiveRecorder,
                                       RateWatch, comment_count, note_items)
from source.collection.protocol import Request, classify
from source.collection.store import Store, encode

NOTE = "0123456789abcdef01234567"
OTHER = "fedcba9876543210fedcba98"


def comment(id_, parent=None, replies=0, children=()):
    item = {"id": id_, "note_id": NOTE, "user_info": {"user_id": "u-" + id_}}
    if parent:
        item["root_comment_id"] = parent
    else:
        item.update(sub_comment_count=replies, sub_comment_has_more=bool(children),
                    sub_comment_cursor="c1", sub_comments=list(children))
    return item


def envelope(items, more=False, cursor=""):
    return {"success": True, "code": 0, "data": {"comments": items, "has_more": more, "cursor": cursor}}


class FakeCDP:
    def __init__(self):
        self.calls = []
        self.handlers = {}

    def on(self, name, handler):
        self.handlers[name] = handler

    async def send(self, method, params=None):
        self.calls.append(method)
        return {}


class FakeContext:
    def __init__(self, cdp):
        self.cdp = cdp

    async def new_cdp_session(self, page):
        return self.cdp


class FakePage:
    """Records every page-level call so tests can prove nothing is injected."""

    def __init__(self, cdp):
        self.context = FakeContext(cdp)
        self.url = f"https://www.xiaohongshu.com/explore/{NOTE}"
        self.calls = []

    def __getattr__(self, name):
        async def forbidden(*args, **kwargs):
            self.calls.append(name)
            raise AssertionError(f"passive mode must not drive the page: {name}")
        return forbidden


class PassiveCaptureTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()

    def tearDown(self):
        self.temp.cleanup()

    async def test_only_network_domain_is_enabled_and_nothing_is_injected(self):
        cdp = FakeCDP()
        page = FakePage(cdp)
        store = Store(Path(self.temp.name), Config())
        try:
            capture = PassiveCapture(PassiveRecorder(store, "http://127.0.0.1:9222"), page)
            await capture.start()
            self.assertEqual(cdp.calls, ["Network.enable"])
            self.assertEqual(set(cdp.handlers), {"Network.requestWillBeSent", "Network.responseReceived",
                                                 "Network.loadingFinished", "Network.loadingFailed"})
            self.assertEqual(page.calls, [])
        finally:
            store.close()

    async def test_non_xiaohongshu_traffic_is_not_recorded(self):
        cdp = FakeCDP()
        store = Store(Path(self.temp.name), Config())
        try:
            recorder = PassiveRecorder(store, "http://127.0.0.1:9222")
            capture = PassiveCapture(recorder, FakePage(cdp))
            await capture.start()
            capture.on_request({"requestId": "1", "type": "XHR",
                                "request": {"url": "https://example.com/api", "method": "GET"}})
            self.assertEqual(capture.requests, {})
            self.assertEqual(recorder.rate.counts(), (0, 0))
        finally:
            store.close()


class ParsingTests(unittest.TestCase):
    def test_note_items_requires_both_id_and_token(self):
        payload = {"data": {"items": [
            {"id": NOTE, "xsec_token": "tok", "note_card": {}},
            {"id": OTHER},
            {"id": "not-a-note", "xsec_token": "tok"},
            {"note_id": OTHER, "xsec_token": "tok2"},
        ]}}
        self.assertEqual(sorted(note_items(payload)), [(NOTE, "tok"), (OTHER, "tok2")])

    def test_comment_count_reads_nested_interact_info(self):
        payload = {"data": {"note": {"interact_info": {"comment_count": "128"}}}}
        self.assertEqual(comment_count(payload), 128)
        self.assertEqual(comment_count({"data": {}}), None)


class CoverageTests(unittest.TestCase):
    def test_repeated_and_out_of_order_pages_do_not_double_count(self):
        coverage = Coverage()
        child = comment("r1", parent="c1")
        first = envelope([comment("c1", replies=3, children=[child])], more=True, cursor="p2")
        self.assertEqual(coverage.observe("comments", first), 2)  # new root + embedded reply
        self.assertEqual(coverage.observe("comments", first), 0)
        self.assertEqual(coverage.observe("replies", envelope([comment("r2", parent="c1")])), 1)
        record = coverage.record()
        self.assertEqual(record["top_level_comments"], 1)
        self.assertEqual(record["replies"], 2)
        self.assertEqual(record["comments_total"], 3)
        self.assertEqual(record["reply_coverage"], {"advertised": 3, "captured": 2, "ratio": round(2 / 3, 4)})
        self.assertEqual(record["threads_partially_seen"], ["c1"])

    def test_reply_coverage_is_not_called_completeness(self):
        coverage = Coverage()
        coverage.observe("comments", envelope([comment("c1", replies=40)], more=True))
        record = coverage.record()
        self.assertNotIn("complete_observed", record)
        self.assertFalse(record["primary_end_seen"])
        self.assertEqual(record["threads_partially_seen"], ["c1"])

    def test_detail_supplies_advertised_note_total(self):
        coverage = Coverage()
        coverage.observe("detail", {"success": True, "data": {"note": {"interact_info": {"comment_count": 99}}}})
        self.assertTrue(coverage.record()["body_saved"])
        self.assertEqual(coverage.record()["advertised_note_comments"], 99)


class RateWatchTests(unittest.TestCase):
    def test_slow_browsing_is_not_flagged(self):
        watch = RateWatch()
        for i in range(5):
            watch.add(now=1000.0 + i * 5)
        state = watch.state(now=1020.0)
        self.assertFalse(state["fast"])
        self.assertEqual(state["last_10s"], 2)

    def test_burst_like_the_observed_461_is_flagged(self):
        watch = RateWatch()
        for i in range(35):
            watch.add(now=1000.0 + i * 0.29)
        state = watch.state(now=1010.0)
        self.assertTrue(state["fast"])
        self.assertIn("放慢", state["advice"])


class RecorderTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.store = Store(Path(self.temp.name), Config())
        self.messages = []
        self.recorder = PassiveRecorder(self.store, "http://127.0.0.1:9222", on_event=self.messages.append)

    def tearDown(self):
        self.store.close()
        self.temp.cleanup()

    def observe(self, url, payload, kind=None):
        req = classify(url)
        self.assertIsNotNone(req, url)
        self.recorder.observe(req, encode(payload), 200)
        return req

    def test_search_response_builds_a_persistent_pending_list(self):
        self.observe("https://edith.xiaohongshu.com/api/sns/web/v1/search/notes",
                     {"success": True, "data": {"items": [{"id": NOTE, "xsec_token": "tok"}]}})
        tasks = self.store.tasks()
        self.assertEqual([t["note_id"] for t in tasks], [NOTE])
        self.assertEqual(tasks[0]["status"], "pending")
        self.assertIn("xsec_token=tok", tasks[0]["url"])

    def test_comments_are_counted_and_archived(self):
        url = f"https://edith.xiaohongshu.com/api/sns/web/v2/comment/page?note_id={NOTE}&cursor="
        req = self.observe(url, envelope([comment("c1", replies=1, children=[comment("r1", parent="c1")])]))
        self.store.save(None, req.note, "network_json", b"{}", {"url": url})
        report = self.recorder.report()
        self.assertEqual(report["comments_total"], 2)
        self.assertEqual(report["notes"][NOTE]["top_level_comments"], 1)

    def test_summary_matches_report_without_the_per_note_detail(self):
        url = f"https://edith.xiaohongshu.com/api/sns/web/v2/comment/page?note_id={NOTE}&cursor="
        self.observe(url, envelope([comment("c1", replies=1, children=[comment("r1", parent="c1")])]))
        summary, report = self.recorder.summary(), self.recorder.report()
        self.assertNotIn("notes", summary)
        self.assertEqual(summary, {k: v for k, v in report.items() if k != "notes"})

    def test_restriction_is_reported_without_any_retry(self):
        self.observe(f"https://edith.xiaohongshu.com/api/sns/web/v2/comment/sub/page?note_id={NOTE}&root_comment_id=c1",
                     {"success": True, "code": 300013, "msg": "访问频繁，请稍后再试", "data": {}})
        self.assertTrue(self.recorder.report()["notes"][NOTE]["access_restricted"])
        self.assertTrue(any("访问限制" in m for m in self.messages))

    def test_export_writes_report(self):
        self.recorder.export()
        report = json.loads((Path(self.temp.name) / "passive_report.json").read_text())
        self.assertEqual(report["mode"], "passive")


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class PassiveBrowserTests(unittest.IsolatedAsyncioTestCase):
    """End-to-end over real CDP against a local Chromium. All traffic is fixture-routed."""

    async def asyncSetUp(self):
        from playwright.async_api import async_playwright

        self.temp = tempfile.TemporaryDirectory()
        self.port = free_port()
        self.playwright = await async_playwright().start()
        self.profile = Path(self.temp.name) / "chrome"
        self.process = subprocess.Popen(
            [self.playwright.chromium.executable_path, "--headless=new", f"--user-data-dir={self.profile}",
             f"--remote-debugging-port={self.port}", "--remote-debugging-address=127.0.0.1",
             "--no-first-run", "--no-default-browser-check", "about:blank"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.cdp = f"http://127.0.0.1:{self.port}"
        for _ in range(100):
            try:
                await asyncio.to_thread(version_info, self.cdp)
                break
            except Exception:
                await asyncio.sleep(0.1)
        else:
            self.fail("local Chromium CDP did not become ready")
        self.store = Store(Path(self.temp.name) / "batch", Config())
        self.recorder = PassiveRecorder(self.store, self.cdp)
        self.task = asyncio.create_task(self.recorder.run(status_interval=0.05))
        self.browser = await self.playwright.chromium.connect_over_cdp(self.cdp)
        self.context = self.browser.contexts[0] if self.browser.contexts else await self.browser.new_context()
        await self.context.route("**/*", self.route)

    async def asyncTearDown(self):
        self.recorder.stop()
        await asyncio.gather(self.task, return_exceptions=True)
        self.store.close()
        # connect_over_cdp().close() can merely disconnect. Ask our own fixture
        # browser to flush and exit before deleting its temporary profile.
        from playwright.async_api import Error
        try:
            session = await self.browser.new_browser_cdp_session()
            await session.send("Browser.close")
        except Error:
            pass  # Browser.close may close the transport before its reply.
        await self.playwright.stop()
        try:
            await asyncio.to_thread(self.process.wait, timeout=5)
        except subprocess.TimeoutExpired:
            self.process.terminate()
            await asyncio.to_thread(self.process.wait, timeout=5)
        import errno
        for attempt in range(20):
            try:
                self.temp.cleanup()
                break
            except OSError as exc:
                # On macOS a fixture helper may finish its final profile write
                # just after the browser exits. Do not hide other cleanup errors.
                if exc.errno != errno.ENOTEMPTY or attempt == 19:
                    raise
                await asyncio.sleep(.05)

    async def route(self, route):
        url = route.request.url
        if "/comment/page" in url:
            payload = envelope([comment("c1", replies=2, children=[comment("r1", parent="c1")])])
            await route.fulfill(status=200, content_type="application/json", body=encode(payload))
        elif "/search/notes" in url:
            payload = {"success": True, "data": {"items": [{"id": OTHER, "xsec_token": "tok"}]}}
            await route.fulfill(status=200, content_type="application/json", body=encode(payload))
        elif "/explore/" in url:
            await route.fulfill(status=200, content_type="text/html", body="<html><body>note</body></html>")
        else:
            await route.fulfill(status=200, content_type="text/plain", body="")

    async def wait_for(self, predicate, timeout=10):
        deadline = asyncio.get_running_loop().time() + timeout
        while asyncio.get_running_loop().time() < deadline:
            if predicate():
                return True
            await asyncio.sleep(0.1)
        return False

    async def test_manual_browsing_is_archived_without_any_injected_globals(self):
        page = await self.context.new_page()
        # Wait for the Network listeners, exactly as the CLI tells the user to.
        self.assertTrue(await self.wait_for(lambda: self.recorder.report()["pages_ready"] >= 1))
        await page.goto(f"https://www.xiaohongshu.com/explore/{NOTE}?xsec_token=fixture-token")
        await page.evaluate("u => fetch(u).then(r => r.text())",
                            f"https://edith.xiaohongshu.com/api/sns/web/v2/comment/page?note_id={NOTE}&cursor=")
        await page.evaluate("u => fetch(u).then(r => r.text())",
                            "https://edith.xiaohongshu.com/api/sns/web/v1/search/notes")
        self.assertTrue(await self.wait_for(lambda: self.recorder.report()["comments_total"] == 2))
        self.assertTrue(await self.wait_for(lambda: OTHER in self.recorder.discovered))
        # The page never received our adapters: passive mode attaches Network listeners only.
        self.assertFalse(await page.evaluate("Boolean(window.__xhsCollect || window.__xhsReadOnly)"))
        report = self.recorder.report()
        self.assertEqual(report["notes"][NOTE]["top_level_comments"], 1)
        self.assertEqual(report["notes"][NOTE]["replies"], 1)
        self.assertTrue(report["notes"][NOTE]["body_saved"])
        self.assertGreaterEqual(report["rate"]["last_60s"], 3)
        rows = self.store.db.execute("SELECT kind,note_id FROM events").fetchall()
        self.assertIn(("document_html", NOTE), [(r["kind"], r["note_id"]) for r in rows])
        self.assertTrue((Path(self.temp.name) / "batch" / "passive_report.json").exists())


if __name__ == "__main__":
    unittest.main()
