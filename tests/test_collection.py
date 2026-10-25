import asyncio
from copy import deepcopy
import gzip
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import AsyncMock, patch
from urllib.parse import parse_qs, urlsplit

from source.collection.capture import Capture
from source.collection.config import Config
from source.collection.protocol import Chain, Ledger, Request
from source.collection.request import request_url
from source.collection.store import Store
from source.collection.service import CollectionService

NOTE = "0123456789abcdef01234567"
URL = f"https://www.xiaohongshu.com/explore/{NOTE}?xsec_token=fixture-token"


def comment(id_, parent=None, target=None):
    result = {"id": id_, "note_id": NOTE, "content": "same text", "unknown_field": {"preserve": True},
              "user_info": {"user_id": "user-" + id_}, "target_comment": target}
    if parent:
        result["root_comment_id"] = parent
    else:
        result.update(sub_comments=[], sub_comment_has_more=False, sub_comment_cursor="")
    return result


def envelope(items, more=False, cursor=""):
    return {"success": True, "code": 0, "data": {"comments": items, "has_more": more, "cursor": cursor}}


class ProtocolTests(unittest.TestCase):
    def test_http_461_success_true_is_still_an_access_restriction(self):
        ledger = Ledger(NOTE)
        ledger.observe(Request("replies", NOTE, "root"),
                       {"success": True, "code": 300013, "msg": "访问频繁，请稍后再试", "data": {}}, 461)
        self.assertTrue(ledger.blocked)
        self.assertFalse(ledger.complete())
        self.assertIsNone(ledger.next_request())
        self.assertIn("business_or_http_error", ledger.gaps)

    def test_short_and_empty_pages_with_more_are_not_terminal(self):
        ledger = Ledger(NOTE)
        ledger.observe(Request("comments", NOTE), envelope([], True, "p1"))
        self.assertFalse(ledger.complete())
        self.assertEqual(ledger.next_request().cursor, "p1")
        ledger.observe(Request("comments", NOTE, cursor="p1"), envelope([comment("a")], True, "p2"))
        self.assertEqual(ledger.next_request().cursor, "p2")
        ledger.observe(Request("comments", NOTE, cursor="p2"), envelope([]))
        self.assertTrue(ledger.complete())

    def test_reply_to_reply_and_identical_text_are_preserved(self):
        root = comment("root")
        root.update(sub_comments=[comment("a", "root")], sub_comment_has_more=True, sub_comment_cursor="a")
        ledger = Ledger(NOTE)
        ledger.observe(Request("comments", NOTE), envelope([root]))
        req = ledger.next_request()
        self.assertEqual((req.kind, req.parent, req.cursor), ("replies", "root", "a"))
        target = {"id": "a", "user_info": {"user_id": "user-a"}, "other": 7}
        ledger.observe(req, envelope([comment("b", "root", target)]))
        self.assertTrue(ledger.complete())
        self.assertEqual(ledger.replies["root"].ids, {"a", "b"})
        self.assertEqual(ledger.comments["b"]["target_comment"], target)
        self.assertEqual(ledger.comments["b"]["user_id"], "user-b")
        self.assertEqual(ledger.record()["unresolved_targets"], [])

    def test_missing_flags_and_cycles_never_complete(self):
        for data in ({"cursor": "a"}, {"has_more": True, "cursor": ""}):
            chain = Chain()
            chain.observe("", data, ["a"])
            self.assertFalse(chain.complete())
            self.assertIsNone(chain.next_cursor())
            self.assertIn("a", chain.ids)
        chain = Chain()
        chain.observe("", {"has_more": True, "cursor": "a"}, [])
        chain.observe("a", {"has_more": True, "cursor": "b"}, [])
        chain.observe("b", {"has_more": True, "cursor": "a"}, [])
        self.assertIn("cursor_cycle", chain.problems)
        self.assertIsNone(chain.next_cursor())

    def test_wrong_ownership_and_business_failure_stop_scheduling(self):
        for req, payload in ((Request("comments", "other"), envelope([])),
                             (Request("comments", NOTE), {"success": False, "code": 123})):
            ledger = Ledger(NOTE)
            ledger.observe(req, payload)
            self.assertFalse(ledger.complete())
            self.assertIsNone(ledger.next_request())

    def test_same_cursor_with_changed_ids_is_not_a_single_snapshot(self):
        chain = Chain()
        chain.observe("", {"has_more": False}, ["a"])
        chain.observe("", {"has_more": False}, ["b"])
        self.assertFalse(chain.complete())
        self.assertIn("page_contents_changed", chain.problems)

    def test_sort_change_and_reply_count_contradiction_are_partial(self):
        ledger = Ledger(NOTE)
        ledger.observe(Request("comments", NOTE, scope=(("sort", "time"),)), envelope([], True, "next"))
        ledger.observe(Request("comments", NOTE, cursor="next", scope=(("sort", "hot"),)), envelope([]))
        self.assertIn("comment_query_scope_changed", ledger.gaps)
        ledger = Ledger(NOTE)
        root = comment("root")
        root["sub_comment_count"] = "2"
        ledger.observe(Request("comments", NOTE), envelope([root]))
        self.assertFalse(ledger.complete())
        self.assertIn("reply_count_mismatch", ledger.gaps)

    def test_observed_parameters_and_zero_cursor_survive(self):
        template = f"https://edith.xiaohongshu.com/api/sns/web/v2/comment/page?note_id={NOTE}&cursor=old&num=13&sort=time&xsec_token=abc"
        url = request_url(Request("comments", NOTE, cursor="0"), {"comments": template}, URL)
        query = parse_qs(urlsplit(url).query)
        self.assertEqual(query["num"], ["13"])
        self.assertEqual(query["sort"], ["time"])
        self.assertEqual(query["cursor"], ["0"])
        self.assertNotIn("count", query)


class ServiceTests(unittest.IsolatedAsyncioTestCase):
    async def test_gui_batch_creation_creates_parent_and_keeps_config(self):
        with tempfile.TemporaryDirectory() as root:
            service = CollectionService(Path(root))
            service._preflight = AsyncMock()
            service.start = AsyncMock()
            await service.create_start({
                "name": "pilot",
                "source": "links",
                "links": URL,
                "output": str(Path(root) / "nested" / "collections"),
                "config": {"cdp": "http://127.0.0.1:9223", "strategy": "request",
                           "open_interval": 1, "comments": True},
            })
            batch = Path(root) / "nested" / "collections" / "pilot"
            self.assertTrue((batch / "run.json").is_file())
            self.assertEqual(json.loads((batch / "run.json").read_text())["label"], "pilot")
            self.assertEqual(json.loads((batch / "run.json").read_text())["config"]["strategy"], "request")
            self.assertEqual(service.selected, batch.resolve())
            service.start.assert_awaited_once_with("resume", preflight=False)

    async def test_gui_rejects_invalid_link_batch_without_creating_directory(self):
        with tempfile.TemporaryDirectory() as root:
            service = CollectionService(Path(root))
            service._preflight = AsyncMock()
            with self.assertRaises(ValueError):
                await service.create_start({"name": "bad", "source": "links", "links": "https://example.com/x",
                                           "output": str(Path(root) / "out"), "config": {}})
            self.assertFalse((Path(root) / "out").exists())


class StoreTests(unittest.TestCase):
    def test_restart_preserves_attempts_and_does_not_join_snapshots(self):
        with tempfile.TemporaryDirectory() as root:
            store = Store(Path(root), Config())
            store.enqueue([URL], {"keyword": "fixture"})
            first = store.begin(NOTE)
            store.save(first, NOTE, "network_json", b"original", progress={"test": "old"})
            store.close()
            store = Store(Path(root))
            try:
                self.assertEqual(store.next_task()["status"], "interrupted")
                second = store.begin(NOTE)
                self.assertNotEqual(first, second)
                self.assertIsNone(store.db.execute("SELECT data FROM progress WHERE attempt=?", (second,)).fetchone())
                self.assertEqual(store.db.execute("SELECT status FROM attempts WHERE id=?", (first,)).fetchone()[0], "interrupted")
            finally:
                store.close()

    def test_corrupt_file_invalidates_complete_task(self):
        with tempfile.TemporaryDirectory() as root:
            store = Store(Path(root), Config())
            store.enqueue([URL], {})
            attempt = store.begin(NOTE)
            store.save(attempt, NOTE, "network_json", b"original")
            store.finish(attempt, {"body": "saved", "comments": "complete_observed", "media": "disabled", "reason": "observed_end"})
            path = next((Path(root) / "raw").glob("*/*.gz"))
            store.close()
            path.write_bytes(b"corrupt")
            store = Store(Path(root))
            try:
                self.assertEqual(store.next_task()["reason"], "archive_corrupt")
                self.assertEqual(len(store.recovery["invalid_events"]), 1)
            finally:
                store.close()


class CaptureTests(unittest.IsolatedAsyncioTestCase):
    async def test_cors_preflight_is_not_a_missing_comment_response(self):
        capture = Capture(None, None, NOTE, Ledger(NOTE))
        url = request_url(Request("comments", NOTE), {}, URL)
        capture.request({"requestId": "preflight", "type": "Preflight",
                         "request": {"url": url, "method": "OPTIONS"}})
        capture.response({"requestId": "preflight", "response": {"status": 200}})
        capture.finished({"requestId": "preflight"})
        self.assertFalse(capture.requests)
        self.assertFalse(capture.jobs)
        self.assertFalse(capture.ledger.gaps)
        capture.request({"requestId": "actual", "type": "XHR",
                         "request": {"url": url, "method": "GET"}})
        self.assertIn("actual", capture.requests)

    async def check_storage_failure(self, fail_kind):
        with tempfile.TemporaryDirectory() as root:
            store = Store(Path(root), Config())
            store.enqueue([URL], {})
            attempt = store.begin(NOTE)
            ledger = Ledger(NOTE)
            capture = Capture(store, attempt, NOTE, ledger)
            url = request_url(Request("comments", NOTE), {}, URL)
            op = capture.begin_operation(url)
            capture.request({"requestId": "r", "request": {"url": url, "method": "GET"}})
            capture.response({"requestId": "r", "response": {"status": 200}})

            class CDP:
                async def send(self, *_):
                    return {"body": json.dumps(envelope([]))}

            capture.cdp = CDP()
            real_save = store.save

            def fail(attempt, note, kind, *args, **kwargs):
                if kind == fail_kind:
                    raise sqlite3.OperationalError("disk full")
                return real_save(attempt, note, kind, *args, **kwargs)

            try:
                with patch.object(store, "save", side_effect=fail):
                    await capture.read_body("r")
                self.assertFalse(op["done"].done())
                self.assertFalse(ledger.complete())
                self.assertEqual(ledger.primary.edges, {})
                with self.assertRaises(sqlite3.OperationalError):
                    capture.check()
                self.assertIsNone(store.db.execute("SELECT data FROM progress WHERE attempt=?", (attempt,)).fetchone())
            finally:
                store.close()

    async def test_wrong_cursor_cannot_acknowledge_controlled_operation(self):
        with tempfile.TemporaryDirectory() as root:
            store = Store(Path(root), Config())
            store.enqueue([URL], {})
            capture = Capture(store, store.begin(NOTE), NOTE, Ledger(NOTE))
            url = request_url(Request("comments", NOTE), {}, URL)
            op = capture.begin_operation(url)
            wrong_url = request_url(Request("comments", NOTE, cursor="wrong"), {}, URL)
            capture.request({"requestId": "late", "request": {"url": wrong_url, "method": "GET"}})
            capture.response({"requestId": "late", "response": {"status": 200}})

            class CDP:
                async def send(self, *_):
                    return {"body": json.dumps(envelope([]))}

            capture.cdp = CDP()
            try:
                await capture.read_body("late")
                self.assertFalse(op["done"].done())
                self.assertFalse(capture.ledger.complete())
                metadata = json.loads(store.db.execute("SELECT metadata FROM events WHERE kind='network_json'").fetchone()[0])
                self.assertIsNone(metadata["operation_id"])
                self.assertEqual(metadata["cursor"], "wrong")
            finally:
                store.close()

    async def test_raw_write_failure_never_acknowledged(self):
        await self.check_storage_failure("network_json")

    async def test_progress_commit_failure_never_acknowledged(self):
        await self.check_storage_failure("comment_progress")


# These use a real local headless Chrome + CDP, but every website/API request is
# fulfilled by synthetic fixtures. They are NOT live-platform acceptance tests.
class BrowserTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        from playwright.async_api import async_playwright
        self.temp = tempfile.TemporaryDirectory()
        self.playwright = await async_playwright().start()
        chrome = Path("/Applications/Google Chrome.app/Contents/MacOS/Google Chrome")
        kwargs = {"executable_path": str(chrome)} if chrome.exists() else {}
        try:
            self.browser = await self.playwright.chromium.launch(headless=True, **kwargs)
        except Exception:
            await self.playwright.stop()
            self.temp.cleanup()
            raise
        self.context = await self.browser.new_context()
        self.calls = []
        self.mode = "full"
        self.responded = []
        await self.context.route("**/*", self.route)

    async def asyncTearDown(self):
        await self.browser.close()
        await self.playwright.stop()
        self.temp.cleanup()

    async def route(self, route):
        url = urlsplit(route.request.url)
        if url.hostname == "www.xiaohongshu.com":
            # Mimic current page shape: bare axios, namespace/default aliases of
            # the configured wrapper. A false bridge success must not override CDP.
            html = '''<!doctype html><html><body><script>
              window.__INITIAL_STATE__ = {note:{noteDetailMap:{'NOTE':{note:{noteId:'NOTE',title:'fixture'}}}}};
              const bare={get(){},interceptors:{request:{handlers:[]},response:{handlers:[]}}};
              const client={get:url=>fetch(url).then(r=>r.json()).then(()=>({success:true,data:{}})),
                makeUri(){},instance:bare,
                interceptors:{request:{},response:{},dispatch:{},result:{}}};
              const req={c:{'bare':{exports:bare},'fixture':{exports:{...client,default:client}}}};
              window.webpackChunkxhs_pc_web={push:chunk=>chunk[2](req)};
            </script></body></html>'''.replace("NOTE", NOTE)
            if self.mode == "unavailable":
                html += "<script>delete window.webpackChunkxhs_pc_web;</script><div class='comments-container'></div>"
            await route.fulfill(status=200, content_type="text/html", body=html)
        elif url.hostname == "edith.xiaohongshu.com":
            query = {k: v[0] for k, v in parse_qs(url.query, keep_blank_values=True).items()}
            self.calls.append((url.path, query))
            cursor = query["cursor"]
            status = 200
            if self.mode == "unavailable":
                payload = envelope([])
            elif self.mode == "blocked":
                payload, status = {"success": False, "code": -100, "msg": "登录失效"}, 403
            elif self.mode == "business_error":
                payload = {"success": False, "code": 123, "msg": "failed"}
            elif self.mode == "cycle":
                payload = envelope([], True, "same")
            elif "/sub/" in url.path:
                start = int(cursor.removeprefix("r"))
                stop = min(start + 20, 65)
                payload = envelope([comment(f"r{i}", "c0", {"id": f"r{i-1}", "other": "kept"})
                                    for i in range(start, stop)], stop < 65, f"r{stop}")
            elif cursor == "":
                items = [comment(f"c{i}") for i in range(20)]
                items[0].update(sub_comments=[comment("r0", "c0"), comment("r1", "c0")],
                                sub_comment_cursor="r2", sub_comment_has_more=True, sub_comment_count="65")
                payload = envelope(items, True, "primary2")
            else:
                payload = envelope([comment("c20")])
            if self.mode == "slow":
                await asyncio.sleep(0.25)
            self.responded.append(deepcopy(payload))
            await route.fulfill(status=status, content_type="application/json", body=json.dumps(payload),
                                headers={"Access-Control-Allow-Origin": "*"})
        else:
            await route.abort()

    async def collect(self, **options):
        from source.collection.runner import Runner
        config = Config(open_interval=0, note_timeout=15, operation_timeout=3, strategy="request", **options)
        self.store = Store(Path(self.temp.name), config)
        self.store.enqueue([URL], {"mode": "test_fixture"})
        runner = Runner(self.store)
        return await runner.collect(self.context, self.store.next_task())

    async def test_discovery_upgrades_bare_links_and_keeps_signed_dom_links(self):
        from source.collection.runner import ADAPTER
        page = await self.context.new_page()
        await page.goto('https://www.xiaohongshu.com/search_result?keyword=fixture')
        await page.evaluate("""note => {
            window.__INITIAL_STATE__.search = {feeds: [{id: note, noteCard: {}, xsecToken: 'fixture+/='}]};
            document.body.innerHTML = `<a href='/explore/${note}'>cover</a>`;
        }""", NOTE)
        await page.add_script_tag(path=str(ADAPTER))
        urls = await page.evaluate('window.__xhsCollect.links()')
        self.assertEqual(len(urls), 1)
        query = parse_qs(urlsplit(urls[0]).query)
        self.assertEqual(query['xsec_token'], ['fixture+/='])
        self.assertEqual(query['xsec_source'], ['pc_search'])
        await page.evaluate("""note => {
            document.body.innerHTML = `<a href='/explore/${note}?xsec_token=dom-fixture&xsec_source=pc_search'>signed</a>
                <a href='/explore/${note}'>bare</a>`;
        }""", NOTE)
        urls = await page.evaluate('window.__xhsCollect.links()')
        self.assertEqual(parse_qs(urlsplit(urls[0]).query)['xsec_token'], ['dom-fixture'])

    async def test_full_pagination_over_20_roots_and_50_replies(self):
        try:
            result = await self.collect()
            self.assertEqual(result["comments"], "complete_observed", result)
            self.assertEqual(len(self.calls), 6)
            row = self.store.db.execute("SELECT data FROM progress").fetchone()
            progress = json.loads(row[0])
            self.assertEqual(len(progress["primary"]["ids"]), 21)
            self.assertEqual(len(progress["replies"]["c0"]["ids"]), 65)
            self.assertEqual(progress["comments"]["r64"]["target_comment"]["id"], "r63")
            events = self.store.db.execute("SELECT path,metadata FROM events WHERE kind='network_json'").fetchall()
            self.assertEqual(len(events), 6)
            actual = [json.loads(gzip.decompress((self.store.root / r["path"]).read_bytes())) for r in events]
            self.assertEqual(actual, self.responded)
            self.assertTrue(all(json.loads(r["metadata"])["operation_id"] for r in events))
        finally:
            self.store.close()

    async def test_budget_is_partial_not_complete(self):
        try:
            result = await self.collect(max_operations=1)
            self.assertEqual(result["comments"], "partial")
            self.assertEqual(result["reason"], "operation_limit")
        finally:
            self.store.close()

    async def test_bridge_wrapping_cannot_hide_business_failure(self):
        self.mode = "business_error"
        try:
            result = await self.collect()
            self.assertEqual(result["comments"], "partial", result)
            self.assertEqual(len(self.calls), 1)
            self.assertEqual(result["reason"], "pagination_evidence_invalid")
        finally:
            self.store.close()

    async def test_repeated_cursor_stops(self):
        self.mode = "cycle"
        try:
            result = await self.collect()
            self.assertEqual(result["comments"], "partial", result)
            self.assertEqual(len(self.calls), 2)
        finally:
            self.store.close()

    async def test_auto_fallback_uses_new_attempt(self):
        from source.collection.runner import Runner
        self.mode = "unavailable"
        store = Store(Path(self.temp.name), Config(open_interval=0, strategy="auto"))
        store.enqueue([URL], {})
        runner = Runner(store)

        async def connect(_):
            return self.browser, self.context

        async def ui_action(page, note, capture, ledger):
            target = request_url(Request("comments", note), {}, URL)
            await page.evaluate("url => fetch(url).then(r => r.json())", target)
            await capture.drain()
            return "observed_end" if ledger.complete() else "fixture_failed"

        try:
            with patch.object(runner, "connect", side_effect=connect), patch.object(runner, "comments", side_effect=ui_action):
                await runner.run()
            attempts = store.db.execute("SELECT result FROM attempts ORDER BY rowid").fetchall()
            self.assertEqual(len(attempts), 2)
            first, second = [json.loads(r[0]) for r in attempts]
            self.assertEqual(first["reason"], "adapter_unavailable")
            self.assertEqual(second["strategy"], "ui")
            self.assertEqual(second["comments"], "complete_observed")
            self.assertEqual(len(self.calls), 1)
        finally:
            store.close()

    async def test_pause_drains_inflight_response_without_advancing(self):
        from source.collection.runner import Paused, Runner
        self.mode = "slow"
        store = Store(Path(self.temp.name), Config(open_interval=0, strategy="request"))
        store.enqueue([URL], {})
        runner = Runner(store)
        task = asyncio.create_task(runner.collect(self.context, store.next_task()))
        try:
            async with asyncio.timeout(5):
                while not self.calls:
                    await asyncio.sleep(0.01)
                runner.pause()
                with self.assertRaises(Paused):
                    await task
            self.assertEqual(len(self.calls), 1)
            self.assertEqual(store.tasks()[0]["status"], "paused")
            self.assertEqual(store.db.execute("SELECT count(*) FROM events WHERE kind='network_json'").fetchone()[0], 1)
            self.assertEqual(store.tasks()[0]["comments"], "partial")
        finally:
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            store.close()

    async def test_restriction_pauses_without_ui_fallback(self):
        from source.collection.runner import Paused
        self.mode = "blocked"
        try:
            with self.assertRaises(Paused):
                await self.collect()
            task = self.store.tasks()[0]
            self.assertEqual(task["status"], "paused")
            self.assertEqual(task["comments"], "blocked")
            self.assertEqual(len(self.calls), 1)
        finally:
            self.store.close()


if __name__ == "__main__":
    unittest.main()
