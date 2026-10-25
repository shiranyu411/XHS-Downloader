import asyncio
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock

from source.collection.config import Config
from source.collection.workspace import Workspace
from source.collection.store import Store
from source.GUI.workbench_service import WorkbenchService
import test_collection as fixtures

NOTE = fixtures.NOTE
URL = fixtures.URL
OTHER = "0" * 24
OTHER_URL = f"https://www.xiaohongshu.com/explore/{OTHER}?xsec_token=fixture-other"


class WorkspaceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.store = Workspace(self.root)

    def tearDown(self):
        self.store.close()
        self.temp.cleanup()

    def test_unique_items_multiple_sources_and_no_url_downgrade(self):
        self.store.enqueue([URL], {"mode": "search-a"})
        self.store.enqueue([URL.split("?")[0]], {"mode": "search-b"})
        self.assertEqual(len(self.store.tasks()), 1)
        self.assertEqual(self.store.links_for([NOTE]), [URL])
        self.assertEqual(self.store.overview()[0]["observations"], 2)

    def test_submissions_are_deduplicated_and_independent(self):
        self.store.enqueue([URL, OTHER_URL], {})
        self.assertEqual(len(self.store.submit([NOTE], "comments")), 1)
        self.assertEqual(self.store.submit([NOTE], "comments"), [])
        self.assertEqual(len(self.store.submit([NOTE], "download")), 1)
        self.assertEqual(len(self.store.pending("comments")), 1)
        self.assertEqual(len(self.store.pending("download")), 1)
        # No auto-enqueue just because another note was discovered.
        self.assertEqual(self.store.pending("comments")[0]["note_id"], NOTE)

    def test_retry_is_explicit_and_success_is_not_resubmitted(self):
        self.store.enqueue([URL], {})
        self.store.submit([NOTE], "comments")
        self.store.job_state(NOTE, "comments", "partial")
        self.assertEqual(self.store.submit([NOTE], "comments"), [])
        self.assertEqual(len(self.store.submit([NOTE], "comments", retry=True)), 1)
        self.store.job_state(NOTE, "comments", "success")
        self.assertEqual(self.store.submit([NOTE], "comments", retry=True), [])

    def test_hidden_items_keep_evidence_and_cancel_pending(self):
        self.store.enqueue([URL], {})
        self.store.save(None, NOTE, "fixture", b"original evidence")
        self.store.submit([NOTE], "comments")
        self.store.hide([NOTE])
        self.assertEqual(self.store.overview(), [])
        self.assertEqual(self.store.pending("comments"), [])
        self.assertEqual(
            self.store.db.execute("SELECT count(*) FROM events").fetchone()[0], 1
        )
        self.assertEqual(len(self.store.tasks()), 1)
        self.assertEqual(self.store.restore_hidden(), 1)
        self.assertEqual(len(self.store.overview()), 1)
        self.assertFalse(self.store.pending("comments"))

    def test_hide_validation_is_atomic(self):
        self.store.enqueue([URL, OTHER_URL], {})
        self.store.submit([NOTE], "comments")
        self.store.job_state(NOTE, "comments", "running")
        with self.assertRaises(ValueError):
            self.store.hide([OTHER, NOTE])
        self.assertEqual(len(self.store.overview()), 2)

    def test_restart_preserves_queue_without_marking_success(self):
        self.store.enqueue([URL], {})
        self.store.submit([NOTE], "download")
        self.store.job_state(NOTE, "download", "running")
        self.store.close()
        self.store = Workspace(self.root)
        self.assertEqual(self.store.overview()[0]["download_job"], "interrupted")
        self.assertEqual(self.store.overview()[0]["media"], "disabled")

    def test_comment_attempt_does_not_reset_media(self):
        self.store.enqueue([URL], {})
        with self.store.db:
            self.store.db.execute("UPDATE tasks SET media='success'")
        attempt = self.store.begin(NOTE)
        self.store.finish(
            attempt,
            {
                "body": "saved",
                "comments": "partial",
                "media": "disabled",
                "reason": "test",
            },
        )
        self.assertEqual(self.store.tasks()[0]["media"], "success")
        self.assertEqual(
            json.loads((self.root / "report.json").read_text())["tasks"][0]["media"],
            "success",
        )

    def test_legacy_batch_is_additive_and_not_auto_scheduled(self):
        legacy = self.root / "legacy"
        old = Store(legacy)
        old.enqueue([URL], {})
        old.close()
        workspace = Workspace(legacy)
        try:
            self.assertEqual(len(workspace.overview()), 1)
            self.assertFalse(workspace.pending("comments"))
        finally:
            workspace.close()


class WorkbenchFlowTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = fixtures.BrowserTests.asyncSetUp
    asyncTearDown = fixtures.BrowserTests.asyncTearDown
    route = fixtures.BrowserTests.route

    async def service(self):
        backend = SimpleNamespace(
            settings={"disclaimer_accepted": True},
            tasks={},
            files={},
            download_lock=asyncio.Lock(),
            _process_task=AsyncMock(),
        )
        host = SimpleNamespace(endpoint="http://127.0.0.1:9234")

        async def connect(p):
            return self.browser, self.context

        host.connect = connect
        host.new_page = self.context.new_page
        host.close_page = lambda page: page.close()
        host.native = AsyncMock()
        service = WorkbenchService(self.temp.name, backend, host)
        service.store = Workspace(
            Path(self.temp.name) / "batch",
            Config(open_interval=0, operation_timeout=3, strategy="request"),
        )
        service.store.enqueue([URL], {})
        return service

    async def test_cancelled_download_is_removed_from_durable_queue(self):
        service = await self.service()
        try:
            ids = service.store.submit([NOTE], "download")
            service.sync_download_tasks()
            self.assertIn(ids[0], service.backend.tasks)
            self.assertTrue(service.cancel_download(ids[0]))
            self.assertFalse(service.store.pending("download"))
            self.assertEqual(service.backend.tasks[ids[0]].state, "cancelled")
        finally:
            await service.close()

    async def test_direct_download_access_restriction_blocks_without_browser(self):
        from source.module.access import AccessRestricted

        service = await self.service()
        service.host.connect = AsyncMock(
            side_effect=AssertionError("media must not connect CDP")
        )
        service.backend._process_task.side_effect = AccessRestricted("http_461")
        try:
            service.store.submit([NOTE], "download")
            service.start("download")
            await service.job
            self.assertTrue(service.blocked)
            service.host.connect.assert_not_awaited()
            self.assertEqual(service.snapshot()["rows"][0]["download_job"], "paused")
            self.assertEqual(
                service.store.db.execute("SELECT count(*) FROM attempts").fetchone()[0],
                0,
            )
        finally:
            await service.close()

    async def test_shared_pool_comments_then_download_no_second_detail_request(self):
        service = await self.service()
        try:
            await service.command("submit", {"ids": [NOTE], "kind": "comments"})
            await service.command("start", {"mode": "comments"})
            await service.job
            row = service.snapshot()["rows"][0]
            self.assertEqual(row["comments"], "complete_observed")
            self.assertEqual(row["collection_job"], "success")
            before = len(self.calls)

            async def download(task):
                self.assertEqual(task.script_data["noteId"], NOTE)
                task.state = "success"

            service.backend._process_task.side_effect = download
            await service.command("submit", {"ids": [NOTE], "kind": "download"})
            await service.command("start", {"mode": "download"})
            await service.job
            row = service.snapshot()["rows"][0]
            self.assertEqual(row["media"], "success")
            self.assertEqual(row["comments"], "complete_observed")
            self.assertEqual(len(self.calls), before)
        finally:
            await service.close()

    async def test_restriction_blocks_other_queue_and_restart(self):
        service = await self.service()
        self.mode = "blocked"
        try:
            service.store.submit([NOTE], "comments")
            service.start("comments")
            await service.job
            self.assertTrue(service.blocked)
            service.store.submit([NOTE], "download")
            with self.assertRaises(ValueError):
                service.start("download")
            path = service.store.root
            service.store.close()
            service.store = Workspace(path)
            self.assertTrue(service.blocked)
            self.assertFalse(service.busy)
            self.assertEqual(service.snapshot()["rows"][0]["comments"], "blocked")
        finally:
            await service.close()

    async def test_pause_before_run_leaves_queue_pending(self):
        service = await self.service()
        try:
            service.store.submit([NOTE], "comments")
            service.start("comments")
            service.pause()
            await service.job
            self.assertFalse(self.calls)
            self.assertEqual(service.snapshot()["rows"][0]["collection_job"], "pending")
        finally:
            await service.close()

    async def test_discovery_only_never_starts_comment_collection(self):
        service = await self.service()
        try:
            page = await self.context.new_page()
            await page.goto("https://www.xiaohongshu.com/search_result?keyword=fixture")
            await page.evaluate(
                """note => {
                window.__INITIAL_STATE__.search={feeds:[{id:note,noteCard:{},xsecToken:'fixture-token'}]};
                document.body.innerHTML=`<a href='/explore/${note}'>fixture</a>`;
            }""",
                OTHER,
            )
            service.host.discovery_page = AsyncMock(return_value=page)
            service.start("discover", 0)
            await service.job
            self.assertEqual(service.snapshot()["counts"]["total"], 2)
            self.assertEqual(service.store.pending("comments"), [])
            self.assertEqual(
                service.store.db.execute("SELECT count(*) FROM attempts").fetchone()[0],
                0,
            )
            self.assertFalse(self.calls)
        finally:
            await service.close()

    async def test_busy_rejects_batch_change_without_stopping_task(self):
        service = await self.service()
        self.mode = "slow"
        try:
            service.store.submit([NOTE], "comments")
            service.start("comments")
            with self.assertRaises(ValueError):
                service.select(Path(self.temp.name) / "other", create=True)
            await service.job
            self.assertEqual(
                service.snapshot()["rows"][0]["comments"], "complete_observed"
            )
        finally:
            await service.close()


class BackendTaskTests(unittest.IsolatedAsyncioTestCase):
    async def test_mixed_download_failure_is_not_success(self):
        from source.GUI.backend import GuiBackend, TaskState

        backend = GuiBackend()

        async def extract(*args, **kwargs):
            kwargs["result_callback"]({"success": 1, "fail": 1, "skip": 0})

        backend.xhs = SimpleNamespace(
            extract=extract, manager=SimpleNamespace(download_record=False)
        )
        task = TaskState("fixture", URL)
        await backend._process_task(task)
        self.assertEqual(task.state, "failed")


if __name__ == "__main__":
    unittest.main()
