"""V2 scope regressions. Synthetic archives only; the browser is never contacted."""
import asyncio
from copy import deepcopy
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch

from source.GUI.backend import GuiBackend
from source.GUI.workbench_service import WorkbenchService
from source.collection.workflow import parse_input
from source.module.settings import Settings

IDS = [f"{i:024x}" for i in range(1, 9)]

def url(id_):
    return f"https://www.xiaohongshu.com/explore/{id_}?xsec_token=fixture"


class ScopedOperations(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.backend = GuiBackend(workbench_root=self.root)
        self.backend.settings = {**deepcopy(Settings.default), "disclaimer_accepted": True, "work_path": str(self.root / "media")}
        runtime_guard = patch("source.GUI.backend.XHS", side_effect=AssertionError("no real downloader in scope fixtures"))
        runtime_guard.start()
        self.addCleanup(runtime_guard.stop)
        self.host = SimpleNamespace(endpoint="http://127.0.0.1:9234", connect=AsyncMock(side_effect=AssertionError("no network")), native=AsyncMock())
        self.service = WorkbenchService(self.root, self.backend, self.host)
        self.backend.workbench = self.service

    async def asyncTearDown(self):
        await self.service.close()
        self.host.connect.assert_not_awaited()
        self.temp.cleanup()

    def populate(self):
        self.service.ensure_store().enqueue([url(i) for i in IDS], {})
        store = self.service.store
        for id_, state in zip(IDS[1:7], ["pending", "paused", "interrupted", "partial", "failed", "success"]):
            store.submit([id_], "comments")
            store.job_state(id_, "comments", state)
        store.hide([IDS[7]])
        return store

    async def test_input_preview_is_offline_and_does_not_create_or_queue(self):
        preview = await self.service.command("preview_input", {"links": f"分享这篇 {url(IDS[0])}，打开查看", "kind": "comments"})
        self.assertEqual(preview["counts"]["new"], 1)
        self.assertEqual(preview["eligible_ids"], IDS[:1])
        self.assertIsNone(self.service.store)
        self.assertFalse((self.root / "collections").exists())
        self.assertFalse(self.service.busy)

    async def test_mixed_selection_has_distinct_start_resume_retry_scopes(self):
        store = self.populate()
        self.assertEqual(self.service.operation_preview({"ids": IDS[:7]})["eligible_ids"], IDS[:2])
        self.assertEqual(self.service.operation_preview({"ids": IDS[:7], "intent": "resume"})["eligible_ids"], IDS[1:4])
        self.assertEqual(self.service.operation_preview({"ids": IDS[:7], "intent": "retry"})["eligible_ids"], IDS[4:6])
        self.assertEqual(store.db.execute("SELECT count(*) FROM workspace_jobs").fetchone()[0], 6)
        with self.assertRaises(ValueError):
            self.service.operation_preview({"ids": IDS})

    async def test_new_input_never_starts_paused_or_another_pending_job(self):
        self.populate()
        request = {"links": url(IDS[0]), "kind": "comments"}
        preview = await self.service.command("preview_input", request)
        with patch.object(self.service, "_run", new=AsyncMock()) as run:
            result = await self.service.command("execute_input", {**request, "preview_token": preview["token"]})
            await self.service.job
        self.assertEqual(result["operation"]["note_ids"], IDS[:1])
        self.assertEqual([r["note_id"] for r in run.await_args.args[1]], IDS[:1])
        self.assertEqual(self.service.store.operation_rows()[IDS[2]]["comments_state"], "paused")
        self.assertEqual(self.service.store.operation_rows()[IDS[1]]["comments_state"], "pending")
        self.assertFalse(self.service.store.pending("download"))

    async def test_state_identity_config_and_batch_changes_invalidate_confirmations(self):
        store = self.populate()
        request = {"ids": IDS[:2], "kind": "comments"}
        preview = self.service.operation_preview(request)
        store.job_state(IDS[1], "comments", "paused")
        with self.assertRaisesRegex(ValueError, "已变化"):
            await self.service.command("execute_selection", {**request, "preview_token": preview["token"]})
        preview = self.service.operation_preview(request)
        self.service.update_config({"page_interval": 21})
        with self.assertRaisesRegex(ValueError, "已变化"):
            await self.service.command("execute_selection", {**request, "preview_token": preview["token"]})
        self.assertFalse(self.service.busy)
        preview = self.service.operation_preview(request)
        self.service.select(self.root / "other", create=True)
        self.service.store.enqueue([url(i) for i in IDS[:2]], {})
        with self.assertRaisesRegex(ValueError, "已变化"):
            await self.service.command("execute_selection", {**request, "preview_token": preview["token"]})

    async def test_queue_token_cannot_confirm_selection_and_double_click_cannot_run_twice(self):
        self.populate()
        request = {"ids": IDS[:2], "kind": "comments"}
        with self.assertRaisesRegex(ValueError, "已变化"):
            await self.service.command("execute_selection", {**request, "preview_token": self.service.preview("comments")["token"]})
        preview = self.service.operation_preview(request)
        with patch.object(self.service, "_run", new=AsyncMock()) as run:
            await self.service.command("execute_selection", {**request, "preview_token": preview["token"]})
            with self.assertRaises(ValueError):
                await self.service.command("execute_selection", {**request, "preview_token": preview["token"]})
            await self.service.job
        run.assert_awaited_once()

    async def test_submit_success_start_failure_reports_durable_queued_state(self):
        self.populate()
        request = {"ids": IDS[:1], "kind": "comments"}
        preview = self.service.operation_preview(request)
        with patch.object(self.service, "start", side_effect=ValueError("fixture failed to start")):
            result = await self.service.command("execute_selection", {**request, "preview_token": preview["token"]})
        self.assertEqual(result["operation"]["state"], "queued_not_started")
        self.assertEqual(self.service.store.operation_rows()[IDS[0]]["comments_state"], "pending")
        self.assertFalse(self.service.busy)

    async def test_duplicate_import_is_normal_and_keeps_exact_handoff(self):
        self.populate()
        result = await self.service.command("import", {"links": f"分享文字 {url(IDS[0])}\n{url(IDS[6])}"})
        self.assertEqual(result["handoff"]["new_ids"], [])
        self.assertEqual(result["handoff"]["existing_ids"], [IDS[0], IDS[6]])
        self.assertFalse(result["busy"])
        preview = self.service.operation_preview({"links": url(IDS[6])}, input_=True)
        self.assertEqual(preview["counts"]["complete"], 1)
        self.assertEqual(preview["count"], 0)

    async def test_short_links_need_explicit_resolution_and_are_refused_before_network(self):
        self.backend.xhs = SimpleNamespace(extract_links=AsyncMock(return_value=[url(IDS[0])]))
        request = {"links": "分享 http://xhslink.com/fixture", "kind": "comments"}
        preview = await self.service.command("preview_input", request)
        self.assertFalse(preview["can_start"])
        self.assertEqual(preview["short_count"], 1)
        self.backend.xhs.extract_links.assert_not_awaited()
        self.service.restrict("verification_required")
        with self.assertRaises(ValueError):
            await self.service.command("resolve_input", request)
        self.backend.xhs.extract_links.assert_not_awaited()
        self.assertIsNone(self.service.store)

    async def test_short_resolution_cannot_borrow_passive_cooldown_permission(self):
        import time
        store = self.service.ensure_store()
        store.control('pacing_cooldown_until', time.time()+300)
        self.backend.xhs = SimpleNamespace(extract_links=AsyncMock())
        with self.assertRaisesRegex(ValueError, '短链接解析'):
            await self.service.command('resolve_input', {'kind':'passive', 'links':'http://xhslink.com/fixture'})
        self.backend.xhs.extract_links.assert_not_awaited()

    async def test_close_waits_for_the_existing_command_owner(self):
        store = self.service.ensure_store()
        async with self.service.lock:
            closing = asyncio.create_task(self.service.close())
            await asyncio.sleep(.01)
            self.assertFalse(closing.done())
            self.assertIs(self.service.store, store)
            self.assertEqual(store.db.execute('SELECT count(*) FROM tasks').fetchone()[0], 0)
        await closing
        self.assertIsNone(self.service.store)

    async def test_media_settings_refuse_during_another_command_or_shutdown(self):
        self.backend.settings_manager = SimpleNamespace(update=Mock())
        with patch.object(self.backend, "_close_xhs", new_callable=AsyncMock) as close_runtime, patch.object(
            self.backend, "_create_xhs", new_callable=AsyncMock
        ) as create_runtime:
            async with self.service.lock:
                result = await self.backend.save_settings({"avg_delay": 90})
                self.assertFalse(result["ok"])
            self.service.closing = True
            result = await self.backend.save_settings({"avg_delay": 90})
            self.assertFalse(result["ok"])
        self.backend.settings_manager.update.assert_not_called()
        close_runtime.assert_not_awaited()
        create_runtime.assert_not_awaited()

    async def test_media_settings_reload_holds_the_existing_command_lock(self):
        self.backend.settings_manager = SimpleNamespace(update=Mock())
        entered, release = asyncio.Event(), asyncio.Event()

        async def close_runtime():
            entered.set()
            await release.wait()

        with patch.object(self.backend, "_close_xhs", side_effect=close_runtime), patch.object(
            self.backend, "_create_xhs", new_callable=AsyncMock
        ) as create_runtime:
            saving = asyncio.create_task(self.backend.save_settings({"avg_delay": 90}))
            try:
                await asyncio.wait_for(entered.wait(), 2)
                self.assertTrue(self.service.lock.locked())
                with self.assertRaisesRegex(ValueError, "另一个操作"):
                    await self.service.command("import", {"links": url(IDS[0])})
                self.assertIsNone(self.service.store)
            finally:
                release.set()
                result = await saving
        self.assertTrue(result["ok"])
        self.assertEqual(result["settings"]["avg_delay"], 90)
        self.backend.settings_manager.update.assert_called_once()
        create_runtime.assert_awaited_once()

    async def test_hide_counts_both_kinds_and_refuses_stale_side_effects(self):
        store = self.populate()
        preview = await self.service.command("hide_preview", {"ids": IDS[1:3]})
        self.assertEqual(preview["cancelled_jobs"], 2)
        store.submit([IDS[1]], "download")
        with self.assertRaisesRegex(ValueError, "已变化"):
            await self.service.command("hide", {"ids": IDS[1:3], "preview_token": preview["token"]})
        self.assertEqual(len(store.overview()), 7)

    async def test_export_filtered_and_all_are_not_limited_to_fifty(self):
        ids = [f"{i:024x}" for i in range(1, 124)]
        self.service.ensure_store().enqueue([url(i) for i in ids], {"mode": "fixture"})
        self.assertEqual(len(self.service.snapshot()["rows"]), 50)
        preview = self.service.export_preview({"scope": "all"})
        self.assertEqual(preview["count"], 123)
        filtered = self.service.export_preview({"scope": "filtered", "view_ids": ids[20:100]})
        self.assertEqual(filtered["note_ids"], ids[20:100])
        self.assertEqual(self.service.snapshot(ids=ids[20:100])["filtered"], 80)

    async def test_unknown_busy_stopping_and_cooling_permissions_are_explicit(self):
        self.service.ensure_store()
        self.backend.settings['disclaimer_accepted'] = False
        self.assertEqual(self.service.permission('comments')['reason_code'], 'consent_required')
        self.backend.settings['disclaimer_accepted'] = True
        self.service.job = asyncio.create_task(asyncio.sleep(10))
        self.service.pause()
        self.assertEqual(self.service.permission('comments')['reason_code'], 'stopping')
        self.assertIn('正在暂停', self.service.summary()['phase_text'])
        self.service.job.cancel()
        await asyncio.gather(self.service.job, return_exceptions=True)

    async def test_skipped_download_is_not_saved_and_paused_projection_stays_paused(self):
        store = self.service.ensure_store()
        store.enqueue([url(IDS[0]), url(IDS[1])], {})
        store.submit(IDS[:2], "download")
        store.job_state(IDS[1], "download", "paused")
        async def skip(task):
            task.state, task.reason = "skipped", "categories_disabled"
        self.backend._process_task = skip
        store.config = __import__("dataclasses").replace(store.config, page_interval=0, max_requests_per_minute=0)
        task = next(r for r in store.pending("download") if r["note_id"] == IDS[0])
        self.service.start("download", task_ids=[task["task_id"]])
        await self.service.job
        row = store.overview()[0]
        self.assertEqual((row["media"], row["download_job"], row["media_reason"]), ("skipped", "skipped", "categories_disabled"))
        self.assertEqual(self.service.summary()["counts"]["downloaded"], 0)
        paused = next(t for t in self.backend.tasks.values() if t.display_text == IDS[1])
        self.assertEqual(paused.state, "paused")


class InputParsing(unittest.TestCase):
    def test_share_text_keeps_tokens_and_deduplicates_note_ids_not_sources(self):
        first = url(IDS[0])
        result = parse_input(f"一篇分享（{first}）。\n另一个入口 {first}&xsec_source=pc_search")
        self.assertEqual(result["urls"], [first, first + "&xsec_source=pc_search"])
        self.assertEqual(result["note_ids"], IDS[:1])
        self.assertEqual(result["invalid_count"], 0)

    def test_foreign_and_author_urls_are_not_treated_as_notes(self):
        result = parse_input("https://evil.invalid/explore/" + IDS[0] + " https://www.xiaohongshu.com/user/profile/" + IDS[0])
        self.assertEqual(result["invalid_count"], 2)
        self.assertEqual(result["note_ids"], [])
