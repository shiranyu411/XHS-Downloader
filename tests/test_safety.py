"""Synthetic safety regressions for workflow scope, storage faults and access policy."""
import asyncio
from copy import deepcopy
import json
from pathlib import Path
import sqlite3
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

from source.GUI.backend import GuiBackend
from source.GUI.workbench_service import WorkbenchService
from source.collection.config import Config
from source.collection.observations import Coverage, ObservationIndex
from source.collection.pacing import Pacer, PacingPause
from source.collection.passive import PassiveCapture, PassiveRecorder
from source.collection.protocol import Request
from source.collection.store import Store, encode
from source.collection.workspace import Workspace
from source.module.settings import Settings
from test_pacing_resume import FakeTime

A, B = 'a' * 24, 'b' * 24
def URL(n):
    return f'https://www.xiaohongshu.com/explore/{n}?xsec_token=fixture'

UNPACED = dict(open_interval=0, page_interval=0, max_requests_per_minute=0, session_request_budget=0)


def payload(comments):
    return {'success': True, 'code': 0, 'data': {'comments': comments, 'has_more': False}}


class ObservationTests(unittest.TestCase):
    def test_reply_uses_request_parent_and_deduplicates_embedded_reply(self):
        c = Coverage()
        c.observe(Request('comments', A), payload([{'id':'root', 'sub_comment_count':2, 'sub_comments':[{'id':'r1'}]}]))
        c.observe(Request('replies', A, 'root'), payload([{'id':'r1'}, {'id':'r2'}]))
        self.assertEqual(c.replies, {'root': {'r1','r2'}})
        self.assertEqual(c.record()['threads_partially_seen'], [])
        self.assertEqual(c.count(), 3)

    def test_unknown_and_conflicting_parent_are_not_assigned_silently(self):
        c = Coverage()
        c.observe(Request('replies', A), payload([{'id':'orphan'}]))
        c.observe(Request('replies', A, 'root'), payload([{'id':'wrong', 'root_comment_id':'other'}]))
        self.assertEqual(c.replies, {})
        self.assertEqual(c.record()['unassigned_replies'], 2)
        self.assertEqual(len(c.record()['problems']), 2)

    def test_failed_response_cannot_claim_body_or_comments(self):
        index = ObservationIndex()
        for kind in ('detail', 'comments'):
            index.observe(Request(kind, A), encode({'success':False, 'data':{'comments':[{'id':'bad'}]}}), 200)
        self.assertEqual(index.summary()['comments_total'], 0)
        self.assertFalse(index.coverage[A].body)


class RecorderTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.store = Store(Path(self.temp.name), Config(**UNPACED))
        self.messages = []
        self.recorder = PassiveRecorder(self.store, 'unused', on_event=self.messages.append)
        self.cdp = SimpleNamespace(on=lambda *_: None,
            send=AsyncMock(return_value={'body':encode(payload([{'id':'c1'}])).decode()}), detach=AsyncMock())
        context = SimpleNamespace(new_cdp_session=AsyncMock(return_value=self.cdp))
        self.capture = PassiveCapture(self.recorder, SimpleNamespace(context=context))

    def tearDown(self):
        self.store.close()
        self.temp.cleanup()

    async def begin(self):
        await self.capture.start()
        self.capture.on_request({'requestId':'1', 'request':{'method':'GET', 'url':f'https://edith.xiaohongshu.com/api/sns/web/v2/comment/page?note_id={A}'}})
        self.capture.on_response({'requestId':'1', 'response':{'status':200}})

    async def test_stop_drains_late_response_but_rejects_new_requests(self):
        await self.begin()
        stop = asyncio.create_task(self.capture.stop(timeout=1))
        await asyncio.sleep(.03)
        self.capture.on_request({'requestId':'2', 'request':{'method':'GET', 'url':f'https://edith.xiaohongshu.com/api/sns/web/v2/comment/page?note_id={B}'}})
        self.capture.on_finished({'requestId':'1'})
        await stop
        self.assertEqual(self.recorder.summary()['comments_total'], 1)
        self.assertEqual(self.recorder.summary()['capture_gaps'], 0)
        self.assertEqual(self.capture.requests, {})
        self.cdp.detach.assert_awaited_once()

    async def test_stop_timeout_records_gap(self):
        await self.begin()
        await self.capture.stop(timeout=.02)
        self.assertEqual(self.recorder.summary()['capture_gaps'], 1)
        self.assertEqual(self.store.db.execute("SELECT count(*) FROM events WHERE kind='passive_gap'").fetchone()[0], 1)

    async def test_unreadable_body_is_durable_gap(self):
        await self.begin()
        self.cdp.send.side_effect = RuntimeError('fixture unreadable')
        self.capture.on_finished({'requestId':'1'})
        await self.capture.stop(timeout=1)
        self.assertEqual(self.recorder.summary()['capture_gaps'], 1)
        self.assertEqual(ObservationIndex.rebuild(self.store.db, self.store.root).gaps, 1)

    async def test_disk_error_propagates_and_stops_recorder(self):
        await self.begin()
        with patch.object(self.store, 'save', side_effect=OSError('fixture disk full')):
            self.capture.on_finished({'requestId':'1'})
            with self.assertRaises(OSError):
                await self.capture.stop(timeout=1)
        self.assertTrue(self.recorder.stopped)
        self.assertTrue(self.recorder.summary()['failed'])
        self.assertTrue(any('archive_write_failed' in m for m in self.messages))

    async def test_listener_start_failure_is_not_silenced(self):
        self.cdp.send.side_effect = RuntimeError('fixture attach error')
        await self.capture.start()
        with self.assertRaises(RuntimeError):
            await self.capture.stop()
        self.assertTrue(self.recorder.stopped)

    async def test_461_headers_lock_even_if_body_cannot_be_read(self):
        await self.begin()
        self.capture.on_response({'requestId':'1', 'response':{'status':461}})
        self.assertTrue(self.recorder.stopped)
        self.assertEqual(self.store.control('workspace_blocked'), 'access_restricted')
        await self.capture.stop(timeout=0)
        self.assertGreater(Pacer(self.store).cooldown_until(), 0)
        self.assertTrue(ObservationIndex.rebuild(self.store.db, self.store.root).access_restricted)

    async def test_cumulative_report_survives_second_empty_session(self):
        await self.begin()
        self.capture.on_finished({'requestId':'1'})
        await self.capture.stop(timeout=1)
        self.recorder.export()
        again = PassiveRecorder(self.store, 'unused')
        again.export()
        report = json.loads((self.store.root/'passive_report.json').read_text())
        self.assertEqual(report['comments_total'], 1)
        self.assertEqual(report['session']['comments_total'], 0)
        self.assertEqual(len(list((self.store.root/'passive_sessions').glob('*.json'))), 2)


class Workflows(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.backend = GuiBackend()
        self.backend.settings = deepcopy(Settings.default)
        self.backend.settings['disclaimer_accepted'] = True
        self.backend.task_queue = asyncio.Queue()
        async def result(*args, **kwargs):
            kwargs['result_callback']({'success':1, 'fail':0, 'skip':0})
        self.backend.xhs = SimpleNamespace(extract_links=AsyncMock(return_value=[URL(B)]),
            extract=AsyncMock(side_effect=result), manager=SimpleNamespace(download_record=False),
            html=SimpleNamespace(access_restriction=None))
        self.host = SimpleNamespace(endpoint='http://127.0.0.1:9234', connect=AsyncMock(side_effect=AssertionError('no network')))
        self.service = WorkbenchService(self.root, self.backend, self.host)
        self.backend.workbench = self.service
        self.service.select(self.root/'batch', create=True, values=UNPACED)
        self.store = self.service.store
        self.store.enqueue([URL(A)], {})

    async def asyncTearDown(self):
        await self.service.close()
        self.temp.cleanup()

    async def restart(self):
        await self.service.close()
        self.service = WorkbenchService(self.root, self.backend, self.host)
        self.backend.workbench = self.service
        self.store = self.service.store

    async def test_new_download_never_resumes_old_paused_task(self):
        self.store.submit([A], 'download')
        self.store.job_state(A, 'download', 'paused')
        await self.backend.create_tasks(URL(B))
        await self.service.job
        self.backend.xhs.extract.assert_awaited_once()
        self.assertEqual(self.backend.xhs.extract.await_args.args[0], URL(B))
        self.assertEqual([r['note_id'] for r in self.store.pending('download')], [A])

    async def test_dismissed_task_stays_dismissed_after_export_and_restart(self):
        await self.backend.create_tasks(URL(B))
        await self.service.job
        self.assertEqual(await self.backend.clear_finished(), 1)
        await self.service.command('export')
        await self.restart()
        self.assertEqual(self.backend.tasks, {})
        self.assertEqual(self.store.db.execute('select count(*) from workspace_jobs').fetchone()[0], 1)

    async def test_retry_projection_has_only_current_job_identity(self):
        old = self.store.submit([A], 'download')[0]
        self.store.job_state(A, 'download', 'failed')
        self.service.sync_download_tasks()
        new = self.store.submit([A], 'download', retry=True)[0]
        self.service.sync_download_tasks()
        self.assertNotEqual(old, new)
        self.assertEqual(set(self.backend.tasks), {new})

    async def test_passive_restriction_locks_all_jobs_and_survives_restart(self):
        recorder = PassiveRecorder(self.store, 'unused', on_restriction=self.service.restrict)
        recorder.observe(Request('comments', A), encode({'code':300013}), 461)
        for mode in ('comments','discover','download','passive'):
            self.assertFalse(self.service.permission(mode)['can_start'])
        until = Pacer(self.store).cooldown_until()
        await self.restart()
        self.assertTrue(self.service.blocked)
        await self.service.command('ack_restriction')
        self.assertTrue(self.service.permission('passive')['can_start'])
        for mode in ('comments','discover','download'):
            self.assertFalse(self.service.permission(mode)['can_start'])
        self.assertEqual(Pacer(self.store).cooldown_until(), until)
        with self.assertRaises(ValueError):
            await self.backend.create_tasks(URL(B))
        self.backend.xhs.extract_links.assert_not_awaited()
        self.host.connect.assert_not_awaited()

    async def test_config_revision_survives_restart_and_keeps_guards(self):
        p = Pacer(self.store)
        p.record()
        self.service.restrict()
        controls = dict(self.store.db.execute("select key,value from controls where key like 'pacing_%' or key like 'workspace_%'"))
        old = self.store.config
        await self.service.command('update_config', {'config':{'page_interval':33},'expected_revision':0})
        self.assertEqual(self.store.config.page_interval, 33)
        revision = self.store.run['config_revision']
        saved = json.loads((self.store.root/'config_revisions'/f'{revision}.json').read_text())
        self.assertEqual(saved['before']['config']['page_interval'], old.page_interval)
        await self.restart()
        self.assertEqual(self.store.config.page_interval, 33)
        self.assertEqual(dict(self.store.db.execute("select key,value from controls where key like 'pacing_%' or key like 'workspace_%'")), controls)
        with self.assertRaises(ValueError):
            self.service.update_config({'page_interval':44}, expected_revision=0)
        with self.assertRaises(ValueError):
            self.service.update_config({'cdp':'http://127.0.0.1:9999'})

    async def test_config_edit_while_running_rejected(self):
        self.service.job = asyncio.create_task(asyncio.sleep(1))
        with self.assertRaises(ValueError):
            self.service.update_config({'page_interval':33})
        self.service.job.cancel()
        await asyncio.gather(self.service.job, return_exceptions=True)

    async def test_config_write_failure_restores_original_config(self):
        before = (self.store.root/'run.json').read_bytes()
        with patch.object(self.store, 'save', side_effect=OSError('fixture failure')):
            with self.assertRaises(OSError):
                self.service.update_config({'page_interval':99})
        self.assertEqual((self.store.root/'run.json').read_bytes(), before)
        self.assertEqual(self.store.config.page_interval, 0)

    async def test_preview_rejects_changed_scope(self):
        self.store.submit([A], 'download')
        preview = self.service.preview('download')
        self.assertEqual(preview['note_ids'], [A])
        self.store.enqueue([URL(B)], {})
        self.store.submit([B], 'download')
        with self.assertRaisesRegex(ValueError, '队列已变化'):
            self.service.start('download', preview_token=preview['token'])
        self.assertFalse(self.service.busy)

    async def test_corruption_invalidates_success_job_and_allows_explicit_retry(self):
        self.store.submit([A], 'comments')
        attempt = self.store.begin(A)
        self.store.save(attempt, A, 'network_json', b'{}')
        self.store.finish(attempt, {'body':'saved','comments':'complete_observed','media':'disabled','reason':'observed_end'})
        self.store.job_state(A, 'comments', 'success')
        path = self.store.root / self.store.db.execute('select path from events limit 1').fetchone()[0]
        await self.service.close()
        path.write_bytes(b'fixture corrupted')
        self.service = WorkbenchService(self.root, self.backend, self.host)
        self.backend.workbench = self.service
        self.store = self.service.store
        self.assertEqual(self.store.overview()[0]['collection_job'], 'partial')
        self.assertEqual(len(self.store.submit([A], 'comments', retry=True)), 1)
        self.assertEqual(self.store.db.execute('select count(*) from attempts').fetchone()[0], 1)
        self.store.save(None, A, 'network_json', b'{}')
        self.assertEqual(self.store.db.execute('select count(*) from events where valid=0').fetchone()[0], 1)
        backups = list((self.store.root/'diagnostics'/'corrupt_blobs').glob('*.gz'))
        self.assertEqual(len(backups), 1)
        self.assertEqual(backups[0].read_bytes(), b'fixture corrupted')

    async def test_observed_column_does_not_claim_complete(self):
        self.store.save(None, A, 'network_json', encode(payload([{'id':'c1'}])),
                        {'mode':'passive','kind':'comments','note_id':A,'status':200})
        await self.restart()
        row = self.store.overview()[0]
        self.assertEqual(row['observed']['comments_total'], 1)
        self.assertEqual(row['comments'], 'not_requested')


class PersistentPacing(unittest.IsolatedAsyncioTestCase):
    async def test_budget_reached_before_pause_is_persisted_immediately(self):
        with tempfile.TemporaryDirectory() as root:
            store = Workspace(Path(root), Config(**{**UNPACED, 'session_request_budget':2, 'session_rest':600}))
            try:
                t = FakeTime()
                p = Pacer(store, t.clock, t.sleep)
                p.record()
                p.record()
                again = Pacer(store, t.clock, t.sleep)
                self.assertEqual(again.session_count, 2)
                self.assertEqual(again.session_id, p.session_id)
                with self.assertRaises(PacingPause):
                    await again.wait()
                t.now += 601
                await again.wait()
                self.assertEqual(again.session_count, 0)
                self.assertNotEqual(again.session_id, p.session_id)
            finally:
                store.close()

    async def test_discovery_obeys_persisted_budget_without_js_timers(self):
        from source.collection.runner import Runner, Paused
        with tempfile.TemporaryDirectory() as root:
            store = Store(Path(root), Config(**{**UNPACED, 'session_request_budget':2, 'session_rest':60}))
            try:
                steps = []
                async def evaluate(expression, arg=None):
                    if "state('')" in expression:
                        return {'blocked':False}
                    if 'discoveryProgress' in expression:
                        return {'links':[], 'filters':[], 'count':len(steps), 'done':False}
                    if 'stepDiscovery' in expression:
                        steps.append(1)
                page = SimpleNamespace(url='https://www.xiaohongshu.com/search_result',
                    add_script_tag=AsyncMock(), content=AsyncMock(return_value='<html/>'), evaluate=evaluate)
                capture = SimpleNamespace(start=AsyncMock(), stop=AsyncMock(), check=lambda:None, ledger=SimpleNamespace(blocked=False))
                runner = Runner(store)
                with patch('source.collection.runner.Capture', return_value=capture):
                    with self.assertRaisesRegex(Paused, 'session_budget_reached'):
                        await runner.discover(SimpleNamespace(pages=[page]))
                self.assertEqual(len(steps), 2)
                self.assertEqual(Pacer(store).session_count, 2)
                capture.stop.assert_awaited_once()
            finally:
                store.close()

    async def test_rate_window_survives_restart(self):
        with tempfile.TemporaryDirectory() as root:
            store = Store(Path(root), Config(**{**UNPACED, 'max_requests_per_minute':2}))
            try:
                t = FakeTime()
                p = Pacer(store, t.clock, t.sleep)
                p.interval = lambda: 0
                p.record()
                p.record()
                again = Pacer(store, t.clock, t.sleep)
                again.interval = lambda: 0
                await again.wait()
                self.assertEqual(t.now, 1_000_060)
            finally:
                store.close()


class ReportRepairTests(unittest.TestCase):
    def test_apply_refuses_open_batch_then_backs_up_without_db_mutation(self):
        from source.collection.repair import repair_report
        import hashlib
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)/'batch'
            store = Store(root, Config())
            store.save(None, A, 'network_json', encode(payload([{'id':'c1'}])),
                        {'mode':'passive','kind':'comments','note_id':A,'status':200})
            report = root/'passive_report.json'
            report.write_bytes(b'{"legacy":true}')
            with self.assertRaises(RuntimeError):
                repair_report(root, apply=True)
            preview = repair_report(root, output=Path(tmp)/'preview.json')
            self.assertEqual(preview['comments_total'], 1)
            self.assertEqual(report.read_bytes(), b'{"legacy":true}')
            store.close()
            before = hashlib.sha256((root/'state.sqlite').read_bytes()).hexdigest()
            result = repair_report(root, apply=True)
            self.assertTrue(result['applied'])
            self.assertEqual(Path(result['backup']).read_bytes(), b'{"legacy":true}')
            self.assertEqual(json.loads(report.read_text())['comments_total'], 1)
            self.assertEqual(hashlib.sha256((root/'state.sqlite').read_bytes()).hexdigest(), before)


if __name__ == '__main__':
    unittest.main()
