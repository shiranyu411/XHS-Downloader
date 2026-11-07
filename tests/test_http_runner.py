"""Narrow offline checks for the new API path; no browser or platform requests."""
import asyncio
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch
from urllib.parse import urlencode

from source.collection.config import Config
from source.collection.http_runner import HTTPRunner, ApiClientUnavailable, ApiRestricted, ApiFailure, decode_payload, normalized_note, REPLIES, COMMENT, Stopped
from source.collection.store import Store

NOTE, ROOT, CHILD = 'a' * 24, 'b' * 24, 'c' * 24
URL = f'https://www.xiaohongshu.com/explore/{NOTE}?xsec_token=fixture&xsec_source=pc_search'


def envelope(comments):
    return {'success': True, 'code': 0, 'data': {'comments': comments, 'has_more': False, 'cursor': ''}}


class FakeTransport:
    headers = {'user-agent': 'fixture', 'referer': 'https://www.xiaohongshu.com/'}
    identity = {'fixture': True}

    def __init__(self, restricted=False, restricted_path=None):
        self.paths = []
        self.calls = []
        self.sent = self.successful = 0
        self.restricted = restricted
        self.restricted_path = restricted_path

    async def request(self, method, path, data):
        self.paths.append(path)
        self.calls.append((method, path, dict(data)))
        self.sent += 1
        if self.restricted and (self.restricted_path is None or self.restricted_path == path):
            payload, status = {'success': False, 'code': 300013, 'msg': '访问频繁，请稍后再试'}, 461
        elif path.endswith('/feed'):
            status = 200
            payload = {'success': True, 'code': 0, 'data': {'items': [{
                'id': NOTE, 'note_card': {'note_id': NOTE, 'title': 'fixture', 'desc': '完整正文',
                                        'image_list': [], 'user': {'user_id': 'fixture'}}}]}}
        elif path.endswith('/sub/page'):
            status = 200
            payload = envelope([{'id': CHILD, 'note_id': NOTE, 'root_comment_id': ROOT,
                                 'content': '这是有效回复', 'user_info': {'user_id': 'reply-user'}}])
        else:
            status = 200
            payload = envelope([{'id': ROOT, 'note_id': NOTE, 'content': '这是有效评论',
                                 'user_info': {'user_id': 'root-user'}, 'sub_comments': [],
                                 'sub_comment_count': 1, 'sub_comment_has_more': True,
                                 'sub_comment_cursor': 'r0'}])
        self.successful += status == 200
        url = 'https://edith.xiaohongshu.com' + path + ('?' + urlencode(data) if method == 'GET' else '')
        return json.dumps(payload).encode(), status, {'url': url, 'duration_seconds': .01}

    async def close(self):
        pass


class FakeMedia:
    def __init__(self, store, checkpoint):
        self.downloader = None

    async def __call__(self, note, url):
        await asyncio.sleep(.001)
        return 'success'

    async def comments(self, note):
        return 'success'

    async def close(self):
        pass


class ProtocolTests(unittest.TestCase):
    def test_http_refusal_overrides_success_envelope(self):
        with self.assertRaises(ApiRestricted):
            decode_payload(json.dumps({'success': True, 'code': 0}).encode(), 461)

    def test_malformed_and_business_failure_are_not_empty_success(self):
        for raw, status in [(b'<html>error</html>', 200), (b'{"success":false,"code":300015}', 200)]:
            with self.assertRaises(ApiFailure):
                decode_payload(raw, status)

    def test_retry_is_explicit_and_not_a_manual_verification_claim(self):
        from source.collection.cli import parser
        args = parser().parse_args(['resume', '--retry-api-once'])
        self.assertTrue(args.retry_api_once)
        self.assertFalse(args.confirmed_access)
        self.assertFalse(parser().parse_args(['resume']).retry_api_once)

    def test_wrong_note_is_not_saved_as_requested_note(self):
        with self.assertRaises(ApiFailure):
            normalized_note({'data': {'items': [{'id': CHILD, 'note_card': {'title': 'wrong'}}]}}, NOTE)


class RunnerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.store = Store(Path(self.temp.name), Config())
        self.store.enqueue([URL], {'keyword': 'fixture'})
        self.media_patch = patch('source.collection.http_runner.MediaCollector', FakeMedia)
        self.download_patch = patch('source.application.download.Download.for_collection', return_value=SimpleNamespace(headers={}))
        self.media_patch.start()
        self.download_patch.start()

    async def asyncTearDown(self):
        self.download_patch.stop()
        self.media_patch.stop()
        self.store.close()
        self.temp.cleanup()

    async def test_shared_client_failure_stops_queue_and_is_durable(self):
        class BrokenClient(FakeTransport):
            async def request(self, method, path, data):
                self.sent += 1
                raise ApiClientUnavailable('webpack_unavailable')
        self.store.enqueue([URL.replace(NOTE, f'{i:024x}') for i in range(30)], {})
        transport = BrokenClient()
        runner = HTTPRunner(self.store, transport)
        await runner.run()
        self.assertEqual(transport.sent, 1)
        self.assertTrue(runner.paused)
        self.assertEqual(self.store.control('workspace_blocked'), 'api_client_unavailable')
        self.assertEqual(json.loads(self.store.control('api_stop_evidence'))['reason'], 'webpack_unavailable')
        again = FakeTransport()
        await HTTPRunner(self.store, again).run()
        self.assertEqual(again.sent, 0)

    async def test_full_pagination_and_reuse_without_second_fetch(self):
        t = FakeTransport()
        await HTTPRunner(self.store, t).run()
        self.assertEqual([p.rsplit('/', 2)[-2:] for p in t.paths],
                         [['v1', 'feed'], ['comment', 'page'], ['sub', 'page']])
        row = self.store.tasks()[0]
        self.assertEqual((row['body'], row['comments'], row['media'], row['comment_media']),
                         ('saved', 'complete_observed', 'success', 'success'))
        self.assertEqual(self.store.db.execute("SELECT count(*) FROM events WHERE kind='network_json'").fetchone()[0], 3)
        again = FakeTransport()
        await HTTPRunner(self.store, again).run()
        self.assertEqual(again.sent, 0)

    async def refuse_reply(self):
        transport = FakeTransport(restricted=True, restricted_path=REPLIES)
        await HTTPRunner(self.store, transport).run()
        self.assertEqual(transport.paths[-1], REPLIES)
        self.assertEqual(self.store.control('workspace_blocked'), 'api_access_restricted')
        return transport, json.loads(self.store.control('api_stop_evidence'))

    async def test_explicit_retry_commits_exact_page_and_continues_without_duplicate_requests(self):
        first, proof = await self.refuse_reply()
        resumed = FakeTransport()
        runner = HTTPRunner(self.store, resumed, retry_api_once=True)
        await runner.run()
        self.assertEqual(resumed.calls, [first.calls[-1]])
        self.assertEqual(self.store.control('workspace_blocked'), '')
        self.assertFalse(runner.paused)
        self.assertEqual(self.store.tasks()[0]['comments'], 'complete_observed')
        receipt = json.loads(self.store.control('api_resume_evidence'))
        self.assertEqual(receipt['previous_event_id'], proof['event_id'])
        self.assertFalse(receipt['manual_verification_claimed'])
        self.assertEqual(self.store.db.execute("SELECT count(*) FROM events WHERE kind='restriction_acknowledged'").fetchone()[0], 0)
        again = FakeTransport()
        await HTTPRunner(self.store, again).run()
        self.assertEqual(again.sent, 0)

    async def test_reviewed_account_error_retries_only_the_confirmed_event(self):
        class AccountError(FakeTransport):
            async def request(self, method, path, data):
                raw, status, detail = await super().request(method, path, data)
                if status == 461:
                    raw = b'{"code":300011,"msg":"account abnormal"}'
                return raw, status, detail

        first = AccountError(True, REPLIES)
        await HTTPRunner(self.store, first).run()
        event = json.loads(self.store.control('api_stop_evidence'))['event_id']
        resumed = FakeTransport()
        runner = HTTPRunner(self.store, resumed)
        runner.session_id = 'reviewed-account-recovery'
        for confirmed in (None, event - 1):
            with self.assertRaisesRegex(Stopped, 'api_retry_requires_access_review'):
                await runner.retry_restricted_page(confirmed_event_id=confirmed)
        self.assertEqual(resumed.sent, 0)
        await runner.retry_restricted_page(confirmed_event_id=event)
        self.assertEqual(resumed.calls, [first.calls[-1]])
        self.assertEqual(self.store.control('workspace_blocked'), '')
        self.assertTrue(json.loads(self.store.control('api_resume_evidence'))['manual_verification_claimed'])

    async def test_reviewed_login_retries_one_page_and_does_not_review_new_refusal(self):
        await self.assert_reviewed_login_scoped('124', COMMENT)

    async def test_reviewed_217_after_relogin_is_scoped_to_one_reply_refusal(self):
        await self.assert_reviewed_login_scoped('217', REPLIES)

    async def assert_reviewed_login_scoped(self, verify_type, restricted_path):
        class LoginChallenge(FakeTransport):
            async def request(self, method, path, data):
                raw, status, detail = await super().request(method, path, data)
                if status == 461:
                    raw = b'{"code":0,"msg":"login verification"}'
                    detail.update(verify_type=verify_type, verification_id_present=True)
                return raw, status, detail

        first = LoginChallenge(True, restricted_path)
        await HTTPRunner(self.store, first).run()
        event = json.loads(self.store.control('api_stop_evidence'))['event_id']
        automatic = FakeTransport()
        await HTTPRunner(self.store, automatic, retry_api_once=True).run()
        self.assertEqual(automatic.sent, 0)
        still_blocked = LoginChallenge(True, restricted_path)
        runner = HTTPRunner(self.store, still_blocked)
        runner.session_id = 'reviewed-login-refused'
        for confirmed in (None, event - 1):
            with self.assertRaisesRegex(Stopped, 'api_retry_requires_access_review'):
                await runner.retry_restricted_page(confirmed_login_event_id=confirmed)
        self.assertEqual(still_blocked.sent, 0)
        with self.assertRaisesRegex(Stopped, 'api_retry_failed'):
            await runner.retry_restricted_page(confirmed_login_event_id=event)
        self.assertEqual(still_blocked.calls, [first.calls[-1]])
        new_event = json.loads(self.store.control('api_stop_evidence'))['event_id']
        self.assertGreater(new_event, event)
        self.assertEqual(self.store.control('workspace_blocked'), 'api_access_restricted')
        self.assertIsNone(self.store.control('api_resume_evidence'))
        resumed = FakeTransport()
        runner = HTTPRunner(self.store, resumed)
        runner.session_id = 'reviewed-login-restored'
        with self.assertRaisesRegex(Stopped, 'api_retry_requires_access_review'):
            await runner.retry_restricted_page(confirmed_login_event_id=event)
        self.assertEqual(resumed.sent, 0)
        await runner.retry_restricted_page(confirmed_login_event_id=new_event)
        self.assertEqual(resumed.calls, [first.calls[-1]])
        self.assertEqual(self.store.control('workspace_blocked'), '')
        receipt = json.loads(self.store.control('api_resume_evidence'))
        self.assertEqual(receipt['previous_event_id'], new_event)
        self.assertTrue(receipt['manual_login_claimed'])
        self.assertFalse(receipt['manual_verification_claimed'])

    async def test_explicit_retry_refused_again_sends_only_one_request(self):
        first, proof = await self.refuse_reply()
        resumed = FakeTransport(restricted=True)
        runner = HTTPRunner(self.store, resumed, retry_api_once=True)
        await runner.run()
        self.assertEqual(resumed.calls, [first.calls[-1]])
        self.assertTrue(runner.paused)
        self.assertEqual(self.store.control('workspace_blocked'), 'api_access_restricted')
        self.assertGreater(json.loads(self.store.control('api_stop_evidence'))['event_id'], proof['event_id'])
        self.assertIsNone(self.store.control('api_resume_evidence'))

    async def test_explicit_retry_does_not_override_verification_login_or_server_wait(self):
        _, proof = await self.refuse_reply()
        row = self.store.db.execute('SELECT metadata FROM events WHERE id=?', (proof['event_id'],)).fetchone()
        original = json.loads(row[0])
        for change in ({'verification_id_present': True}, {'verify_type': 'captcha'},
                       {'status': 401}, {'status': 471}, {'retry_after': '3600'},
                       {'retry_after': 'Sun, 01 Nov 2099 15:00:00 GMT'}, {'retry_after': 'invalid'}):
            with self.subTest(change=change):
                with self.store.db:
                    self.store.db.execute('UPDATE events SET metadata=? WHERE id=?',
                                          (json.dumps({**original, **change}), proof['event_id']))
                resumed = FakeTransport()
                runner = HTTPRunner(self.store, resumed, retry_api_once=True)
                await runner.run()
                self.assertEqual(resumed.sent, 0)
                self.assertTrue(runner.paused)
                self.assertEqual(self.store.control('workspace_blocked'), 'api_access_restricted')

    async def test_explicit_retry_respects_existing_cooldown(self):
        import time
        await self.refuse_reply()
        self.store.control('pacing_cooldown_until', time.time() + 3600)
        resumed = FakeTransport()
        await HTTPRunner(self.store, resumed, retry_api_once=True).run()
        self.assertEqual(resumed.sent, 0)
        self.assertEqual(self.store.control('workspace_blocked'), 'api_access_restricted')

    async def test_invalid_success_payload_does_not_unlock_or_start_queue(self):
        await self.refuse_reply()

        class InvalidTransport(FakeTransport):
            async def request(self, method, path, data):
                _, status, detail = await super().request(method, path, data)
                return b'{"success":true,"code":0,"data":{}}', status, detail

        resumed = InvalidTransport()
        runner = HTTPRunner(self.store, resumed, retry_api_once=True)
        await runner.run()
        self.assertEqual(resumed.paths, [REPLIES])
        self.assertTrue(runner.paused)
        self.assertEqual(self.store.control('workspace_blocked'), 'api_access_restricted')
        self.assertIsNone(self.store.control('api_resume_evidence'))

    async def test_retry_does_not_clear_unrelated_block(self):
        self.store.control('workspace_blocked', 'api_protocol_or_transport_failures')
        resumed = FakeTransport()
        await HTTPRunner(self.store, resumed, retry_api_once=True).run()
        self.assertEqual(resumed.sent, 0)
        self.assertEqual(self.store.control('workspace_blocked'), 'api_protocol_or_transport_failures')

    async def test_scoped_deferral_keeps_reply_pending_and_explicit_recovery_fills_it(self):
        await self.refuse_reply()
        self.store.control('api_reply_blocked', self.store.control('api_stop_evidence'))
        self.store.control('workspace_blocked', '')
        self.store.run['api_options'] = {'defer_replies': True}
        primary = FakeTransport()
        await HTTPRunner(self.store, primary).run()
        self.assertEqual(primary.sent, 0, 'already-saved primary page must not be fetched again')
        self.assertEqual(self.store.tasks()[0]['comments'], 'partial')
        self.assertEqual(self.store.control('api_primary_stage_complete'), '1')
        self.assertTrue(self.store.control('api_reply_blocked'))
        resumed = FakeTransport()
        await HTTPRunner(self.store, resumed, retry_api_once=True).run()
        self.assertEqual(resumed.paths, [REPLIES])
        self.assertEqual(self.store.tasks()[0]['comments'], 'complete_observed')
        self.assertEqual(self.store.control('api_reply_blocked'), '')
        self.assertFalse(self.store.run['api_options']['defer_replies'])

    async def test_scoped_deferral_continues_later_primary_pages_without_reply_requests(self):
        class MorePrimary(FakeTransport):
            async def request(self, method, path, data):
                raw, status, detail = await super().request(method, path, data)
                if path == COMMENT and status == 200:
                    payload = json.loads(raw) if not data.get('cursor') else envelope([])
                    if not data.get('cursor'):
                        payload['data'].update(has_more=True, cursor='p2')
                    raw = json.dumps(payload).encode()
                return raw, status, detail
        await HTTPRunner(self.store, MorePrimary(True, REPLIES)).run()
        self.store.control('api_reply_blocked', self.store.control('api_stop_evidence'))
        self.store.control('workspace_blocked', '')
        self.store.run['api_options'] = {'defer_replies': True}
        primary = MorePrimary()
        await HTTPRunner(self.store, primary).run()
        self.assertEqual(primary.paths, [COMMENT])
        self.assertEqual(primary.calls[0][2]['cursor'], 'p2')
        self.assertEqual(self.store.tasks()[0]['comments'], 'partial')

    async def test_global_primary_refusal_and_older_reply_scope_both_recover(self):
        class PrimaryHistory(FakeTransport):
            fail_primary = False
            async def request(self, method, path, data):
                raw, status, detail = await super().request(method, path, data)
                if path == COMMENT and status == 200:
                    if not data.get('cursor'):
                        payload = json.loads(raw)
                        payload['data'].update(has_more=True, cursor='p2')
                    elif self.fail_primary:
                        payload = {'success': False, 'code': 300013, 'msg': '访问频繁'}
                        status = 461
                        self.successful -= 1
                    else:
                        payload = envelope([])
                    raw = json.dumps(payload).encode()
                return raw, status, detail
        await HTTPRunner(self.store, PrimaryHistory(True, REPLIES)).run()
        self.store.control('api_reply_blocked', self.store.control('api_stop_evidence'))
        self.store.control('workspace_blocked', '')
        self.store.run['api_options'] = {'defer_replies': True}
        failed = PrimaryHistory()
        failed.fail_primary = True
        await HTTPRunner(self.store, failed).run()
        self.assertEqual(json.loads(self.store.control('api_stop_evidence'))['path'], COMMENT)
        restored = PrimaryHistory()
        await HTTPRunner(self.store, restored, retry_api_once=True).run()
        self.assertEqual(restored.paths, [COMMENT, REPLIES])
        self.assertEqual(restored.calls[0][2]['cursor'], 'p2')
        self.assertEqual(self.store.control('workspace_blocked'), '')
        self.assertEqual(self.store.control('api_reply_blocked'), '')
        self.assertFalse(self.store.run['api_options']['defer_replies'])
        self.assertEqual(self.store.tasks()[0]['comments'], 'complete_observed')

    async def test_recovery_page_with_new_images_does_not_skip_media_work(self):
        from source.collection.media import register_comments as original_register
        await self.refuse_reply()
        self.store.control('api_reply_blocked', self.store.control('api_stop_evidence'))
        self.store.control('workspace_blocked', '')
        self.store.run['api_options'] = {'defer_replies': True}
        await HTTPRunner(self.store, FakeTransport()).run()
        self.assertEqual(self.store.tasks()[0]['comment_media'], 'success')
        def register_with_image(store, note, payload, request, event):
            original_register(store, note, payload, request, event)
            store.db.execute("INSERT INTO media_files(note_id,comment_id,media_id,kind,urls,metadata,status) VALUES(?,?,?,?,?,?,?)",
                             (NOTE, CHILD, 'new-image', 'image', '[]', '{}', 'pending'))
        with patch('source.collection.http_runner.register_comments', side_effect=register_with_image), \
             patch.object(FakeMedia, 'comments', new_callable=AsyncMock, return_value='partial') as media:
            await HTTPRunner(self.store, FakeTransport(), retry_api_once=True).run()
            self.assertEqual(media.await_count, 1)
        self.assertEqual(self.store.tasks()[0]['comments'], 'complete_observed')
        self.assertEqual(self.store.tasks()[0]['comment_media'], 'partial')

    async def test_reply_scope_guard_blocks_accidental_dispatch(self):
        self.store.control('api_reply_blocked', '{"event_id":1}')
        transport = FakeTransport()
        runner = HTTPRunner(self.store, transport)
        with self.assertRaises(Stopped):
            await runner.fetch(None, NOTE, 'GET', REPLIES, {})
        self.assertEqual(transport.sent, 0)

    async def test_primary_or_detail_refusal_still_stops_all_requests(self):
        self.store.control('api_reply_blocked', '{"event_id":1}')
        self.store.run['api_options'] = {'defer_replies': True}
        transport = FakeTransport(restricted=True)
        runner = HTTPRunner(self.store, transport)
        await runner.run()
        self.assertEqual(transport.sent, 1)
        self.assertEqual(self.store.control('workspace_blocked'), 'api_access_restricted')
        self.assertTrue(runner.paused)

    async def test_refusal_is_durable_and_does_not_restart_or_switch(self):
        t = FakeTransport(restricted=True)
        runner = HTTPRunner(self.store, t)
        await runner.run()
        self.assertEqual(t.sent, 1)
        self.assertEqual(self.store.control('workspace_blocked'), 'api_access_restricted')
        evidence = json.loads(self.store.control('api_stop_evidence'))
        self.assertEqual(evidence['http_status'], 461)
        self.assertTrue(runner.paused)
        again = FakeTransport()
        await HTTPRunner(self.store, again).run()
        self.assertEqual(again.sent, 0)


if __name__ == '__main__':
    unittest.main()
