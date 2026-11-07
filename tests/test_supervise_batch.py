"""Offline decisions of scripts/supervise_batch.py; launches and notifications are stubbed."""
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace
import tempfile
import time
import unittest
from unittest.mock import patch

from source.collection.config import Config
from source.collection.store import Store

SPEC = importlib.util.spec_from_file_location('supervise_batch', Path(__file__).resolve().parents[1] / 'scripts' / 'supervise_batch.py')
supervise = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(supervise)


class SupervisorDecisionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.batch = Path(self.temp.name).resolve()
        store = Store(self.batch, Config())
        store.enqueue(['https://www.xiaohongshu.com/explore/' + 'a' * 24], {})
        store.close()
        self.launched, self.notified = [], []
        self.supervisor = supervise.Supervisor(self.batch)
        self.supervisor.worker = lambda: None
        self.supervisor.chrome_ready = lambda: True
        self.supervisor.launch = self.fake_launch
        self.run_patch = patch.object(supervise.subprocess, 'run', side_effect=lambda cmd, **_: self.notified.append(cmd[-1]))
        self.run_patch.start()

    def tearDown(self):
        self.run_patch.stop()
        self.temp.cleanup()

    def fake_launch(self, reason):
        self.launched.append(reason)
        self.supervisor.state['launches'].append({'at': time.time(), 'pid': 1, 'reason': reason,
                                                  'finished_before': self.supervisor.finished()})

    def controls(self, **values):
        store = Store(self.batch)
        try:
            for key, value in values.items():
                store.control(key, value)
        finally:
            store.close()

    def decide(self, now=None):
        return self.supervisor.decide(now or time.time(), self.supervisor.controls())

    def test_pause_file_wins(self):
        (self.batch / '.pause-requested').write_text('pause')
        self.assertEqual(self.decide()[0], 'paused_by_user')
        self.assertEqual(self.launched, [])

    def test_captcha_or_login_waits_for_a_person_and_notifies_once(self):
        self.controls(workspace_blocked='login_or_verification_required')
        self.assertEqual(self.decide()[0], 'needs_person')
        self.assertEqual(self.decide()[0], 'needs_person')
        self.assertEqual((len(self.notified), self.launched), (1, []))

    def test_api_refusal_does_not_claim_login_or_automatically_retry(self):
        self.controls(workspace_blocked='api_access_restricted',
                      api_stop_evidence=json.dumps({'http_status': 461, 'business_code': 300013}))
        state, message, _ = self.decide()
        self.assertEqual(state, 'needs_person')
        self.assertIn('461', message)
        self.assertIn('300013', message)
        self.assertNotIn('ack-restriction --confirmed', message)
        self.assertEqual(self.launched, [])
        self.decide()
        self.assertEqual(len(self.notified), 1)

    def test_primary_stage_end_does_not_claim_full_completion_or_restart_loop(self):
        self.controls(api_reply_blocked='{"event_id":1}', api_primary_stage_complete='1')
        state, message, _ = self.decide()
        self.assertEqual(state, 'needs_person')
        self.assertIn('二级回复仍待恢复', message)
        self.assertIn('未宣称全量完成', message)
        self.assertEqual(self.launched, [])

    def test_api_internal_failure_does_not_request_login(self):
        self.controls(workspace_blocked='api_protocol_or_transport_failures')
        state, message, _ = self.decide()
        self.assertEqual(state, 'needs_person')
        self.assertNotIn('ack-restriction --confirmed', message)
        self.assertEqual(self.launched, [])

    def test_local_cdp_body_loss_resumes_without_acknowledging_other_errors(self):
        now = time.time()
        self.controls(workspace_blocked='api_protocol_or_transport_failures',
                      api_stop_evidence=json.dumps({'stage': 'native_cdp_body',
                          'error': 'No resource with given identifier found'}))
        self.supervisor.state['launches'] = [{'at': now - 120, 'ended': now,
            'blocked': 'api_protocol_or_transport_failures', 'progress': 0}]
        self.assertEqual(self.decide(now + 59)[0], 'backoff')
        (self.batch / '.pause-requested').touch()
        self.assertEqual(self.decide(now + 61)[0], 'paused_by_user')
        (self.batch / '.pause-requested').unlink()
        self.assertEqual(self.decide(now + 61)[0], 'collecting')
        self.assertEqual(self.launched, ['local_cdp_body_recovery'])
        self.assertEqual(self.supervisor.controls()['workspace_blocked'], '')

    def test_refusal_waits_out_cooldown_then_resumes_once_acknowledged(self):
        now = time.time()
        self.controls(workspace_blocked='access_restricted', pacing_cooldown_until=now + 7200,
                      pacing_restriction_log=json.dumps([now - 60]))
        self.assertEqual(self.decide(now)[0], 'cooldown')
        self.assertEqual(self.decide(now + 7201)[0], 'collecting')
        self.assertEqual(self.launched, ['after_cooldown'])
        store = Store(self.batch)
        try:
            self.assertEqual(store.control('workspace_blocked'), '')
            self.assertEqual(store.db.execute("SELECT count(*) FROM events WHERE kind='supervisor_resume'").fetchone()[0], 1)
        finally:
            store.close()

    def test_third_refusal_within_48h_needs_a_person(self):
        now = time.time()
        self.controls(workspace_blocked='access_restricted', pacing_cooldown_until=now - 1,
                      pacing_restriction_log=json.dumps([now - 40 * 3600, now - 7200, now - 60]))
        self.assertEqual(self.decide(now)[0], 'needs_person')
        self.controls(pacing_restriction_log=json.dumps([now - 49 * 3600, now - 7200, now - 60]))
        self.assertEqual(self.decide(now)[0], 'collecting')

    def test_refusal_threshold_can_be_disabled_for_an_attended_batch(self):
        now = time.time()
        self.controls(workspace_blocked='access_restricted', pacing_cooldown_until=now - 1,
                      pacing_restriction_log=json.dumps([now - 7200, now - 3600, now - 60]),
                      pacing_refusal_manual_threshold=0)
        self.assertEqual(self.decide(now)[0], 'collecting')
        self.assertEqual(self.launched, ['after_cooldown'])

    def test_exits_without_progress_back_off_then_stop(self):
        now = time.time()
        self.assertEqual(self.decide(now)[0], 'collecting')
        for minutes in (15, 30, 60):
            self.decide(now)  # records the exit of the last launch: no finished post
            self.assertEqual(self.decide(now + minutes * 60 - 5)[0], 'backoff')
            now += minutes * 60
            self.assertEqual(self.decide(now)[0], 'collecting')
        self.assertEqual(self.decide(now + 7200)[0], 'needs_person')
        (self.batch / '.supervisor-reset').touch()
        self.supervisor.tick()  # applies the reset
        self.assertEqual(self.decide(now + 7201)[0], 'collecting')
        self.assertEqual(len(self.launched), 5)

    def test_finished_batch_is_not_relaunched(self):
        store = Store(self.batch)
        try:
            store.finish(store.begin('a' * 24), {'body': 'saved', 'comments': 'sampled', 'reason': 'primary_end'}, 'finished')
        finally:
            store.close()
        self.assertEqual(self.decide()[0], 'finished')
        self.assertEqual(self.launched, [])

    def test_api_error_writes_are_not_progress_and_partial_is_not_finished(self):
        from datetime import datetime, timezone
        self.supervisor.api_mode = True
        now = time.time()
        store = Store(self.batch)
        try:
            attempt = store.begin('a' * 24)
            store.save(attempt, 'a' * 24, 'network_json', b'{}', {'business_success': True})
            store.db.execute('UPDATE events SET seen=?', (datetime.fromtimestamp(now - 4000, timezone.utc).isoformat(),))
            store.save(attempt, 'a' * 24, 'api_failure', b'{"reason":"webpack_unavailable"}')
            store.save(attempt, 'a' * 24, 'comment_progress', b'{}')
            store.finish(attempt, {'body': 'saved', 'comments': 'partial', 'reason': 'webpack_unavailable'}, 'finished')
        finally:
            store.close()
        self.assertEqual(self.supervisor.finished(), 0)
        self.assertEqual(self.supervisor.remaining({})[0], 1)
        with patch.object(supervise.os, 'kill') as kill:
            self.assertEqual(self.supervisor.watch(1234, now)[0], 'stopping_stalled')
            kill.assert_called_once_with(1234, supervise.signal.SIGTERM)

    def test_api_rate_limit_is_never_retried_or_restarted_automatically(self):
        store = Store(self.batch)
        try:
            event = store.save(None, None, 'network_json', b'{"code":300013}', {'status': 461, 'retry_after': '60'})
            store.control('workspace_blocked', 'api_access_restricted')
            store.control('api_stop_evidence', json.dumps({'event_id': event, 'http_status': 461, 'business_code': 300013}))
        finally:
            store.close()
        with patch.object(supervise.os, 'kill') as kill, patch.object(supervise.subprocess, 'Popen') as popen:
            self.assertEqual(self.decide()[0], 'needs_person')
            self.assertEqual(self.decide()[0], 'needs_person')
        kill.assert_not_called()
        popen.assert_not_called()
        self.assertEqual(self.launched, [])

    def test_closed_chrome_is_reported_not_retried(self):
        self.supervisor.chrome_ready = lambda: False
        self.assertEqual(self.decide()[0], 'needs_person')
        self.assertEqual(self.launched, [])


if __name__ == '__main__':
    unittest.main()
