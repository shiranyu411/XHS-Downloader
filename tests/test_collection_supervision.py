import unittest

from source.collection.supervision import browser_resume_reason, retry_decision, stalled_worker_decision


class CollectionSupervisionTests(unittest.TestCase):
    def setUp(self):
        self.status = {'outcome': 'partial', 'access_restricted': False,
                       'tasks': [{'status': 'pending', 'body': 'partial'}],
                       'search': [{'keyword': 'fixture', 'status': 'running'}]}
        self.resume = {'status': 'launched', 'requested_at_epoch': 10,
                       'auto_retry_access': True, 'auto_retry_transient': True,
                       'wait_seconds': 1800}
        self.controls = {}

    def decide(self, **kwargs):
        return retry_decision(self.status, self.controls, self.resume,
                              **({'now': 100, 'alive': False, 'active': False, 'free_gib': 100} | kwargs))

    def test_live_pid_or_writer_lock_prevents_duplicate_worker(self):
        for alive, active in [(True, False), (False, True), (True, True)]:
            with self.subTest(alive=alive, active=active):
                self.assertEqual(self.decide(alive=alive, active=active)['action'], 'observe')

    def test_user_pause_cancels_even_during_cooldown_or_active_worker(self):
        self.controls['workspace_blocked'] = 'access_restricted'
        self.controls['pacing_cooldown_until'] = '50'
        self.assertEqual(self.decide(pause_mtime=11, active=True)['action'], 'cancel')
        self.resume['status'] = 'cancelled_by_user'
        self.assertEqual(self.decide()['action'], 'cancel')

    def test_old_pause_before_explicit_request_does_not_cancel(self):
        self.assertEqual(self.decide(pause_mtime=9)['action'], 'wait')

    def test_transient_wait_is_persisted_not_extended_each_poll(self):
        first = self.decide()
        self.assertEqual(first['not_before'], 1900)
        self.resume.update(status='waiting', not_before=first['not_before'], retry_reason=first['reason'])
        self.assertEqual(self.decide(now=1000)['not_before'], 1900)
        self.assertEqual(self.decide(now=1900)['action'], 'check_browser')

    def test_stronger_platform_wait_overrides_transient_deadline(self):
        self.resume.update(status='waiting', not_before=120)
        self.controls.update(workspace_blocked='access_restricted', pacing_cooldown_until='4000',
                             pacing_session_rest_until='5000')
        decision = self.decide()
        self.assertEqual(decision['not_before'], 5000)
        self.assertEqual(decision['action'], 'wait')
        self.assertEqual(self.decide(now=5000)['action'], 'check_browser')
        self.assertEqual(self.controls['workspace_blocked'], 'access_restricted', 'policy must never clear access controls')

    def test_session_rest_resumes_after_persisted_deadline(self):
        self.controls['pacing_session_rest_until'] = '200'
        first = self.decide()
        self.assertEqual(first['not_before'], 200)
        self.resume.update(status='waiting', not_before=200, retry_reason=first['reason'])
        self.assertEqual(self.decide(now=200)['action'], 'check_browser')

    def test_restart_is_opt_in(self):
        self.resume['auto_retry_transient'] = False
        self.assertEqual(self.decide()['reason'], 'worker_not_running')
        self.controls['workspace_blocked'] = 'access_restricted'
        self.resume['auto_retry_access'] = False
        self.assertEqual(self.decide()['reason'], 'access_restricted')

    def test_storage_and_explicit_user_pause_are_not_silent_retries(self):
        for reason in ['storage_error', 'archive_corrupt', 'user_pause']:
            self.status['tasks'] = [{'status': 'paused', 'body': 'saved', 'reason': reason,
                                     'updated': '1970-01-01T00:00:20+00:00'}]
            self.assertEqual(self.decide()['action'], 'attention', reason)
        self.status['tasks'] = [{'status': 'paused', 'body': 'saved',
                                 'reason': 'controlled_page_changed_or_redirected'}]
        self.assertEqual(self.decide()['action'], 'wait')
        self.status['tasks'] = []
        self.status['search'] = [{'status': 'paused', 'reason': 'search_page_changed'}]
        self.assertEqual(self.decide()['action'], 'wait')
        self.assertEqual(self.decide(free_gib=.5)['reason'], 'insufficient_disk_space')

    def test_new_explicit_resume_can_supersede_old_task_pause(self):
        self.status['tasks'] = [{'status': 'paused', 'reason': 'user_pause',
                                'updated': '1970-01-01T00:00:05+00:00'}]
        self.assertEqual(self.decide()['action'], 'wait')

    def test_unavailable_posts_are_not_retried_forever_or_called_complete(self):
        self.status['tasks'] = [{'status': 'finished', 'body': 'unavailable'}]
        self.status['search'] = [{'status': 'complete'}]
        self.assertEqual(self.decide()['reason'], 'no_retryable_work_but_goal_incomplete')

    def test_finished_partial_comments_and_media_are_retryable(self):
        self.status['tasks'] = [{'status': 'finished', 'body': 'saved', 'comments': 'partial',
                                'media': 'success', 'comment_media': 'partial'}]
        self.status['search'] = [{'status': 'complete'}]
        self.assertEqual(self.decide()['action'], 'wait')

    def test_unblocked_home_cannot_mask_verification_on_another_tab(self):
        self.assertEqual(browser_resume_reason([{'logged_in': True, 'blocked': False}]), '')
        self.assertEqual(browser_resume_reason([
            {'logged_in': True, 'blocked': False}, {'logged_in': True, 'blocked': True}
        ]), 'manual_login_or_verification_required')
        self.assertTrue(browser_resume_reason([]))
        self.assertTrue(browser_resume_reason([{'logged_in': None, 'blocked': False}]))

    def test_projection_complete_never_authorizes_goal_completion(self):
        self.status['outcome'] = 'complete'
        self.assertEqual(self.decide()['action'], 'audit')

    def test_stalled_worker_requires_explicit_opt_in_and_idle_deadline(self):
        args = dict(now=1000, alive=True, progress_at=1)
        self.assertIsNone(stalled_worker_decision(self.resume, {}, **args))
        self.resume.update(auto_restart_stalled=True, stall_timeout_seconds=600)
        self.assertEqual(stalled_worker_decision(self.resume, {}, **args)['action'], 'restart')
        self.assertIsNone(stalled_worker_decision(self.resume, {}, **(args | {'progress_at': 900})))
        self.assertIsNone(stalled_worker_decision(self.resume, {}, **(args | {'alive': False})))

    def test_stalled_watchdog_respects_planned_waits_and_user_cancellation(self):
        self.resume['auto_restart_stalled'] = True
        args = dict(now=1000, alive=True, progress_at=1)
        self.assertIsNone(stalled_worker_decision(self.resume, {'pacing_cooldown_until': '1100'}, **args))
        self.assertIsNone(stalled_worker_decision(self.resume, {}, next_retry_at=1100, **args))
        self.resume['status'] = 'cancelled_by_user'
        self.assertIsNone(stalled_worker_decision(self.resume, {}, **args))


if __name__ == '__main__':
    unittest.main()
