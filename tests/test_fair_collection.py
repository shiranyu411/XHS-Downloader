"""Offline fair scheduling and cross-slice pagination tests; no platform traffic."""
import json
from pathlib import Path
import tempfile
import unittest

from source.collection.config import Config
from source.collection.replay import resume_seed
from source.collection.runner import Runner, Paused
from source.collection.store import Store
from tests import test_collection as fixtures
from tests.test_collection import NOTE, URL, UNPACED


class FairQueueTests(unittest.TestCase):
    def test_quantum_validation_and_legacy_default(self):
        self.assertIsNone(Config().comment_page_quantum)
        self.assertEqual(Config(comment_page_quantum=2).comment_page_quantum, 2)
        for bad in (0, -1, True, 1.5, '2'):
            with self.assertRaises(ValueError):
                Config(comment_page_quantum=bad)

    def test_fresh_and_old_lanes_do_not_starve_and_survive_restart(self):
        with tempfile.TemporaryDirectory() as root:
            store = Store(Path(root), Config(comment_page_quantum=2))
            ids = [f'{i:024x}' for i in range(1, 8)]
            store.enqueue([f'https://www.xiaohongshu.com/explore/{id_}' for id_ in ids], {})
            def finish(id_, status='pending'):
                attempt = store.begin(id_)
                store.finish(attempt, {'body': 'saved', 'comments': 'partial',
                                      'reason': 'comment_slice_yield'}, status)
            try:
                finish(ids[0])
                store.control('scheduler_fresh_streak', 0)
                for id_ in ids[1:3]:
                    self.assertEqual(store.next_task()['note_id'], id_)
                    finish(id_)
                store.close()
                store = Store(Path(root))
                self.assertEqual(store.next_task()['note_id'], ids[3])
                finish(ids[3])
                self.assertEqual(store.next_task()['note_id'], ids[0])
                finish(ids[0], 'finished')
                for id_ in ids[4:7]:
                    self.assertEqual(store.next_task()['note_id'], id_)
                    finish(id_)
                # With fresh work exhausted, use oldest yielded task, not row 1.
                self.assertEqual(store.next_task()['note_id'], ids[1])
                finish(ids[1])
                self.assertEqual(store.next_task()['note_id'], ids[2])
            finally:
                store.close()

    def test_legacy_fifo_is_unchanged(self):
        with tempfile.TemporaryDirectory() as root:
            store = Store(Path(root), Config())
            try:
                store.enqueue([URL, 'https://www.xiaohongshu.com/explore/' + 'a' * 24], {})
                attempt = store.begin(NOTE)
                store.finish(attempt, {'body': 'saved', 'comments': 'partial', 'reason': 'fixture'}, 'paused')
                self.assertEqual(store.next_task()['note_id'], NOTE)
            finally:
                store.close()


class SliceBrowserTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = fixtures.BrowserTests.asyncSetUp
    asyncTearDown = fixtures.BrowserTests.asyncTearDown
    route = fixtures.BrowserTests.route

    async def test_three_slices_preserve_body_all_replies_and_exact_cursor(self):
        path = Path(self.temp.name) / 'sliced'
        store = Store(path, Config(open_interval=0, page_interval=0, media=False,
                                  comment_page_quantum=2, max_operations=None, **UNPACED))
        store.enqueue([URL], {})
        try:
            for index in range(3):
                result = await Runner(store).collect(self.context, store.next_task())
                task = store.tasks()[0]
                self.assertEqual(task['body'], 'saved')
                if index < 2:
                    self.assertEqual((task['status'], result['reason'], task['comments']),
                                     ('pending', 'comment_slice_yield', 'partial'))
                    ledger, chain, stats = resume_seed(store, NOTE)
                    self.assertEqual(len(chain), index + 1)
                    self.assertEqual(stats['unreadable'], 0)
                    self.assertFalse(ledger.complete())
                    store.close()
                    store = Store(path)
                else:
                    self.assertEqual((task['status'], task['comments']), ('finished', 'complete_observed'))
                    self.assertIsNone(store.next_task())
            self.assertEqual([q['cursor'] for _, q in self.calls], ['', 'r2', 'r22', 'r42', 'r62', 'primary2'])
            ledger, chain, _ = resume_seed(store, NOTE)
            self.assertTrue(ledger.complete())
            self.assertEqual((len(ledger.primary.ids), len(ledger.replies['c0'].ids)), (21, 65))
            self.assertEqual(len(chain), 3)
        finally:
            store.close()

    async def test_slice_does_not_hide_access_restriction(self):
        store = Store(Path(self.temp.name) / 'restricted', Config(open_interval=0, page_interval=0,
                      media=False, comment_page_quantum=2, max_operations=None, **UNPACED))
        store.enqueue([URL], {})
        try:
            await Runner(store).collect(self.context, store.next_task())
            self.mode = 'throttle_once'
            with self.assertRaisesRegex(Paused, 'access_restricted'):
                await Runner(store).collect(self.context, store.next_task())
            task = store.tasks()[0]
            self.assertEqual((task['status'], task['comments'], task['reason']),
                             ('paused', 'blocked', 'access_restricted'))
            self.assertEqual(len(self.calls), 3)
            self.assertIsNone(store.next_retry_time(), 'a refusal is never retried per post')
        finally:
            store.close()
