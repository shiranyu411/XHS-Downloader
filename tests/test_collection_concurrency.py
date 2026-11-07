"""Offline worker scheduling regressions; no platform traffic."""
import asyncio
from pathlib import Path
import tempfile
import unittest

from source.collection.config import Config
from source.collection.pacing import Pacer
from source.collection.runner import Paused, Runner
from source.collection.store import Store


class ConcurrentCollectionTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.store = Store(Path(self.temp.name), Config(post_concurrency=5))
        self.ids = [f'{i:024x}' for i in range(1, 9)]
        self.store.enqueue([f'https://www.xiaohongshu.com/explore/{id_}' for id_ in self.ids], {})
        self.runner = Runner(self.store)
        self.release = {id_: asyncio.Event() for id_ in self.ids}
        self.started = []

        async def collect(context, task, strategy=None):
            id_ = task['note_id']
            attempt = self.store.begin(id_)
            self.started.append(id_)
            await self.release[id_].wait()
            result = {'body': 'saved', 'comments': 'complete_observed', 'reason': 'observed_end'}
            self.store.finish(attempt, result, 'finished')
            return result

        self.runner.collect = collect

    async def asyncTearDown(self):
        for event in self.release.values():
            event.set()
        await self.runner.drain_workers()
        self.store.close()
        self.temp.cleanup()

    async def test_search_can_continue_with_five_distinct_posts_in_flight(self):
        await asyncio.wait_for(self.runner.collect_turn(None, wait=False), timeout=1)
        self.assertEqual(len(self.started), 5)
        self.assertEqual(len(set(self.started)), 5)
        self.assertEqual(sum(t['status'] == 'running' for t in self.store.tasks()), 5)
        # Repeated search pages must neither block nor grow the pool.
        for _ in range(3):
            await asyncio.wait_for(self.runner.collect_turn(None, wait=False), timeout=1)
        self.assertEqual(len(self.started), 5)
        self.release[self.started[0]].set()
        await asyncio.wait_for(self.runner.post_progress.wait(), timeout=1)
        await self.runner.collect_turn(None, wait=False)
        self.assertEqual(len(self.started), 6)
        self.assertEqual(len(self.runner.post_workers), 5)
        self.assertEqual(len(set(self.started)), 6)

    async def test_after_search_waits_for_pool_before_reporting_finished(self):
        await self.runner.collect_turn(None, wait=False)
        turn = asyncio.create_task(self.runner.collect_turn(None))
        await asyncio.sleep(0)
        self.assertFalse(turn.done())
        self.release[self.started[0]].set()
        self.assertTrue(await asyncio.wait_for(turn, timeout=1))
        for event in self.release.values():
            event.set()
        while await self.runner.collect_turn(None):
            pass
        self.assertEqual(len(self.started), len(self.ids))
        self.assertTrue(all(t['status'] == 'finished' for t in self.store.tasks()))

    async def test_peer_restriction_stops_search_and_replenishment(self):
        await self.runner.collect_turn(None, wait=False)
        self.runner.worker_failure = Paused('access_restricted')
        with self.assertRaisesRegex(Paused, 'access_restricted'):
            await self.runner.collect_turn(None, wait=False)
        self.assertEqual(len(self.started), 5)

    async def test_shared_pacing_reservations_stay_spaced_under_contention(self):
        now = [1_000_000.0]

        async def sleep(seconds):
            now[0] += seconds
            await asyncio.sleep(0)

        self.runner.pacer = Pacer(self.store, clock=lambda: now[0], sleep=sleep)
        await asyncio.gather(*(self.runner.paced(None) for _ in range(5)))
        times = list(self.runner.pacer.window)
        self.assertEqual(self.runner.pacer.session_count, 5)
        self.assertTrue(all(b - a >= 15 for a, b in zip(times, times[1:])))


if __name__ == '__main__':
    unittest.main()
