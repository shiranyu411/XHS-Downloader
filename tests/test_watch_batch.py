"""Offline event lines of scripts/watch_batch.py, on a fixture batch."""
from datetime import datetime
import importlib.util
import json
import os
from pathlib import Path
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from source.collection.config import Config
from source.collection.store import Store

SPEC = importlib.util.spec_from_file_location('watch_batch', Path(__file__).resolve().parents[1] / 'scripts' / 'watch_batch.py')
watch = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(watch)


class Done(Exception):
    """Ends watch.follow after a set number of polls."""


class WatchBatchTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.batch = Path(self.temp.name).resolve()
        self.store = Store(self.batch, Config())
        for keyword, id_ in (('A', 'a' * 24), ('A', 'b' * 24), ('B', 'c' * 24)):
            self.store.enqueue([f'https://www.xiaohongshu.com/explore/{id_}'], {'mode': 'keyword_search', 'request': {'keyword': keyword}})
        self.search(B='pending')
        self.report(state='collecting', message='采集中')
        for pidfile in watch.HELPERS:
            (self.batch / pidfile).write_text(str(os.getpid()))

    def tearDown(self):
        self.store.close()
        self.temp.cleanup()

    def search(self, **statuses):
        self.store.control('search_progress', json.dumps([{'keyword': 'A', 'status': 'complete', 'pages': [{}, {}]}] + [
            {'keyword': k, 'status': s, 'pages': [{}] * (s != 'pending')} for k, s in statuses.items()]))

    def report(self, **fields):
        (self.batch / 'supervisor.json').write_text(json.dumps(
            {'checked_at': f'{datetime.now():%Y-%m-%dT%H:%M:%S}', 'supervisor': {'launches': [{'at': time.time()}]}, **fields}))

    def finish(self, id_):
        self.store.finish(self.store.begin(id_), {'body': 'saved', 'comments': 'sampled', 'reason': 'fixture'}, 'finished')

    def follow(self, start, polls, every=3600):
        """Lines from `polls` polls of watch.follow, 20 s apart on a fake clock from `start`."""
        clock, lines, slept = [start], [], []

        def sleep(seconds):
            slept.append(seconds)
            if len(slept) == polls:
                raise Done
            clock[0] += seconds

        with patch.object(watch.shutil, 'disk_usage', return_value=SimpleNamespace(free=100 * 1024**3)), self.assertRaises(Done):
            watch.follow(self.batch, every, lambda: clock[0], sleep, lines.append)
        return lines

    def test_progress_line_counts_finished_posts_and_the_change_since_the_last_line(self):
        self.assertIn('已完成 0 篇，', watch.progress(self.batch))
        self.assertIsNone(watch.progress(self.batch))  # nothing moved, e.g. during a cooldown
        self.finish('a' * 24)
        line = watch.progress(self.batch)
        self.assertIn('以来 +1）', line)
        self.assertIn('A 1(+1) B 0', line)
        self.assertIn('搜索中：B 未开始', line)
        self.report(state='cooldown', message='冷却')
        self.assertIn('📊 被限制后冷却中｜已完成 1 篇（', watch.progress(self.batch))

    def test_events_follow_state_refusals_and_searches_and_stay_silent_otherwise(self):
        old = watch.poll(self.batch)
        self.assertEqual(list(watch.events(old, old, time.time())), [])
        self.store.control('pacing_restriction_log', json.dumps([time.time()]))
        self.search(B='running')
        self.report(state='cooldown', message='冷却', next_at='10-31 10:00')
        lines = list(watch.events(old, watch.poll(self.batch), time.time()))
        self.assertEqual(len(lines), 3)
        self.assertIn('采集中 → 被限制后冷却中｜冷却｜下一步 10-31 10:00', lines[0])
        self.assertIn('48 小时内第 1 次', lines[1])
        self.assertTrue(lines[2].endswith('B：开始搜索'))

    def test_alarms_for_a_silent_supervisor_an_idle_collector_or_low_disk(self):
        new = watch.poll(self.batch)
        with patch.object(watch.shutil, 'disk_usage', return_value=SimpleNamespace(free=100 * 1024**3)):
            self.assertEqual(watch.alarms(self.batch, new, time.time()), {})
            later = time.time() + watch.QUIET + 60
            self.assertEqual(set(watch.alarms(self.batch, new, later)), {'stale', 'quiet'})
            # A fresh launch, after a cooldown, restarts the quiet clock.
            self.assertEqual(watch.alarms(self.batch, {**new, 'launched': later - 60, 'checked': later}, later), {})
        with patch.object(watch.shutil, 'disk_usage', return_value=SimpleNamespace(free=10 * 1024**3)):
            self.assertEqual(set(watch.alarms(self.batch, new, time.time())), {'disk'})

    def test_progress_lines_keep_their_schedule_across_restarts(self):
        start = time.time()
        with patch.object(watch, 'alarms', return_value={}):
            self.assertEqual(len(self.follow(start, 3, 40)), 2)  # at once (no earlier line), then 40 s later
            self.assertEqual(self.follow(start + 50, 1, 40), [])  # a re-armed watcher waits for +80
            self.assertIn('📊 采集中｜已完成 0 篇（', self.follow(start + 85, 1, 40)[0])
            self.assertEqual(len(self.follow(start + 121, 1, 40)), 1)  # next at +120, not 40 s after +85
            self.assertEqual(len(self.follow(start + 300, 1, 40)), 1)  # away for whole periods: one line
            self.assertEqual(self.follow(start + 330, 1, 40), [])  # and the next 40 s after it
            (self.batch / '.watch-loop.json').unlink()
            self.assertEqual(self.follow(start + 20, 1, 40), [])  # without its file: 40 s after the last line

    def test_a_rearmed_watcher_reports_what_happened_meanwhile_and_repeats_no_alarm(self):
        start = time.time()
        self.assertEqual(len(self.follow(start, 1)), 1)  # the first progress line
        self.store.control('pacing_restriction_log', json.dumps([time.time()]))
        self.search(B='running')
        self.report(state='cooldown', message='冷却')
        (self.batch / 'exporter.pid').unlink()
        lines = self.follow(start + 20, 1)
        self.assertEqual(len(lines), 4, lines)
        self.assertIn('采集中 → 被限制后冷却中', lines[0])
        self.assertIn('48 小时内第 1 次', lines[1])
        self.assertTrue(lines[2].endswith('B：开始搜索'))
        self.assertTrue(lines[3].endswith('增量导出程序没在运行'))
        self.assertEqual(self.follow(start + 40, 1), [])
        (self.batch / 'exporter.pid').write_text(str(os.getpid()))
        lines = self.follow(start + 60, 1)
        self.assertEqual(len(lines), 1, lines)
        self.assertTrue(lines[0].endswith('增量导出程序又在运行了'))

    def test_unreadable_state_is_reported_once_until_it_reads_again(self):
        start = time.time()
        (self.batch / 'supervisor.json').write_text('{')
        lines = [line for line in self.follow(start, 10) if '📊' not in line]
        self.assertEqual(len(lines), 1, lines)
        self.assertIn('3 分钟读不到批次状态', lines[0])
        self.assertEqual(self.follow(start + 200, 10), [])  # still unreadable after a restart: no repeat
        self.report(state='collecting', message='采集中')
        lines = self.follow(time.time(), 1)  # fresh, so the supervisor does not look silent
        self.assertEqual(len(lines), 1, lines)
        self.assertTrue(lines[0].endswith('又能读到批次状态了'))


if __name__ == '__main__':
    unittest.main()
