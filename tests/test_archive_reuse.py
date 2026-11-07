"""Synthetic cross-archive reuse tests. Never connects to a real browser or CDN."""
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest

from source.collection.config import Config
from source.collection.replay import chain_key, resume_seed
from source.collection.results import read_post
from source.collection.reuse import reuse_complete_comments
from source.collection.runner import Runner
from source.collection.store import Store, encode

NOTE = '0123456789abcdef01234567'
URL = f'https://www.xiaohongshu.com/explore/{NOTE}?xsec_token=new-fixture'


class ArchiveReuseTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.source = Store(self.root / 'old', Config(media=False))
        self.source.enqueue([URL.replace('new-fixture', 'old-fixture')], {})
        first = self.source.begin(NOTE)
        note = {'noteId': NOTE, 'title': 'fixture', 'desc': 'saved body', 'imageList': []}
        self.source.save(first, NOTE, 'note_object', encode(note))
        primary = {'id': 'c1', 'note_id': NOTE, 'content': 'primary', 'user_info': {'user_id': 'u1'},
                   'sub_comment_has_more': False, 'sub_comment_cursor': '', 'sub_comment_count': 1,
                   'sub_comments': [{'id': 'r1', 'note_id': NOTE, 'root_comment_id': 'c1',
                                     'content': 'reply', 'user_info': {'user_id': 'u2'}}]}
        self.page(first, '', [primary], True, 'next')
        self.source.finish(first, {'body': 'saved', 'comments': 'partial', 'media': 'disabled', 'reason': 'interrupted'}, 'interrupted')
        second = self.source.begin(NOTE)
        self.source.save(second, NOTE, 'note_object', encode(note))
        self.page(second, 'next', [], False, '')
        self.source.control(chain_key(NOTE), encode([first, second]).decode())
        self.source.finish(second, {'body': 'saved', 'comments': 'complete_observed', 'media': 'disabled', 'reason': 'observed_end'})
        self.dest = Store(self.root / 'new', Config())
        self.dest.enqueue([URL], {'keyword': 'fixture'})

    def tearDown(self):
        self.dest.close()
        self.source.close()
        self.temp.cleanup()

    def page(self, attempt, cursor, comments, more, next_cursor):
        self.source.save(attempt, NOTE, 'network_json', encode({
            'success': True, 'code': 0,
            'data': {'comments': comments, 'has_more': more, 'cursor': next_cursor}}),
            {'kind': 'comments', 'note': NOTE, 'cursor': cursor, 'status': 200,
             'path': '/api/sns/web/v2/comment/page', 'scope': []})

    async def test_two_attempt_chain_is_reused_without_a_browser_or_fake_media_success(self):
        source_events = self.source.db.execute('SELECT count(*) FROM events').fetchone()[0]
        result = reuse_complete_comments(self.dest, [self.source.root])
        self.assertEqual(result['imported'], 1)
        task = self.dest.next_task()
        self.assertEqual(task['url'], URL, 'retain the fresh discovery access URL')
        self.assertEqual((task['status'], task['body'], task['comments'], task['media']),
                         ('pending', 'saved', 'complete_observed', 'disabled'))
        ledger, chain, _ = resume_seed(self.dest, NOTE)
        self.assertTrue(ledger.complete())
        self.assertEqual(len(chain), 2)
        post = read_post(self.dest.root, NOTE)
        self.assertTrue(post['active_complete_verified'])
        self.assertEqual({c['comment_id'] for c in post['comments']}, {'c1', 'r1'})
        self.assertEqual(post['outcome'], 'partial', 'missing media must not be claimed complete')
        imported_seen = [r[0] for r in self.dest.db.execute("SELECT seen FROM events WHERE kind='network_json' ORDER BY id")]
        original_seen = [r[0] for r in self.source.db.execute("SELECT seen FROM events WHERE kind='network_json' ORDER BY id")]
        self.assertEqual(imported_seen, original_seen, 'retain historical observation times')
        self.assertEqual(reuse_complete_comments(self.dest, [self.source.root])['imported'], 0)
        self.assertEqual(self.source.db.execute('SELECT count(*) FROM events').fetchone()[0], source_events)
        # context=None would fail immediately if the runner tried to navigate.
        finished = await Runner(self.dest).collect(None, task)
        self.assertEqual(finished['comments'], 'complete_observed')
        self.assertEqual(finished['reason'], 'observed_end')
        self.assertEqual(finished['media'], 'partial')
        self.assertEqual(finished['comment_media'], 'success')

    async def test_failed_transaction_does_not_attach_half_an_import(self):
        self.dest.db.execute("""CREATE TRIGGER simulate_failure BEFORE INSERT ON events
                             WHEN NEW.kind='archive_reuse' BEGIN SELECT RAISE(ABORT,'fixture failure'); END""")
        self.dest.db.commit()
        with self.assertRaises(sqlite3.IntegrityError):
            reuse_complete_comments(self.dest, [self.source.root])
        self.assertEqual(self.dest.db.execute('SELECT count(*) FROM attempts').fetchone()[0], 0)
        self.assertEqual(self.dest.db.execute('SELECT count(*) FROM events').fetchone()[0], 0)
        self.assertIsNone(self.dest.control(chain_key(NOTE)))
        self.assertEqual(self.dest.next_task()['body'], 'partial')
        self.dest.db.execute('DROP TRIGGER simulate_failure')
        self.dest.db.commit()
        self.assertEqual(reuse_complete_comments(self.dest, [self.source.root])['imported'], 1)
        self.assertEqual(self.dest.verify()['invalid_events'], [])

    async def test_terminal_cursors_do_not_authorize_reusing_a_known_count_gap(self):
        chain = json.loads(self.source.control(chain_key(NOTE)))
        self.source.save(chain[-1], NOTE, 'note_object', encode({
            'noteId': NOTE, 'title': 'fixture', 'desc': 'body', 'imageList': [],
            'interactInfo': {'commentCount': '3'}}))
        old = read_post(self.source.root, NOTE)
        self.assertTrue(old['active_complete_verified'])
        self.assertEqual(old['comment_count_evidence']['status'], 'mismatch')
        before = self.dest.tasks()
        result = reuse_complete_comments(self.dest, [self.source.root])
        self.assertEqual((result['imported'], result['count_mismatch_skipped']), (0, 1))
        self.assertEqual(result['sources'][0]['count_mismatch_queue_positions'], [1])
        self.assertEqual(self.dest.tasks(), before, 'keep the task pending for fresh collection')
        self.assertIsNone(self.dest.control(chain_key(NOTE)))
        self.assertEqual(self.dest.db.execute('SELECT count(*) FROM attempts').fetchone()[0], 0)

    async def test_matching_comment_total_is_reusable_even_when_media_is_unfinished(self):
        chain = json.loads(self.source.control(chain_key(NOTE)))
        self.source.save(chain[-1], NOTE, 'note_object', encode({
            'noteId': NOTE, 'title': 'fixture', 'desc': 'body', 'imageList': [],
            'interactInfo': {'commentCount': '2'}}))
        result = reuse_complete_comments(self.dest, [self.source.root])
        self.assertEqual((result['imported'], result['count_mismatch_skipped']), (1, 0))
        self.assertEqual(read_post(self.dest.root, NOTE)['comment_count_evidence']['status'], 'matched')
        self.assertEqual(self.dest.next_task()['media'], 'disabled')
        provenance = json.loads(self.dest.control('archive_reuse:' + NOTE))
        self.assertEqual(provenance['comment_count_evidence']['status'], 'matched')

    async def test_unverified_source_flags_do_not_count_as_complete(self):
        row = self.source.db.execute("SELECT path FROM events WHERE kind='network_json' ORDER BY id DESC LIMIT 1").fetchone()
        (self.source.root / row['path']).write_bytes(b'corrupt fixture')
        result = reuse_complete_comments(self.dest, [self.source.root])
        self.assertEqual(result['imported'], 0)
        self.assertEqual(result['unverified_skipped'], 1)
        self.assertEqual(self.dest.next_task()['body'], 'partial')

    async def test_existing_destination_work_is_not_overwritten(self):
        attempt = self.dest.begin(NOTE)
        self.dest.finish(attempt, {'body': 'partial', 'comments': 'partial', 'media': 'disabled', 'reason': 'fixture'})
        self.dest.retry_incomplete()
        before = self.dest.tasks()
        result = reuse_complete_comments(self.dest, [self.source.root])
        self.assertEqual(result['imported'], 0)
        self.assertEqual(result['existing_attempts_skipped'], 1)
        self.assertEqual(self.dest.tasks(), before)

    async def test_only_existing_tasks_and_sibling_archives_are_allowed(self):
        self.dest.db.execute('DELETE FROM tasks')
        self.dest.db.commit()
        self.assertEqual(reuse_complete_comments(self.dest, [self.source.root])['imported'], 0)
        self.assertEqual(self.dest.tasks(), [])
        with self.assertRaises(ValueError):
            reuse_complete_comments(self.dest, [self.dest.root])
        with self.assertRaises(ValueError):
            reuse_complete_comments(self.dest, [self.root.parent])


if __name__ == '__main__':
    unittest.main()
