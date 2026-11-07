"""Exact platform totals must not be hidden by a successful pagination chain."""
import csv
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from source.collection.config import Config
from source.collection.media import asset, register
from source.collection.results import collection_status, comment_count_evidence, export_complete, read_post
from source.collection.store import Store, atomic_write, encode
from source.collection.supervision import retry_decision

NOTE = '0123456789abcdef01234567'
URL = f'https://www.xiaohongshu.com/explore/{NOTE}?xsec_token=fixture'


class CommentCountTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.store = Store(Path(self.temp.name), Config())
        self.store.enqueue([URL], {})
        self.attempt = self.store.begin(NOTE)
        self.note = {'noteId': NOTE, 'title': 'fixture', 'desc': 'body',
                     'interactInfo': {'commentCount': '2'},
                     'imageList': [{'fileId': 'fixture-image', 'urlDefault': 'https://sns-img-bd.xhscdn.com/fixture'}]}
        self.set_total('2')
        comment = {'id': 'root', 'note_id': NOTE, 'content': 'primary', 'user_info': {'user_id': 'u1'},
                   'sub_comment_count': 1, 'sub_comment_has_more': False, 'sub_comment_cursor': '',
                   'sub_comments': [{'id': 'reply', 'root_comment_id': 'root', 'note_id': NOTE,
                                     'content': 'reply', 'user_info': {'user_id': 'u2'}}]}
        self.store.save(self.attempt, NOTE, 'network_json', encode({
            'success': True, 'code': 0, 'data': {'comments': [comment], 'has_more': False, 'cursor': ''}}),
            {'kind': 'comments', 'note': NOTE, 'cursor': '', 'status': 200,
             'path': '/api/sns/web/v2/comment/page', 'scope': []})
        item = asset(self.note['imageList'][0], 'image')
        register(self.store, NOTE, '', [item])
        data = b'local fixture media'
        path = 'media/files/fixture.jpeg'
        atomic_write(self.store.root / path, data)
        with self.store.db:
            self.store.db.execute("UPDATE media_files SET status='saved',path=?,sha256=?,size=?",
                                  (path, hashlib.sha256(data).hexdigest(), len(data)))
        self.store.finish(self.attempt, {'body': 'saved', 'comments': 'complete_observed',
                                        'media': 'success', 'comment_media': 'success', 'reason': 'observed_end'})

    def tearDown(self):
        self.store.close()
        self.temp.cleanup()

    def set_total(self, total):
        self.note['interactInfo']['commentCount'] = total
        self.store.save(self.attempt, NOTE, 'note_object', encode(self.note))

    def test_matching_exact_total_remains_complete(self):
        post = read_post(self.store.root, NOTE)
        self.assertEqual(post['outcome'], 'complete')
        self.assertEqual(post['comment_count_evidence']['status'], 'matched')
        self.assertEqual(post['comment_count_evidence']['difference'], 0)
        self.assertEqual(post['warnings'], [])

    def test_ended_chains_and_saved_media_cannot_hide_an_exact_shortfall(self):
        self.set_total('240')
        before = self.store.tasks()
        post = read_post(self.store.root, NOTE)
        self.assertTrue(post['active_complete_verified'], 'keep the accurate pagination fact')
        self.assertEqual(post['status']['comments'], 'complete_observed')
        self.assertEqual(post['outcome'], 'partial')
        self.assertIn('advertised_comment_total_mismatch', post['warnings'])
        self.assertEqual(post['comment_count_evidence']['difference'], 238)
        state = collection_status(self.store.root)
        self.assertEqual(state['counts']['complete'], 0)
        self.assertEqual(state['counts']['partial'], 1)
        self.assertEqual(state['tasks'][0]['missing'], ['comment_count_reconciliation'])
        self.assertEqual(self.store.tasks(), before, 'read-only quality checks must not rewrite pagination')
        # Repeating the same terminal pagination without new evidence does not fix
        # a stale/hidden/deleted counter. Surface the gap, do not loop requests.
        decision = retry_decision(state, {}, {'auto_retry_access': True, 'auto_retry_transient': True},
                                  now=100, alive=False, active=False, free_gib=100)
        self.assertEqual(decision['reason'], 'no_retryable_work_but_goal_incomplete')

    def test_export_preserves_the_discrepancy_and_does_not_claim_complete(self):
        self.set_total('240')
        folder = Path(export_complete(self.store.root)['folder'])
        summary = json.loads((folder / 'summary.json').read_text())
        result = summary['results'][0]
        self.assertEqual(result['outcome'], 'partial')
        self.assertEqual(result['comment_count_evidence']['advertised_exact'], 240)
        with (folder / 'posts.csv').open(encoding='utf-8-sig', newline='') as stream:
            row = next(csv.DictReader(stream))
        self.assertEqual((row['comment_count'], row['observed_comment_count'], row['comment_count_status'],
                          row['comment_count_difference'], row['outcome']), ('240', '2', 'mismatch', '238', 'partial'))
        with (folder / 'comments.csv').open(encoding='utf-8-sig', newline='') as stream:
            self.assertEqual(len(list(csv.DictReader(stream))), 2)

    def test_counters_are_not_converted_from_approximations_or_assumed_during_progress(self):
        for raw in ['', '1.2万', '100+', 'unknown']:
            with self.subTest(raw=raw):
                result = comment_count_evidence({'comment_count': raw}, [1, 2], True)
                self.assertEqual(result['status'], 'not_exact')
                self.assertIsNone(result['advertised_exact'])
                self.assertIsNone(result['difference'])
        result = comment_count_evidence({'comment_count': '1,200'}, [1, 2], True)
        self.assertEqual(result['advertised_exact'], 1200)
        result = comment_count_evidence({'comment_count': '240'}, [1, 2], False)
        self.assertEqual(result['status'], 'in_progress')
        self.set_total('1')
        self.assertEqual(read_post(self.store.root, NOTE)['comment_count_evidence']['difference'], -1)
        self.assertEqual(read_post(self.store.root, NOTE)['outcome'], 'partial')


if __name__ == '__main__':
    unittest.main()
