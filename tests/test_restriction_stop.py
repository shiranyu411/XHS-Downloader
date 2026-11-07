"""Local browser checks: any refusal stops the whole batch; the next session resumes it."""
import asyncio
import json
from pathlib import Path
import unittest
from urllib.parse import parse_qs, urlsplit

from source.collection.config import Config
from source.collection.runner import Runner, Paused
from source.collection.store import Store
from tests import test_collection as fixtures
from tests.test_collection import UNPACED

BAD = 'a' * 24
GOOD = 'b' * 24


class RestrictionStopTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        await fixtures.BrowserTests.asyncSetUp(self)
        self.documents = []

    asyncTearDown = fixtures.BrowserTests.asyncTearDown

    async def route(self, route):
        url = urlsplit(route.request.url)
        if url.hostname == 'www.xiaohongshu.com':
            note = url.path.rsplit('/', 1)[-1]
            self.documents.append(url.path)
            html = '''<script>
                window.__INITIAL_STATE__={note:{noteDetailMap:{'NOTE':{note:{noteId:'NOTE',title:'fixture'}}}}};
                const client={get:u=>fetch(u).then(r=>r.json()),makeUri(){},instance:{},
                  interceptors:{request:{},response:{},dispatch:{},result:{}}};
                window.webpackChunkxhs_pc_web={push:c=>c[2]({c:{fixture:{exports:client}}})};
            </script>'''.replace('NOTE', note)
            if self.mode == 'automatic':
                # The real note page loads its first comment page by itself.
                html += '<script>fetch("https://edith.xiaohongshu.com/api/sns/web/v2/comment/page?note_id=' + note + '&cursor=")</script>'
            await route.fulfill(content_type='text/html', body=html)
        elif url.hostname == 'edith.xiaohongshu.com':
            query = parse_qs(url.query, keep_blank_values=True)
            self.calls.append((url.path, query))
            note = query['note_id'][0]
            if self.mode == 'automatic' or '/sub/' in url.path and self.mode != 'recovered':
                await route.fulfill(status=461, content_type='application/json',
                                    body=json.dumps({'success': False, 'code': 300013}),
                                    headers={'Access-Control-Allow-Origin': '*'})
                return
            if '/sub/' in url.path:
                comments = [{'id': 'reply', 'note_id': note, 'root_comment_id': 'root'}]
            elif note == BAD:
                comments = [{'id': 'root', 'note_id': note,
                             'sub_comments': [{'id': 'embedded', 'note_id': note, 'root_comment_id': 'root'}],
                             'sub_comment_count': 2, 'sub_comment_has_more': True, 'sub_comment_cursor': 'embedded'}]
            else:
                comments = []
            await route.fulfill(content_type='application/json',
                                body=json.dumps({'success': True, 'code': 0,
                                    'data': {'comments': comments, 'has_more': False, 'cursor': ''}}),
                                headers={'Access-Control-Allow-Origin': '*'})
        else:
            await route.abort()

    def store(self, **options):
        return Store(Path(self.temp.name) / 'batch', Config(**{'post_concurrency': 2,
            'open_interval': 0, 'page_interval': 0, 'media': False, **UNPACED, **options}))

    async def test_refused_reply_stops_peer_and_next_session_resumes_same_chain(self):
        store = self.store()
        store.enqueue([f'https://www.xiaohongshu.com/explore/{id_}' for id_ in (BAD, GOOD)], {})
        runner = Runner(store)
        try:
            results = await asyncio.gather(*(runner.collect(self.context, task) for task in store.tasks()),
                                           return_exceptions=True)
            self.assertIsInstance(results[0], Paused)
            self.assertEqual(str(results[0]), 'access_restricted')
            self.assertTrue(runner.pause_requested)
            self.assertEqual(str(runner.worker_failure), 'access_restricted')
            with self.assertRaisesRegex(Paused, 'access_restricted'):
                runner.checkpoint()
            bad = store.tasks()[0]
            self.assertEqual((bad['status'], bad['body'], bad['comments']), ('paused', 'saved', 'blocked'))
            self.assertTrue(store.note_object(BAD))
            self.assertIsNone(store.next_retry_time(), 'no per-post retry around a refusal')
            # After the batch cooldown a new session resumes the refused reply chain.
            self.mode = 'recovered'
            runner = Runner(store)
            while (task := store.next_task()) is not None:
                result = await runner.collect(self.context, task)
                self.assertEqual(result['comments'], 'complete_observed')
            self.assertEqual([t['comments'] for t in store.tasks()], ['complete_observed'] * 2)
            last = store.db.execute('SELECT result FROM attempts WHERE note_id=? ORDER BY rowid DESC LIMIT 1', (BAD,)).fetchone()
            self.assertEqual(json.loads(last[0])['resume_chain'], 2)
            replies = [q['cursor'][0] for path, q in self.calls if '/sub/' in path]
            self.assertEqual(replies, ['embedded', 'embedded'], 'the refused page is requested once more, nothing else')
        finally:
            store.close()

    async def test_refused_page_request_stops_before_collector_sends_any(self):
        self.mode = 'automatic'
        store = self.store(post_concurrency=1)
        store.enqueue([f'https://www.xiaohongshu.com/explore/{id_}' for id_ in (GOOD, BAD)], {})
        runner = Runner(store)
        try:
            with self.assertRaisesRegex(Paused, 'access_restricted'):
                await runner.collect_next(self.context)
            self.assertEqual(len(self.calls), 1, "only the page's own request; the collector sent none")
            self.assertEqual(self.documents, ['/explore/' + GOOD], 'the next post is not opened')
            task = store.tasks()[0]
            self.assertEqual((task['status'], task['comments']), ('paused', 'blocked'))
            self.assertIsNone(store.next_retry_time())
            self.assertEqual(store.next_task()['note_id'], GOOD, 'the next session starts with the same post')
        finally:
            store.close()


if __name__ == '__main__':
    unittest.main()
