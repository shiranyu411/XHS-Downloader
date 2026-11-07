"""Native reply page size and non-disruptive legacy compatibility; fixtures only."""
import asyncio
import json
from pathlib import Path
import unittest
from urllib.parse import parse_qs, urlsplit

from source.collection.capture import Capture
from source.collection.config import Config
from source.collection.protocol import Ledger, Request
from source.collection.replay import load_blob
from source.collection.request import request_url
from source.collection.runner import REQUEST_ADAPTER
from source.collection.store import Store
from tests import test_collection as fixtures
from tests.test_collection import NOTE, URL, comment, envelope

COMPAT = REQUEST_ADAPTER.with_name('collection-request-compat.js')
LEGACY = f'https://edith.xiaohongshu.com/api/sns/web/v2/comment/sub/page?note_id={NOTE}&root_comment_id=root&cursor=r0&count=20&image_formats=webp'


class ReplySizeBuilderTests(unittest.TestCase):
    def test_reply_fallback_uses_native_num_without_a_total_limit(self):
        result = request_url(Request('replies', NOTE, 'root', 'r0'), {}, URL)
        query = parse_qs(urlsplit(result).query)
        self.assertEqual(query['num'], ['10'])
        self.assertNotIn('count', query)
        self.assertEqual(query['root_comment_id'], ['root'])
        self.assertEqual(query['cursor'], ['r0'])

    def test_observed_reply_size_is_never_overridden(self):
        template = LEGACY + '&num=13'
        result = request_url(Request('replies', NOTE, 'other', 'next'), {'replies': template}, URL)
        query = parse_qs(urlsplit(result).query)
        self.assertEqual(query['num'], ['13'])
        self.assertEqual(query['count'], ['20'])
        self.assertEqual(query['root_comment_id'], ['other'])


class ReplySizeBrowserTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = fixtures.BrowserTests.asyncSetUp
    asyncTearDown = fixtures.BrowserTests.asyncTearDown

    async def route(self, route):
        url = urlsplit(route.request.url)
        if url.hostname == 'www.xiaohongshu.com':
            get = 'url=>{window.fixtureUrls.push(url);return new Promise(resolve=>{window.fixtureResolve=resolve})}' if self.mode == 'held' else 'url=>fetch(url).then(r=>r.json())'
            html = '''<script>
              window.fixtureUrls=[];
              const client={get:GET,makeUri(){},instance:{},interceptors:{request:{},response:{},dispatch:{},result:{}}};
              window.webpackChunkxhs_pc_web={push:c=>c[2]({c:{fixture:{exports:client}}})};
            </script>'''.replace('GET', get)
            await route.fulfill(content_type='text/html', body=html)
        elif url.hostname == 'edith.xiaohongshu.com':
            query = parse_qs(url.query)
            self.calls.append(query)
            # Model the observed native endpoint: num=10 returns a capped five;
            # legacy count=20 is ignored and returns only three.
            n = 5 if query.get('num') == ['10'] else 3
            payload = envelope([comment(f'r{i}', 'root') for i in range(1, n + 1)], n != 5, f'r{n}')
            await route.fulfill(content_type='application/json', body=json.dumps(payload), headers={'Access-Control-Allow-Origin': '*'})
        else:
            await route.abort()

    async def test_legacy_operation_still_matches_durable_cdp_response_and_valid_chain(self):
        store = Store(Path(self.temp.name) / 'batch', Config())
        store.enqueue([URL], {})
        attempt = store.begin(NOTE)
        ledger = Ledger(NOTE)
        root = comment('root')
        root.update(sub_comments=[comment('r0', 'root')], sub_comment_count=6,
                    sub_comment_has_more=True, sub_comment_cursor='r0')
        ledger.observe(Request('comments', NOTE), envelope([root]))
        capture = Capture(store, attempt, NOTE, ledger)
        page = await self.context.new_page()
        try:
            await capture.start(page)
            await page.add_init_script(path=str(REQUEST_ADAPTER))
            await page.goto(URL)
            installed = await page.evaluate(COMPAT.read_text())
            self.assertTrue(installed['installed'])
            op = capture.begin_operation(LEGACY)
            started = await page.evaluate('a=>window.__xhsReadOnly.start(a.id,a.url)', {'id': op['id'], 'url': LEGACY})
            self.assertTrue(started['started'])
            event_id = await asyncio.wait_for(op['done'], timeout=5)
            await capture.drain()
            capture.end_operation()
            row = store.db.execute('SELECT * FROM events WHERE id=?', (event_id,)).fetchone()
            meta = json.loads(row['metadata'])
            query = parse_qs(urlsplit(meta['url']).query)
            self.assertEqual(query['num'], ['10'])
            self.assertEqual(query['count'], ['20'], 'keep the original expected parameter for old in-flight collectors')
            self.assertEqual(meta['operation_id'], op['id'])
            self.assertEqual(len(json.loads(load_blob(store.root, row))['data']['comments']), 5)
            self.assertTrue(ledger.complete())
            self.assertEqual(ledger.gaps, [])
            self.assertEqual(len(ledger.replies['root'].ids), 6)
        finally:
            await capture.stop()
            await page.close()
            store.close()

    async def test_compat_preserves_pending_operation_guards_idempotence_and_rollback(self):
        self.mode = 'held'
        page = await self.context.new_page()
        try:
            await page.add_init_script(path=str(REQUEST_ADAPTER))
            await page.goto(URL)
            first = await page.evaluate('url=>window.__xhsReadOnly.start("first",url)', LEGACY + '&num=13')
            self.assertTrue(first['started'])
            await page.evaluate('window.fixtureOriginalStart=window.__xhsReadOnly.start')
            await page.evaluate(COMPAT.read_text())
            self.assertEqual(await page.evaluate('window.__xhsReadOnly.status("first").state'), 'pending')
            second = await page.evaluate('url=>window.__xhsReadOnly.start("blocked",url)', LEGACY)
            self.assertEqual(second, {'started': False, 'reason': 'operation_pending'})
            self.assertTrue((await page.evaluate(COMPAT.read_text()))['already_installed'])
            await page.evaluate('window.fixtureResolve({})')
            await page.wait_for_function('window.__xhsReadOnly.status("first").state === "settled"')
            await page.evaluate('url=>window.__xhsReadOnly.start("second",url)', LEGACY)
            urls = await page.evaluate('window.fixtureUrls')
            self.assertEqual(parse_qs(urlsplit(urls[0]).query)['num'], ['13'])
            self.assertEqual(parse_qs(urlsplit(urls[1]).query)['num'], ['10'])
            await page.evaluate('window.fixtureResolve({})')
            await page.wait_for_function('window.__xhsReadOnly.status("second").state === "settled"')
            denied = await page.evaluate('url=>window.__xhsReadOnly.start("bad",url)', LEGACY.replace('/comment/sub/page', '/note/like'))
            self.assertEqual(denied['reason'], 'invalid_readonly_request')
            self.assertEqual(await page.evaluate('window.fixtureUrls.length'), 2)
            self.assertTrue(await page.evaluate('window.__xhsReadOnly.replySizeCompat.rollback()'))
            self.assertTrue(await page.evaluate('window.__xhsReadOnly.start === window.fixtureOriginalStart'))
        finally:
            await page.close()


if __name__ == '__main__':
    unittest.main()
