"""Local browser checks: posts open only at /explore/{id}; refusal pages stop the batch."""
import json
from pathlib import Path
import unittest
from urllib.parse import urlsplit

from source.collection.config import Config
from source.collection.runner import ADAPTER, Paused, Runner, gate_reason
from source.collection.store import Store
from tests import test_collection as fixtures

FIRST, SECOND, THIRD = 'a' * 24, 'b' * 24, 'c' * 24
REDIRECTS = {'refused': '/website-login/error?redirectPath=fixture',
             'captcha': '/website-login/captcha?redirectPath=fixture', 'broken': '/explore'}


class DirectNavigationTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = fixtures.BrowserTests.asyncSetUp
    asyncTearDown = fixtures.BrowserTests.asyncTearDown

    async def route(self, route):
        url = urlsplit(route.request.url)
        if url.hostname == 'www.xiaohongshu.com':
            self.calls.append(('document', url.path, url.query))
            if url.path.startswith('/website-login/'):
                title = '安全验证' if url.path.endswith('/captcha') else '安全限制'
                await route.fulfill(content_type='text/html', body=f'<title>{title}</title>')
            elif url.path.startswith('/explore/') and self.mode in REDIRECTS:
                # Client-side redirect: Playwright routes it, unlike the target of an HTTP 302.
                await route.fulfill(content_type='text/html',
                                    body=f'<script>location.replace({json.dumps(REDIRECTS[self.mode])})</script>')
            else:
                note = url.path.rsplit('/', 1)[-1]
                html = '''<script>
                    window.__INITIAL_STATE__={note:{noteDetailMap:{'NOTE':{note:{noteId:'NOTE',title:'fixture'}}}}};
                    const client={get:u=>fetch(u).then(r=>r.json()),makeUri(){},instance:{},
                      interceptors:{request:{},response:{},dispatch:{},result:{}}};
                    window.webpackChunkxhs_pc_web={push:c=>c[2]({c:{fixture:{exports:client}}})};
                </script>'''.replace('NOTE', note)
                await route.fulfill(content_type='text/html', body=html)
        elif url.hostname == 'edith.xiaohongshu.com':
            self.calls.append(('api', url.path, url.query))
            await route.fulfill(content_type='application/json',
                body=json.dumps({'success': True, 'code': 0, 'data': {'comments': [], 'has_more': False, 'cursor': ''}}),
                headers={'Access-Control-Allow-Origin': '*'})
        else:
            await route.abort()

    def batch(self, ids):
        store = Store(Path(self.temp.name) / 'batch', Config(open_interval=0, page_interval=0,
                      media=False, **fixtures.UNPACED))
        store.enqueue([f'https://www.xiaohongshu.com/search_result/{id_}?xsec_token=t-{id_[0]}&xsec_source=pc_search'
                       for id_ in ids], {})
        return store

    def documents(self):
        return [(path, query) for kind, path, query in self.calls if kind == 'document']

    async def test_search_result_task_opens_explore_url_with_its_token(self):
        store = self.batch([FIRST])
        try:
            result = await Runner(store).collect(self.context, store.next_task())
            self.assertEqual((result['body'], result['comments']), ('saved', 'complete_observed'))
            self.assertEqual(self.documents(), [('/explore/' + FIRST, 'xsec_token=t-a&xsec_source=pc_search')])
        finally:
            store.close()

    async def test_refusal_page_stops_batch_without_retry(self):
        self.mode = 'refused'
        store = self.batch([FIRST, SECOND])
        runner = Runner(store)
        try:
            with self.assertRaisesRegex(Paused, '^access_restricted$'):
                await runner.collect_next(self.context)
            self.assertEqual([path for path, _ in self.documents()], ['/explore/' + FIRST, '/website-login/error'])
            task = store.tasks()[0]
            self.assertEqual((task['status'], task['reason']), ('paused', 'access_restricted'))
            self.assertIsNone(store.next_retry_time(), 'a refused open is never retried by another route')
        finally:
            store.close()

    async def test_captcha_page_waits_for_a_person(self):
        self.mode = 'captcha'
        store = self.batch([FIRST])
        try:
            with self.assertRaisesRegex(Paused, '^login_or_verification_required$'):
                await Runner(store).collect_next(self.context)
            self.assertEqual([path for path, _ in self.documents()], ['/explore/' + FIRST, '/website-login/captcha'])
        finally:
            store.close()

    async def test_unexpected_redirects_retry_later_then_stop_after_three(self):
        self.mode = 'broken'
        store = self.batch([FIRST, SECOND, THIRD])
        runner = Runner(store)
        try:
            self.assertTrue(await runner.collect_next(self.context))
            self.assertTrue(await runner.collect_next(self.context))
            self.assertTrue(store.tasks()[0]['reason'].endswith('_retry_scheduled'), store.tasks()[0])
            self.assertIsNotNone(store.next_retry_time())
            with self.assertRaisesRegex(Paused, 'consecutive_page_failures'):
                await runner.collect_next(self.context)
            self.assertEqual([path for path, _ in self.documents()], ['/explore/' + FIRST, '/explore',
                             '/explore/' + SECOND, '/explore', '/explore/' + THIRD, '/explore'])
        finally:
            store.close()

    async def test_block_kind_prefers_a_gate_that_needs_a_person(self):
        page = await self.context.new_page()
        await page.goto('https://www.xiaohongshu.com/website-login/error')
        await page.add_script_tag(path=str(ADAPTER))
        cases = {'<title>安全限制</title>': 'access', '<title>安全验证</title>': 'verification',
                 '<title>安全限制</title><div class="captcha-container">x</div>': 'verification',
                 '<div class="login-container">登录</div>': 'login'}
        for html, kind in cases.items():
            await page.evaluate('html => { document.head.innerHTML = ""; document.body.innerHTML = html; '
                                'const t = /<title>(.*)<\\/title>/.exec(html); document.title = t ? t[1] : ""; }', html)
            state = await page.evaluate("window.__xhsCollect.state('')")
            self.assertEqual((state['blocked'], state['blockKind']), (True, kind), html)


class GateReasonTests(unittest.TestCase):
    def test_refusal_needs_time_and_gates_need_a_person(self):
        site = 'https://www.xiaohongshu.com'
        cases = [({'blocked': True, 'blockKind': 'access'}, '/explore/' + FIRST, 'access_restricted'),
                 ({'blocked': True, 'blockKind': 'access'}, '/website-login/error?redirectPath=x', 'access_restricted'),
                 ({}, '/website-login/error', 'access_restricted'),
                 ({'blocked': True, 'blockKind': 'verification'}, '/website-login/error', 'login_or_verification_required'),
                 ({'blocked': True, 'blockKind': 'access'}, '/website-login/captcha', 'login_or_verification_required'),
                 ({'blocked': True, 'blockKind': 'login'}, '/explore/' + FIRST, 'login_or_verification_required'),
                 ({}, '/login', 'login_or_verification_required'),
                 ({'blocked': False}, '/explore/' + FIRST, None)]
        for state, path, reason in cases:
            self.assertEqual(gate_reason(state, site + path), reason, (state, path))


if __name__ == '__main__':
    unittest.main()
