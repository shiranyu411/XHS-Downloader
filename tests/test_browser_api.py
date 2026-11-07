"""Offline CDP tests: exact request ownership, no preflight bodies, raw errors, no headers forwarded."""
import base64
import json
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock

from source.collection.browser_api import BrowserApiTransport, BrowserClient
from source.collection.http_runner import ApiClientUnavailable, COMMENT, DETAIL, HOST

NOTE = 'a' * 24


class FakeCDP:
    def __init__(self, encoded=False):
        self.handlers, self.read_ids, self.detached = {}, [], False
        self.encoded = encoded

    def on(self, event, callback):
        self.handlers[event] = callback

    async def send(self, method, params):
        if method == 'Network.enable':
            assert params['maxResourceBufferSize'] > 0
            assert params['enableDurableMessages'] is True
            return {}
        assert method == 'Network.getResponseBody'
        self.read_ids.append(params['requestId'])
        assert params['requestId'] == 'business', 'preflight/wrong-note body must not be read'
        body = b'{"code":300013,"success":false}'
        return {'body': base64.b64encode(body).decode() if self.encoded else body.decode(), 'base64Encoded': self.encoded}

    async def detach(self):
        self.detached = True

    def emit(self, key, request, typ='XHR'):
        self.handlers['Network.requestWillBeSent']({'requestId': key, 'type': typ, 'request': request})
        self.handlers['Network.responseReceived']({'requestId': key, 'response': {
            'url': request['url'], 'status': 461, 'headers': {'Retry-After': '60', 'VerifyType': 'fixture'}}})
        self.handlers['Network.loadingFinished']({'requestId': key})


class Page:
    def __init__(self, started=True, encoded=False):
        self.started, self.calls, self.cdp = started, [], FakeCDP(encoded)
        self.context = SimpleNamespace(new_cdp_session=self.new_cdp)

    async def new_cdp(self, page):
        assert page is self
        return self.cdp

    async def evaluate(self, expression, arguments=None):
        if arguments is None:
            return {'allowed': True, 'ready': True, 'capability': {'available': True}}
        self.calls.append(arguments)
        if self.started:
            request = {'url': arguments['url'], 'method': arguments['method']}
            if arguments['payload'] is not None:
                request['postData'] = json.dumps(arguments['payload'])
            # Even if the wrapper labels a preflight with the business method,
            # CDP's explicit resource type must keep it out of corpus capture.
            self.cdp.emit('preflight', request, typ='Preflight')
            wrong = {**request, 'url': request['url'].replace(NOTE, 'b' * 24)}
            if 'postData' in request:
                wrong['postData'] = json.dumps({**arguments['payload'], 'source_note_id': 'b' * 24})
            self.cdp.emit('wrong_note', wrong)
            self.cdp.emit('wrong_method', {**request, 'method': 'DELETE'})
            self.cdp.emit('business', request)
        return {'started': self.started, 'reason': 'fixture_dispatch_refused'}


class NativeClientTests(unittest.IsolatedAsyncioTestCase):
    async def test_close_preserves_manual_review_tab_only(self):
        for status, verify_type, preserved in ((200, None, False), (461, '217', True), (461, '124', True)):
            with self.subTest(status=status, verify_type=verify_type):
                page = SimpleNamespace(is_closed=lambda: False, close=AsyncMock())
                playwright = SimpleNamespace(stop=AsyncMock())
                client = BrowserClient(playwright, page, owned=True)
                client._capture = AsyncMock(return_value=SimpleNamespace(
                    status_code=status, headers={'verifytype': verify_type}))
                await client.request('GET', HOST + COMMENT, data=None, headers={}, allow_redirects=False)
                await client.close()
                self.assertEqual(page.close.await_count, 0 if preserved else 1)
                playwright.stop.assert_awaited_once()
                client._capture.assert_awaited_once()

    async def test_original_error_is_captured_without_forwarding_python_headers(self):
        url = HOST + COMMENT + '?note_id=' + NOTE + '&cursor=&xsec_token=fixture'
        page = Page()
        result = await BrowserClient(None, page).request('GET', url, data=None,
                         headers={'cookie': 'must-not-forward', 'x-s': 'synthetic'}, allow_redirects=False)
        self.assertEqual(result.status_code, 461)
        self.assertEqual(json.loads(result.content)['code'], 300013)
        self.assertEqual(result.headers['retry-after'], '60')
        self.assertEqual(set(page.calls[0]), {'method', 'url', 'payload'})
        self.assertNotIn('must-not-forward', json.dumps(page.calls))
        self.assertEqual(page.cdp.read_ids, ['business'])
        self.assertTrue(page.cdp.detached)

    async def test_post_response_must_belong_to_exact_payload(self):
        page = Page()
        payload = {'source_note_id': NOTE, 'xsec_token': 'fixture'}
        await BrowserClient(None, page).request('POST', HOST + DETAIL, data=json.dumps(payload), headers={}, allow_redirects=False)
        self.assertEqual(page.cdp.read_ids, ['business'])

    async def test_base64_response_is_kept_as_original_bytes(self):
        page = Page(encoded=True)
        result = await BrowserClient(None, page).request('GET', HOST + COMMENT + '?note_id=' + NOTE,
                                                       data=None, headers={}, allow_redirects=False)
        self.assertEqual(result.content, b'{"code":300013,"success":false}')

    async def test_rejected_dispatch_is_not_retried(self):
        page = Page(started=False)
        with self.assertRaisesRegex(ApiClientUnavailable, 'fixture_dispatch_refused'):
            await BrowserClient(None, page).request('GET', HOST + COMMENT, data=None, headers={}, allow_redirects=False)
        self.assertEqual(len(page.calls), 1)
        self.assertEqual(page.cdp.read_ids, [])
        self.assertTrue(page.cdp.detached)

    def test_browser_preparation_contains_no_synthetic_headers(self):
        transport = BrowserApiTransport.__new__(BrowserApiTransport)
        url, body, headers = transport.prepare_request('GET', COMMENT, {'note_id': NOTE, 'cursor': '', 'xsec_token': 'fixture'})
        self.assertEqual(headers, {})
        self.assertIsNone(body)
        self.assertIn('cursor=', url)
        _, body, headers = transport.prepare_request('POST', DETAIL, {'source_note_id': NOTE, 'xsec_token': 'fixture'})
        self.assertEqual(headers, {})
        self.assertEqual(json.loads(body)['source_note_id'], NOTE)


if __name__ == '__main__':
    unittest.main()
