"""The same API/archive pipeline over one resident, authenticated Chrome page.

The native page client signs requests and Chrome sends them. No navigation,
header impersonation, alternate identity, automatic retry, or browser teardown.
"""
import asyncio
import base64
import json
import re
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import parse_qsl, urlencode, urlsplit

from playwright.async_api import async_playwright, Error as PlaywrightError, TimeoutError as PlaywrightTimeoutError

from .http_runner import ApiClientUnavailable, ApiFailure, ApiTransport, HOST

BRIDGE = Path(__file__).resolve().parents[2] / 'static/collection-native-api.js'


class BrowserClient:
    def __init__(self, playwright, page, owned=False):
        self.playwright, self.page, self.owned = playwright, page, owned
        self.repaired_without_response = False
        self.preserve_on_close = False

    async def request(self, method, url, *, data, headers, allow_redirects):
        try:
            async with asyncio.timeout(25):
                response = await self._capture(method, url, data)
                # These refusals require manual review; retain the actual failed
                # tab rather than sending the operator to an unrelated page.
                if response.status_code == 461 and response.headers.get('verifytype') in {'124', '217'}:
                    self.preserve_on_close = True
                self.repaired_without_response = False
                return response
        except (TimeoutError, PlaywrightTimeoutError):
            raise ApiFailure('api_response_failed', {'stage': 'native_response_timeout'}) from None
        except PlaywrightError as exc:
            detail = re.sub(r'https?://[^\s]+', '<url>', str(exc).splitlines()[0])[:400]
            raise ApiClientUnavailable('native_browser_connection', {'error': detail}) from None

    async def _ensure_ready(self):
        state = await self.page.evaluate("""() => ({
            allowed: ['www.xiaohongshu.com', 'www.rednote.com'].includes(location.hostname),
            ready: typeof window.__xhsNativeApi?.start === 'function',
            capability: window.__xhsNativeApi?.probe?.(),
            path: location.pathname, document_state: document.readyState
        })""")
        if not state.get('allowed'):
            raise ApiClientUnavailable('native_page_origin_changed')
        if not state.get('ready'):
            # Repair local globals only, before dispatch. No reload or retry of
            # an API request that may already have reached the server.
            await self.page.evaluate(BRIDGE.read_text())
        if not (state.get('capability') or {}).get('available'):
            try:
                await self.page.wait_for_function("window.__xhsNativeApi?.probe().available === true", timeout=5000)
            except PlaywrightTimeoutError:
                challenge = await self.page.evaluate("/安全验证|滑动验证|拖动滑块|手机号登录|扫码登录/.test(document.body?.innerText || '')")
                if self.owned and not self.repaired_without_response and not challenge:
                    # A discarded/reloaded document may lose its application
                    # runtime. Rebuild this owned tab before dispatch, once;
                    # never replay an already-dispatched API request here.
                    self.repaired_without_response = True
                    if urlsplit(self.page.url).path == '/website-login/error':
                        # Reloading the error route can never restore webpack.
                        # Re-enter the normal app using this same logged-in tab.
                        await self.page.goto('https://www.xiaohongshu.com/explore',
                                             wait_until='domcontentloaded', timeout=10000)
                    else:
                        await self.page.reload(wait_until='domcontentloaded', timeout=10000)
                    await self.page.evaluate(BRIDGE.read_text())
                    return await self._ensure_ready()
                capability = await self.page.evaluate('window.__xhsNativeApi?.probe()')
                raise ApiClientUnavailable('native_client_not_ready', {
                    'capability': capability, 'path': state.get('path'),
                    'document_state': state.get('document_state')}) from None

    async def _capture(self, method, url, data):
        await self._ensure_ready()
        # An explicit CDP buffer is required on this Chrome connection. The
        # generic Playwright Response.body() can see a response whose body is
        # unavailable in its own session. Follow the existing Capture approach.
        target = urlsplit(url)
        query = dict(parse_qsl(target.query, keep_blank_values=True))
        payload = json.loads(data) if data is not None else None
        cdp = await self.page.context.new_cdp_session(self.page)
        future = asyncio.get_running_loop().create_future()
        requests, responses, jobs = {}, {}, set()

        def requested(event):
            request = event['request']
            if event.get('type') == 'Preflight' or request['method'] == 'OPTIONS' or request['method'] != method:
                return
            parts = urlsplit(request['url'])
            if (parts.scheme, parts.netloc, parts.path) != (target.scheme, target.netloc, target.path):
                return
            if method == 'POST':
                try:
                    matches = json.loads(request.get('postData', 'null')) == payload
                except (ValueError, TypeError):
                    matches = False
            else:
                actual = dict(parse_qsl(parts.query, keep_blank_values=True))
                matches = all(actual.get(k) == v for k, v in query.items())
            if matches:
                requests[event['requestId']] = request

        def responded(event):
            if event['requestId'] in requests:
                responses[event['requestId']] = event['response']

        async def read_body(key):
            if future.done():
                return
            try:
                result = await cdp.send('Network.getResponseBody', {'requestId': key})
                raw = base64.b64decode(result['body']) if result.get('base64Encoded') else result['body'].encode()
                response = responses[key]
                if not future.done():
                    future.set_result(SimpleNamespace(content=raw, status_code=int(response['status']),
                        headers={k.lower(): str(v) for k, v in response.get('headers', {}).items()},
                        url=response['url']))
            except Exception as exc:
                if not future.done():
                    detail = re.sub(r'https?://[^\s]+', '<url>', str(exc).splitlines()[0])[:400]
                    future.set_exception(ApiFailure('api_response_failed', {'stage': 'native_cdp_body', 'error': detail}))

        def finished(event):
            if event['requestId'] in responses and not future.done():
                job = asyncio.create_task(read_body(event['requestId']))
                jobs.add(job)
                job.add_done_callback(jobs.discard)

        def failed(event):
            if event['requestId'] in requests and not future.done():
                future.set_exception(ApiFailure('api_response_failed', {'stage': 'native_network_loading_failed',
                                     'error': str(event.get('errorText', 'unknown'))[:200]}))

        cdp.on('Network.requestWillBeSent', requested)
        cdp.on('Network.responseReceived', responded)
        cdp.on('Network.loadingFinished', finished)
        cdp.on('Network.loadingFailed', failed)
        try:
            # Retain raw bodies outside the renderer, including across the site's
            # error-page navigation. Otherwise loadingFinished can be delivered
            # after the renderer has already discarded the response body.
            await cdp.send('Network.enable', {'maxTotalBufferSize': 100_000_000,
                                             'maxResourceBufferSize': 20_000_000,
                                             'enableDurableMessages': True})
            started = await self.page.evaluate('args => window.__xhsNativeApi.start(args)',
                                               {'method': method, 'url': url, 'payload': payload})
            if not started.get('started'):
                raise ApiClientUnavailable(started.get('reason', 'native_client_unavailable'),
                    await self.page.evaluate("""() => ({path:location.pathname,
                        document_state:document.readyState,
                        capability:window.__xhsNativeApi?.probe?.()})"""))
            return await asyncio.wait_for(future, timeout=20)
        finally:
            if not future.done():
                future.cancel()
            if jobs:
                await asyncio.gather(*jobs, return_exceptions=True)
            await cdp.detach()

    async def close(self):
        # Retain manual-review failures; otherwise close only our own tab.
        try:
            if self.owned and not self.preserve_on_close and not self.page.is_closed():
                await self.page.close()
        finally:
            await self.playwright.stop()


class BrowserApiTransport(ApiTransport):
    @classmethod
    async def connect(cls, profile, checkpoint, *, cdp, interval, concurrency, comment_interval=None):
        transport = cls(profile, checkpoint, interval=interval, concurrency=concurrency, comment_interval=comment_interval)
        playwright = await async_playwright().start()
        page = None
        try:
            browser = await playwright.chromium.connect_over_cdp(cdp, no_defaults=True, timeout=5000)
            contexts = [context for context in browser.contexts if any(
                urlsplit(candidate.url).hostname == 'www.xiaohongshu.com' for candidate in context.pages)]
            if len(contexts) != 1:
                raise ApiClientUnavailable('resident_xhs_context_not_unique')
            context = contexts[0]
            cookies = {c['name']: c['value'] for c in await context.cookies([HOST + '/'])}
            if any(cookies.get(k) != transport.cookies.get(k) for k in ('a1', 'web_session')):
                raise ApiClientUnavailable('resident_browser_identity_changed')
            # Exactly one browser bootstrap per collector, not one navigation per note.
            # Do not borrow a tab the user or another tool can navigate underneath us.
            page = await context.new_page()
            await page.goto('https://www.xiaohongshu.com/explore', wait_until='domcontentloaded', timeout=20000)
            await page.evaluate(BRIDGE.read_text())
            await BrowserClient(playwright, page)._ensure_ready()
            await page.evaluate("document.title = 'API采集驻留页（请勿导航）'")
            capability = await page.evaluate('window.__xhsNativeApi.probe()')
            if not capability.get('available'):
                raise ApiClientUnavailable(capability.get('reason', 'native_client_unavailable'))
            challenge = await page.evaluate("/安全验证|滑动验证|拖动滑块|手机号登录|扫码登录/.test(document.body?.innerText || '')")
            if challenge:
                raise ApiClientUnavailable('resident_browser_login_or_verification_visible')
            await transport.client.close()
            transport.client = BrowserClient(playwright, page, owned=True)
            transport.identity.update(wire_transport='native_chrome_page_client', browser_version=browser.version,
                                      signature_source='native_page_client', owned_resident_page=True,
                                      setup_navigation_count=1, setup_traffic='browser_bootstrap_not_corpus')
            return transport
        except BaseException:
            if page is not None and not page.is_closed():
                await page.close()
            await playwright.stop()
            await transport.client.close()
            raise

    def prepare_request(self, method, path, data):
        if method == 'GET':
            return HOST + path + '?' + urlencode(data), None, {}
        return HOST + path, json.dumps(data, ensure_ascii=False, separators=(',', ':')), {}
