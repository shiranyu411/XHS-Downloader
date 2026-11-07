"""Capture this project's browser session locally, without navigating or printing secrets."""
import argparse
import asyncio
from datetime import datetime, timezone
import json
import os
from pathlib import Path
from urllib.parse import urlsplit

from playwright.async_api import async_playwright

from .config import Config
from .store import atomic_write, encode


async def capture_session(batch, cdp, timeout=45):
    Config(cdp=cdp)
    folder = Path(batch).resolve() / '.credentials'
    folder.mkdir(mode=0o700, exist_ok=True)
    folder.chmod(0o700)
    async with async_playwright() as playwright:
        browser = await playwright.chromium.connect_over_cdp(cdp, no_defaults=True, timeout=5000)
        contexts = [c for c in browser.contexts if any(
            urlsplit(p.url).hostname == 'www.xiaohongshu.com' for p in c.pages)]
        if len(contexts) != 1:
            raise RuntimeError('dedicated_xhs_context_not_unique')
        context = contexts[0]
        future = asyncio.get_running_loop().create_future()
        jobs = set()

        async def observe(response):
            u = urlsplit(response.url)
            if future.done() or response.status != 200 or u.hostname != 'edith.xiaohongshu.com':
                return
            if u.path not in {'/api/sns/web/v2/comment/page', '/api/sns/web/v2/comment/sub/page',
                              '/api/sns/web/v1/feed', '/api/sns/web/v1/search/notes'}:
                return
            headers = await response.request.all_headers()
            if headers.get('x-s') and not future.done():
                future.set_result({'path': u.path, 'headers': headers})

        def response_handler(response):
            job = asyncio.create_task(observe(response))
            jobs.add(job)
            job.add_done_callback(jobs.discard)

        context.on('response', response_handler)
        try:
            observed = await asyncio.wait_for(future, timeout)
            cookies = await context.cookies(['https://edith.xiaohongshu.com/'])
            values = {c['name']: c['value'] for c in cookies}
            if not values.get('web_session') or not values.get('a1'):
                raise RuntimeError('authenticated_xhs_cookie_missing')
            record = {'captured_at': datetime.now(timezone.utc).isoformat(), 'cdp': cdp,
                      'browser_version': browser.version, 'source_path': observed['path'],
                      'headers': observed['headers'], 'cookies': values}
            target = folder / 'browser-session.json'
            atomic_write(target, encode(record))
            target.chmod(0o600)
            return {'path': str(target), 'captured_at': record['captured_at'],
                    'browser_version': browser.version, 'source_path': observed['path'],
                    'cookie_count': len(values), 'authenticated_cookie_present': True,
                    'header_names': sorted(observed['headers']), 'platform_requests_sent': 0}
        finally:
            context.remove_listener('response', response_handler)
            if jobs:
                await asyncio.gather(*jobs, return_exceptions=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--batch', type=Path, required=True)
    parser.add_argument('--cdp', default='http://127.0.0.1:9223')
    args = parser.parse_args()
    os.umask(0o077)
    print(json.dumps(asyncio.run(capture_session(args.batch, args.cdp)), ensure_ascii=False))


if __name__ == '__main__':
    main()
