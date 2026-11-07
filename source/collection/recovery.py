"""Check an interrupted note in a fresh background page before resuming."""
import asyncio
from contextlib import closing
from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace
from urllib.parse import parse_qs, urlsplit

from playwright.async_api import async_playwright

from .config import Config
from .request import navigation_url
from .runner import ADAPTER, Runner
from .store import atomic_write, encode


async def probe_recovery(root):
    root = Path(root).resolve()
    config = json.loads((root / 'run.json').read_text())['config']
    with closing(sqlite3.connect((root / 'state.sqlite').as_uri() + '?mode=ro', uri=True)) as db:
        rows = []
        for condition in ("status='paused' AND body!='unavailable'",
                          "status='finished' AND body='saved'", "status='pending' AND body!='unavailable'"):
            row = db.execute(f'SELECT note_id,url FROM tasks WHERE {condition} ORDER BY updated DESC LIMIT 1').fetchone()
            if row and row not in rows:
                rows.append(row)
    proof = {'checked_at': datetime.now(timezone.utc).isoformat(),
             'note_id': None, 'reason': 'no_probe_note', 'attempts': []}
    try:
        if rows:
            # Bound the entire check, including navigation and browser connection.
            # Compare a failed note with a known-good or fresh candidate. A
            # single unavailable note is not proof that the whole session failed.
            async with asyncio.timeout(22), async_playwright() as playwright:
                browser = await playwright.chromium.connect_over_cdp(config['cdp'], no_defaults=True, timeout=5000)
                runner = Runner(SimpleNamespace(config=Config(cdp=config['cdp'])))
                page = await runner.new_page(browser.contexts[0])
                detail_status = {}
                def received(response):
                    if urlsplit(response.url).path != '/api/sns/web/v1/feed':
                        return
                    try:
                        data = response.request.post_data_json or {}
                        query = parse_qs(urlsplit(response.url).query)
                        note_id = data.get('source_note_id') or data.get('note_id') or query.get('source_note_id', [None])[0]
                        if note_id:
                            detail_status[note_id] = response.status
                    except (ValueError, TypeError, AttributeError):
                        pass
                page.on('response', received)
                try:
                    await page.add_init_script(path=str(ADAPTER))
                    for note_id, url in rows:
                        attempt = {'note_id': note_id, 'reason': 'probe_note_not_ready'}
                        proof['attempts'].append(attempt)
                        proof.update(note_id=note_id, reason=attempt['reason'])
                        target = navigation_url(url)
                        response = await runner.open_note(page, target)
                        attempt['navigation'] = 'document'
                        deadline = asyncio.get_running_loop().time() + 5
                        while True:
                            state = await page.evaluate('id => window.__xhsCollect?.state(id)', note_id)
                            attempt['http_status'] = response.status if response else detail_status.get(note_id)
                            attempt.update(path=urlsplit(page.url).path, title=await page.title())
                            if state and state['blocked']:
                                attempt['reason'] = 'login_or_restriction_still_present'
                                attempt['block_kind'] = state.get('blockKind')
                                break
                            if state and state['note'] and attempt['http_status'] == 200:
                                attempt.update(reason='', note_body_observed=True)
                                break
                            if state and state['unavailable']:
                                attempt['reason'] = 'probe_note_unavailable'
                                break
                            if asyncio.get_running_loop().time() >= deadline:
                                break
                            await asyncio.sleep(.2)
                        proof.update(attempt)
                        if not attempt['reason'] or attempt.get('block_kind') in {'login', 'verification'}:
                            break
                finally:
                    await page.close()
    except Exception as exc:
        proof['reason'] = 'browser_not_ready:' + type(exc).__name__
    finally:
        atomic_write(root / 'diagnostics' / 'recovery-probe.json', encode(proof))
    return proof
