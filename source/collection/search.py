"""Durable keyword pagination through the browser's existing signed request client."""
import asyncio
from datetime import datetime, timezone
import hashlib
import json
import re
import time
from urllib.parse import urlencode, urlsplit, parse_qs
from uuid import uuid4

from playwright.async_api import Error as BrowserError

from .capture import Capture
from .protocol import Ledger, restricted
from .replay import load_blob
from .screening import classify_title, priority, profile_for
from .store import encode

SEARCH_URL = 'https://edith.xiaohongshu.com/api/sns/web/v1/search/notes'


def queued_keywords(store):
    """Read append-only user requests without a second SQLite writer."""
    path = store.root / '.keywords-requested.json'
    if not path.exists():
        return []
    keywords = json.loads(path.read_text())
    if not isinstance(keywords, list):
        raise ValueError('追加关键词必须是列表')
    return keywords


def add_keywords(store, keywords, progress=None):
    progress = json.loads(store.control('search_progress') or '[]') if progress is None else progress
    existing = {s['keyword'] for s in progress}
    for keyword in keywords:
        if not isinstance(keyword, str) or not keyword.strip() or len(keyword) > 200:
            raise ValueError('关键词须为 1–200 个字符')
        keyword = keyword.strip()
        if keyword not in existing:
            progress.append({'keyword': keyword, 'page_size': 20, 'search_id': uuid4().hex,
                             'sort': 'general', 'note_type': 0, 'next_page': 1, 'pages': [],
                             'status': 'pending', 'reason': '', 'source': 'search_api'})
            existing.add(keyword)
    store.control('search_progress', encode(progress).decode())
    return progress


def accept_page(store, state, row):
    """Register the terminal page's items BEFORE interpreting its has_more flag."""
    from .runner import Paused
    meta = json.loads(row['metadata'])
    request = json.loads(meta.get('post_data') or '{}')
    if request.get('keyword') != state['keyword'] or request.get('page') != state['next_page']:
        raise ValueError('search_response_scope_mismatch')
    payload = json.loads(load_blob(store.root, row))
    if restricted(payload, meta.get('status')):
        raise Paused('access_restricted')
    if meta.get('status') != 200 or not isinstance(payload, dict) or payload.get('success') is not True or payload.get('code', 0) != 0:
        raise ValueError('search_response_failed')
    data = payload.get('data')
    if not isinstance(data, dict):
        raise ValueError('search_items_structure_unknown')
    items = data.get('items')
    # The live API can return only {has_more: false} on an empty terminal page.
    if items is None and data.get('has_more') is False:
        items = []
    if not isinstance(items, list):
        raise ValueError('search_items_structure_unknown')
    links, ids, skipped, ranks = [], [], 0, []
    profile = profile_for(store.root)
    for item in items:
        if not isinstance(item, dict):
            skipped += 1
            continue
        id_ = item.get('id') or item.get('note_id')
        if item.get('model_type', 'note') not in {'note', 'note_card'} or not isinstance(id_, str) or not re.fullmatch(r'[a-fA-F0-9]{24}', id_):
            skipped += 1
            continue
        id_ = id_.lower()
        token = item.get('xsec_token', item.get('xsecToken'))
        source = item.get('xsec_source', item.get('xsecSource')) or 'pc_search'
        query = urlencode({'xsec_token': token or '', 'xsec_source': source})
        links.append(f'https://www.xiaohongshu.com/search_result/{id_}?{query}')
        ids.append(id_)
        title = str((item.get('note_card') or {}).get('display_title') or '').strip()
        verdict, why = classify_title(title, [state['keyword']], profile)
        ranks.append((id_, priority(verdict, [state['keyword']], profile), verdict, why))
    store.enqueue(links, {'mode': 'keyword_search', 'request': request, 'event_id': row['id'], 'page': request['page']})
    # Title screening orders the queue; a post already ranked keeps its best rank.
    known = {r[0]: r[1] for r in store.db.execute(
        f"SELECT note_id, priority FROM task_priority WHERE note_id IN ({','.join('?' * len(ranks))})", [r[0] for r in ranks])} if ranks else {}
    store.prioritize([r for r in ranks if r[0] not in known or 0 <= r[1] < known[r[0]] or known[r[0]] < 0 <= r[1]])
    fingerprint = hashlib.sha256(encode(sorted(set(ids)))).hexdigest()
    repeated = bool(ids) and any(p['page'] != request['page'] and p['fingerprint'] == fingerprint for p in state['pages'])
    more = data.get('has_more')
    state['pages'] = [p for p in state['pages'] if p['page'] != request['page']]
    state['pages'].append({'page': request['page'], 'event_id': row['id'], 'ids': sorted(set(ids)),
                           'fingerprint': fingerprint, 'has_more': more, 'skipped_non_notes': skipped})
    if type(more) is not bool:
        state.update(status='partial', reason='missing_has_more')
    elif repeated and more:
        state.update(status='partial', reason='repeated_search_page')
    elif more and not ids:
        state.update(status='partial', reason='empty_search_page_with_has_more')
    elif more:
        state.update(status='running', reason='', next_page=request['page'] + 1)
    else:
        state.update(status='complete', reason='has_more_false', end_event=row['id'])
    return state


async def refused(page):
    """Stop on a refusal, captcha or login page, checked before any page-changed test."""
    from .runner import Paused, gate_reason
    state = await page.evaluate("window.__xhsCollect?.state('') ?? {blocked: false}")
    if reason := gate_reason(state, page.url):
        raise Paused(reason)


async def discover_keywords(runner, context, collect_posts=False):
    from .runner import ADAPTER, REQUEST_ADAPTER, Paused
    store = runner.store
    progress = add_keywords(store, queued_keywords(store))
    def persist():
        # Mutate the live list so newly queued terms join this search pass.
        add_keywords(store, queued_keywords(store), progress)
        store.export(force=False)
    for state in progress:
        if state['status'] == 'complete':
            continue
        runner.checkpoint()
        await runner.gate.wait(lambda: runner.pause_requested)
        await runner.paced(None)
        page = await runner.new_page(context)
        capture = Capture(store, None, None, Ledger(None))
        runner.active_captures.add(capture)
        state.update(status='running', reason='')
        try:
            await capture.start(page)
            await page.add_init_script(path=str(ADAPTER))
            await page.add_init_script(path=str(REQUEST_ADAPTER))
            await page.goto('https://www.xiaohongshu.com/search_result?' + urlencode({'keyword': state['keyword'], 'source': 'web_explore_feed'}),
                            wait_until='domcontentloaded', timeout=30000)
            deadline = time.monotonic() + 20
            while True:
                runner.checkpoint(capture)
                await refused(page)
                capability = await page.evaluate('window.__xhsReadOnly.probe()')
                if capability['available'] or time.monotonic() >= deadline:
                    break
                await asyncio.sleep(.2)
            await capture.drain(timeout=runner.config.operation_timeout)
            # Capture the site's real search shape once, including filters and search ID.
            if state['next_page'] == 1 and not state.get('template'):
                for row in store.db.execute("SELECT * FROM events WHERE kind='network_json' ORDER BY id DESC").fetchall():
                    meta = json.loads(row['metadata'])
                    if meta.get('session_id') != capture.session_id or meta.get('path') != '/api/sns/web/v1/search/notes':
                        continue
                    try:
                        request = json.loads(meta.get('post_data') or '{}')
                    except ValueError:
                        continue
                    if request.get('keyword') == state['keyword'] and request.get('page') == 1:
                        state['template'] = request
                        for key in ('page_size', 'search_id', 'sort', 'note_type'):
                            state[key] = request.get(key, state[key])
                        accept_page(store, state, row)
                        persist()
                        if collect_posts:
                            await runner.collect_turn(context, wait=False)
                        break
            if (not capability['available'] or not capability.get('search', False)) and state['status'] != 'complete':
                # Existing DOM discovery is a bounded fallback, never full search coverage.
                state.update(source='browser_dom', status='partial', reason=capability.get('reason', 'search_client_unavailable'))
                await page.evaluate('a => window.__xhsCollect.startDiscovery(a, 0, true)', runner.config.max_scrolls)
                while True:
                    runner.checkpoint(capture)
                    await refused(page)
                    snapshot = await page.evaluate('window.__xhsCollect.discoveryProgress()')
                    store.enqueue(snapshot['links'], {'mode': 'keyword_dom_fallback', 'keyword': state['keyword'], 'scrolls': snapshot['count']})
                    persist()
                    if snapshot['done']:
                        state['reason'] = 'dom_' + snapshot['reason']
                        store.save(None, None, 'search_dom_coverage', encode(snapshot), {'keyword': state['keyword']})
                        if collect_posts:
                            await runner.collect_turn(context, wait=False)
                        break
                    await runner.paced(capture)
                    await page.evaluate('window.__xhsCollect.stepDiscovery()')
                continue
            while state['status'] == 'running':
                runner.checkpoint(capture)
                await refused(page)
                current = urlsplit(page.url)
                if current.hostname != 'www.xiaohongshu.com' or current.path.rstrip('/') != '/search_result' or parse_qs(current.query).get('keyword') != [state['keyword']]:
                    raise Paused('search_page_changed')
                # A results page is a page view like a post open: it takes a turn at the same gate,
                # so searching never adds views on top of the open interval.
                await runner.gate.wait(lambda: runner.pause_requested)
                await runner.paced(capture)
                payload = {**state.get('template', {}), **{k: state[k] for k in ('keyword', 'page_size', 'search_id', 'sort', 'note_type')}, 'page': state['next_page']}
                op = capture.begin_operation(SEARCH_URL, payload)
                reason = 'search_response_incomplete'
                try:
                    started = await page.evaluate('a => window.__xhsReadOnly.search(a.id, a.payload)', {'id': op['id'], 'payload': payload})
                    if not started['started']:
                        raise ValueError(started['reason'])
                    deadline = time.monotonic() + runner.config.operation_timeout
                    while True:
                        runner.checkpoint(capture)
                        feedback = await page.evaluate('id => window.__xhsReadOnly.status(id)', op['id'])
                        if op['done'].done() and feedback and feedback['state'] != 'pending':
                            break
                        if time.monotonic() > deadline:
                            raise TimeoutError('search_response_timeout')
                        await asyncio.sleep(.05)
                    event = op['done'].result()
                    row = store.db.execute('SELECT * FROM events WHERE id=?', (event,)).fetchone()
                    accept_page(store, state, row)
                    if runner.pacer:
                        runner.pacer.succeeded()
                    reason = None
                    persist()
                    runner.message = f"搜索 {state['keyword']}：已保存 {len(state['pages'])} 页，发现 {len(store.tasks())} 篇"
                finally:
                    capture.end_operation(reason)
                if collect_posts:
                    await runner.collect_turn(context, wait=False)
        except Paused as exc:
            if state['status'] != 'complete':
                state.update(status='restricted' if 'access' in str(exc) or 'login' in str(exc) else 'paused',
                             reason=str(exc), paused_at=datetime.now(timezone.utc).isoformat())
            raise
        except BrowserError as exc:
            state.update(status='partial', reason='browser_timeout' if type(exc).__name__ == 'TimeoutError' else 'browser_connection_error')
        except (ValueError, TimeoutError) as exc:
            state.update(status='partial', reason=str(exc))
        finally:
            try:
                await capture.stop()
            finally:
                runner.active_captures.discard(capture)
                persist()
                if runner.host:
                    await runner.host.close_page(page)
                else:
                    await page.close()
