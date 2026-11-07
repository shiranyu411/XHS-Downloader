"""Direct API collection over the existing archive. No per-note browser navigation.

Cookie/base headers are captured from the dedicated Chrome by browser_session.
A single request gate replaces (not stacks with) the old navigation/pacing gates.
"""
import asyncio
from collections import deque
from dataclasses import asdict, replace
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import json
from pathlib import Path
import sqlite3
import stat
import time
from urllib.parse import parse_qsl, urlsplit
from uuid import uuid4

from curl_cffi.requests import AsyncSession
from curl_cffi.requests.exceptions import RequestException
from xhshow import CryptoConfig, SessionManager, Xhshow

from .media import MediaCollector, register_comments
from .protocol import classify, restricted
from .replay import chain_key, load_blob, resume_seed
from .request import request_url
from .store import atomic_write, encode

HOST = 'https://edith.xiaohongshu.com'
DETAIL = '/api/sns/web/v1/feed'
COMMENT = '/api/sns/web/v2/comment/page'
REPLIES = '/api/sns/web/v2/comment/sub/page'


class Stopped(Exception):
    pass


class ApiFailure(Exception):
    def __init__(self, reason, evidence=None):
        super().__init__(reason)
        self.reason, self.evidence = reason, evidence or {}


class ApiClientUnavailable(ApiFailure):
    """A shared browser/client failure, not a bad note; stop before next dispatch."""


class ApiRestricted(ApiFailure):
    pass


class NoteUnavailable(ApiFailure):
    pass


def decode_payload(raw, status):
    try:
        payload = json.loads(raw)
    except (ValueError, TypeError):
        payload = None
    proof = {'http_status': status, 'business_code': payload.get('code') if isinstance(payload, dict) else None}
    if restricted(payload, status):
        raise ApiRestricted('api_access_restricted', proof)
    if isinstance(payload, dict) and str(payload.get('code')) in {'-510000', '-510001'}:
        raise NoteUnavailable('note_unavailable', proof)
    if status != 200 or not isinstance(payload, dict) or payload.get('success') is not True or payload.get('code', 0) != 0:
        raise ApiFailure('api_response_failed', proof)
    return payload


def normalized_note(payload, id_):
    items = (payload.get('data') or {}).get('items') or []
    item = next((x for x in items if isinstance(x, dict) and x.get('id') == id_), None)
    if item is None or not isinstance(item.get('note_card'), dict):
        raise ApiFailure('detail_ownership_or_shape_invalid')
    raw = item['note_card']
    if raw.get('note_id', raw.get('noteId', id_)) != id_:
        raise ApiFailure('detail_ownership_or_shape_invalid')
    # Existing downloader consumes browser-style camelCase. The untouched API
    # envelope is separately archived before this reversible projection.
    def camel(value):
        if isinstance(value, list):
            return [camel(v) for v in value]
        if isinstance(value, dict):
            def key(k):
                a, *rest = k.split('_')
                return a + ''.join(s[:1].upper() + s[1:] for s in rest)
            return {key(k): camel(v) for k, v in value.items()}
        return value
    return {**camel(raw), 'noteId': id_}


class ApiTransport:
    def __init__(self, profile, checkpoint, interval=1.0, concurrency=3, comment_interval=None):
        path = Path(profile)
        if stat.S_IMODE(path.stat().st_mode) & 0o077:
            raise ValueError('browser_session_file_must_be_private')
        auth = json.loads(path.read_text())
        self.cookies = dict(auth['cookies'])
        if not self.cookies.get('web_session') or not self.cookies.get('a1'):
            raise ValueError('authenticated_cookie_missing')
        dynamic = {'cookie', 'host', 'content-length', 'connection', 'transfer-encoding',
                   'x-s', 'x-t', 'x-s-common', 'x-b3-traceid', 'x-xray-traceid', 'x-rap-param'}
        self.headers = {k.lower(): v for k, v in auth['headers'].items()
                        if not k.startswith(':') and k.lower() not in dynamic}
        defaults = CryptoConfig()
        observed = Xhshow().decode_xs(auth['headers']['x-s'])
        template = {**defaults.SIGNATURE_DATA_TEMPLATE,
                    **{k: observed[k] for k in ('x0', 'x1', 'x2', 'x4') if k in observed}, 'x3': ''}
        common = {**defaults.SIGNATURE_XSCOMMON_TEMPLATE,
                  'x1': template['x0'], 'x2': template['x2'], 'x3': template['x1']}
        config = defaults.with_overrides(PUBLIC_USERAGENT=self.headers['user-agent'],
                                        SIGNATURE_DATA_TEMPLATE=template,
                                        SIGNATURE_XSCOMMON_TEMPLATE=common)
        self.signer, self.sign_session = Xhshow(config), SessionManager(config)
        # curl's latest available Chrome transport; actual browser headers are
        # passed explicitly. This is not a claim of Chrome 156 TLS equivalence.
        self.client = AsyncSession(impersonate='chrome', default_headers=False,
                                   max_clients=concurrency, timeout=20)
        self.checkpoint, self.interval = checkpoint, interval
        self.comment_interval = interval if comment_interval is None else comment_interval
        self.slots, self.gate = asyncio.Semaphore(concurrency), asyncio.Lock()
        self.last = self.last_comment = 0.0
        self.sent = self.successful = 0
        self.blocked = False
        self.identity = {'browser_version': auth['browser_version'], 'source_path': auth['source_path'],
                         'signature_version': template['x0'], 'platform': template['x2'],
                         'headers_from_browser': True, 'cookie_values_logged': False}

    def prepare_request(self, method, path, data):
        # Sign at dispatch, never reuse a previously captured signature.
        if method == 'GET':
            signed = self.signer.sign_headers_get(path, self.cookies, params=data, session=self.sign_session)
            url, body = self.signer.build_url(HOST + path, data), None
        else:
            signed = self.signer.sign_headers_post(path, self.cookies, payload=data,
                                                  session=self.sign_session, x_rap=True)
            url, body = HOST + path, json.dumps(data, ensure_ascii=False, separators=(',', ':'))
        headers = {**self.headers, **signed,
                   'cookie': '; '.join(f'{k}={v}' for k, v in self.cookies.items())}
        if body is not None:
            headers['content-type'] = 'application/json;charset=UTF-8'
        return url, body, headers

    async def dispatch(self, method, path, data):
        is_comment = path in {COMMENT, REPLIES}
        while True:
            self.checkpoint()
            if self.blocked:
                raise Stopped(getattr(self, 'block_reason', 'api_access_restricted'))
            async with self.gate:
                due = max(self.last + self.interval,
                          self.last_comment + self.comment_interval if is_comment else 0)
                remaining = due - time.monotonic()
                if remaining <= 0:
                    # Reserve a network slot only when this endpoint is due.
                    # Sleeping comment requests must not hold all slots/the gate
                    # and prevent an eligible detail request from proceeding.
                    await self.slots.acquire()
                    try:
                        self.checkpoint()
                        if self.blocked:
                            raise Stopped(getattr(self, 'block_reason', 'api_access_restricted'))
                        prepared = self.prepare_request(method, path, data)
                        self.last = time.monotonic()
                        if is_comment:
                            self.last_comment = self.last
                        self.sent += 1
                        return prepared
                    except BaseException:
                        self.slots.release()
                        raise
            await asyncio.sleep(min(.1, remaining))

    async def request(self, method, path, data):
        if path not in {DETAIL, COMMENT, REPLIES} or method != ('POST' if path == DETAIL else 'GET'):
            raise ValueError('unsupported_collection_request')
        url, body, headers = await self.dispatch(method, path, data)
        try:
            started = time.monotonic()
            response = await self.client.request(method, url, data=body, headers=headers, allow_redirects=False)
            elapsed = time.monotonic() - started
            raw, status = response.content, response.status_code
            try:
                payload = json.loads(raw)
            except ValueError:
                payload = None
            if restricted(payload, status):
                self.blocked = True
            successful = status == 200 and isinstance(payload, dict) and payload.get('success') is True and payload.get('code', 0) == 0
            if successful:
                self.successful += 1
            # The server's actual wait instruction is retained, never invented.
            return raw, status, {'url': getattr(response, 'url', url), 'duration_seconds': elapsed,
                                 'retry_after': response.headers.get('retry-after'),
                                 'verify_type': response.headers.get('verifytype'),
                                 'verification_id_present': bool(response.headers.get('verifyuuid')),
                                 'content_type': response.headers.get('content-type'), 'business_success': successful}
        except ApiClientUnavailable:
            # Close the gate before releasing the slot to queued notes.
            self.blocked, self.block_reason = True, 'api_client_unavailable'
            raise
        finally:
            self.slots.release()

    async def close(self):
        await self.client.close()


class HTTPRunner:
    def __init__(self, store, transport=None, *, retry_api_once=False):
        self.store, self.config = store, store.config
        self.transport = transport
        self.retry_api_once = retry_api_once
        self.recovery_request_active = False
        self.running = self.paused = self.pause_requested = False
        self.message = '准备直接 API 采集'
        self.failure = None
        self.failed_responses = 0
        self.completed = 0

    def pause(self):
        self.pause_requested = True
        self.message = '正在暂停 API 采集，保存在途响应'

    def checkpoint(self):
        if self.pause_requested or (self.store.root / '.pause-requested').exists():
            raise Stopped(self.failure or 'user_pause')

    def stop_client(self, exc):
        self.failure, self.pause_requested = 'api_client_unavailable', True
        proof = {'reason': exc.reason, **exc.evidence}
        self.store.control('workspace_blocked', self.failure)
        self.store.control('api_stop_evidence', encode(proof).decode())
        self.store.save(None, None, 'api_stop', encode(proof))

    async def fetch(self, attempt, id_, method, path, data):
        self.checkpoint()
        if path == REPLIES and self.store.control('api_reply_blocked') and not self.recovery_request_active:
            raise Stopped('reply_endpoint_deferred')
        operation = uuid4().hex
        started = time.monotonic()
        raw, status, detail = await self.transport.request(method, path, data)
        req = classify(detail['url'], json.dumps(data) if method == 'POST' else None)
        meta = {**asdict(req), **detail, 'method': method, 'status': status,
                'operation_id': operation, 'session_id': self.session_id,
                'post_data': json.dumps(data) if method == 'POST' else None,
                'transport': 'http_api', 'wire_transport': self.transport.identity.get('wire_transport', 'curl_cffi'),
                'total_seconds': time.monotonic() - started}
        event = self.store.save(attempt, id_, 'network_json', raw, meta)
        try:
            payload = decode_payload(raw, status)
        except ApiRestricted as exc:
            proof = {**exc.evidence, 'event_id': event, 'path': path, 'note_id': id_,
                     'retry_after': detail.get('retry_after'), 'verify_type': detail.get('verify_type'),
                     'verification_id_present': detail.get('verification_id_present')}
            self.failure = 'api_access_restricted'
            self.pause_requested = True
            self.store.control('workspace_blocked', 'api_access_restricted')
            self.store.control('api_stop_evidence', encode(proof).decode())
            self.store.save(None, id_, 'api_stop', encode(proof))
            raise
        except ApiFailure as exc:
            exc.evidence.update(event_id=event, path=path)
            raise
        return payload, req, event

    async def collect(self, task):
        id_ = task['note_id']
        ledger, chain, stats = resume_seed(self.store, id_)
        attempt = self.store.begin(id_)
        self.store.control(chain_key(id_), encode(chain + [attempt]).decode())
        result = {'body': task['body'], 'comments': 'partial', 'media': task['media'],
                  'comment_media': task['comment_media'], 'reason': 'observed_end', 'strategy': 'http_api'}
        status = 'finished'
        assets = MediaCollector(self.store, self.checkpoint)
        body_media = None
        try:
            self.checkpoint()
            if stats is not None:
                self.store.save(attempt, id_, 'resume_seed', encode(stats))
            note = self.store.note_object(id_)
            query = dict(parse_qsl(urlsplit(task['url']).query))
            if not query.get('xsec_token'):
                raise ApiFailure('access_parameter_missing')
            if note is None:
                self.message = f'API 获取正文：{id_}'
                payload, _, _ = await self.fetch(attempt, id_, 'POST', DETAIL, {
                    'source_note_id': id_, 'image_formats': ['jpg', 'webp', 'avif'],
                    'extra': {'need_body_topic': '1'}, 'xsec_token': query['xsec_token'],
                    'xsec_source': query.get('xsec_source', 'pc_search')})
                note = normalized_note(payload, id_)
            self.store.save(attempt, id_, 'note_object', encode(note), {'verified_note_id': id_, 'transport': 'http_api'})
            result['body'] = 'saved'
            self.store.update_parts(id_, {'body': 'saved'})
            from ..application.download import Download
            assets.downloader = Download.for_collection(self.store.root / 'media')
            assets.downloader.headers = {k: self.transport.headers[k] for k in ('user-agent', 'referer', 'accept-language')
                                         if k in self.transport.headers}

            async def save_media(comments=False):
                key = 'comment_media' if comments else 'media'
                try:
                    result[key] = await (assets.comments(id_) if comments else assets(note, task['url']))
                except Stopped:
                    raise
                except (OSError, sqlite3.Error):
                    raise
                except Exception as exc:
                    # A CDN-object failure is local; HTTP API authentication is
                    # classified separately by fetch(). Do not erase its reason.
                    result[key] = 'partial'
                    self.store.save(attempt, id_, 'media_stage_error', encode({'stage': key, 'error': type(exc).__name__}))
                self.store.update_parts(id_, {key: result[key]})

            body_media = asyncio.create_task(save_media())
            defer_replies = bool(self.store.run.get('api_options', {}).get('defer_replies'))
            while not (ledger.primary.complete() if defer_replies else ledger.complete()):
                self.checkpoint()
                request = ledger.next_request(primary_only=defer_replies)
                if request is None:
                    raise ApiFailure('pagination_evidence_invalid')
                url = request_url(request, {}, task['url'])
                path = urlsplit(url).path
                data = dict(parse_qsl(urlsplit(url).query, keep_blank_values=True))
                self.message = f'API 采集{request.kind}：{id_}'
                payload, observed, event = await self.fetch(attempt, id_, 'GET', path, data)
                ledger.observe(observed, payload)
                register_comments(self.store, id_, payload, observed, event)
                record = ledger.record()
                self.store.save(attempt, id_, 'comment_progress', encode(record), {'raw_event': event}, progress=record)
            result['comments'] = 'complete_observed' if ledger.complete() else 'partial'
            if defer_replies and result['comments'] != 'complete_observed':
                status, result['reason'] = 'paused', 'reply_endpoint_deferred'
            self.store.update_parts(id_, {'comments': result['comments']})
            await save_media(comments=True)
            await body_media
            self.failed_responses = 0
            self.completed += all(result[k] == expected for k, expected in (
                ('body', 'saved'), ('comments', 'complete_observed'), ('media', 'success'), ('comment_media', 'success')))
        except NoteUnavailable:
            result.update(body='unavailable', reason='note_unavailable')
        except (ApiRestricted, Stopped) as exc:
            status, result['reason'] = 'paused', str(exc)
            if isinstance(exc, ApiRestricted):
                ledger.blocked = True
                result['comments'] = 'blocked'
        except (ApiFailure, RequestException) as exc:
            result['reason'] = str(exc) if isinstance(exc, ApiFailure) else type(exc).__name__
            if isinstance(exc, ApiClientUnavailable):
                status = 'paused'
                self.stop_client(exc)
            elif isinstance(exc, RequestException) or result['reason'] not in {
                    'access_parameter_missing', 'detail_ownership_or_shape_invalid', 'pagination_evidence_invalid'}:
                self.failed_responses += 1
            if self.failed_responses >= 3 or getattr(exc, 'evidence', {}).get('business_code') == 300015:
                self.failure, self.pause_requested = 'api_protocol_or_transport_failures', True
                self.store.control('workspace_blocked', self.failure)
                self.store.control('api_stop_evidence', encode(getattr(exc, 'evidence', {})).decode())
            self.store.save(attempt, id_, 'api_failure', encode({'reason': result['reason'],
                            'evidence': getattr(exc, 'evidence', {})}))
        except asyncio.CancelledError:
            status, result['reason'] = 'interrupted', 'cancelled'
            raise
        except BaseException:
            self.failure, self.pause_requested = 'api_storage_or_internal_error', True
            status, result['reason'] = 'interrupted', self.failure
            raise
        finally:
            if body_media is not None:
                outcomes = await asyncio.gather(body_media, return_exceptions=True)
                for outcome in outcomes:
                    if isinstance(outcome, BaseException) and not isinstance(outcome, Stopped):
                        if isinstance(outcome, OSError):
                            self.failure, self.pause_requested = 'storage_error', True
                            status, result['reason'] = 'interrupted', 'storage_error'
            await assets.close()
            record = ledger.record()
            self.store.save(attempt, id_, 'comment_progress', encode(record), progress=record)
            self.store.finish(attempt, result, status)
        return result

    def retry_evidence(self, *, confirmed_event_id=None, confirmed_login_event_id=None):
        """Review is explicit and scoped to one refusal, never inherited by a new one.

        A login confirmation may review historical 461/0 responses carrying
        verify_type=124 or 217 only after the operator confirms login and checks
        the authenticated browser. It does not claim that a CAPTCHA was solved
        or authorize automatic retry.
        """
        scoped = self.store.control('api_reply_blocked') if not self.store.control('workspace_blocked') else None
        proof = json.loads(scoped or self.store.control('api_stop_evidence') or '{}')
        row = self.store.db.execute("SELECT * FROM events WHERE id=? AND kind='network_json' AND valid=1",
                                    (proof.get('event_id'),)).fetchone()
        if row is None:
            raise Stopped('api_retry_evidence_missing')
        meta = json.loads(row['metadata'])
        refused = json.loads(load_blob(self.store.root, row))
        message = str(refused.get('msg', refused.get('message', ''))) if isinstance(refused, dict) else ''
        rate_limited = meta.get('status') == 429 or (
            meta.get('status') == 461 and isinstance(refused, dict) and str(refused.get('code')) == '300013')
        account_reviewed = (confirmed_event_id == row['id'] and meta.get('status') == 461
                            and isinstance(refused, dict) and str(refused.get('code')) == '300011')
        login_reviewed = (confirmed_login_event_id == row['id'] and meta.get('status') == 461
                          and isinstance(refused, dict) and str(refused.get('code')) == '0'
                          and str(meta.get('verify_type')) in {'124', '217'} and meta.get('verification_id_present') is True)
        if not login_reviewed and (not (rate_limited or account_reviewed) or meta.get('verify_type') or meta.get('verification_id_present') or
                any(word in message.lower() for word in ('登录', '验证', 'captcha', 'login'))):
            raise Stopped('api_retry_requires_access_review')
        wait = meta.get('retry_after')
        if wait:
            try:
                value = str(wait).strip()
                deadline = (datetime.fromisoformat(row['seen']).timestamp() + int(value) if value.isdigit()
                            else parsedate_to_datetime(value).timestamp())
            except (ValueError, TypeError, OverflowError):
                raise Stopped('api_retry_after_invalid') from None
            if deadline > time.time():
                raise Stopped('server_retry_after_active')
        return proof, row, meta

    async def retry_restricted_page(self, *, confirmed_event_id=None, confirmed_login_event_id=None):
        """Retry only the refused page; keep the block until validated data is saved."""
        proof, row, meta = self.retry_evidence(confirmed_event_id=confirmed_event_id,
                                             confirmed_login_event_id=confirmed_login_event_id)
        url = urlsplit(meta.get('url', ''))
        method = meta.get('method')
        req = classify(meta.get('url', ''), meta.get('post_data'))
        if (url.scheme != 'https' or url.netloc != 'edith.xiaohongshu.com' or
                url.path not in {DETAIL, COMMENT, REPLIES} or
                method != ('POST' if url.path == DETAIL else 'GET') or req is None or
                req.note != row['note_id'] or req.note != proof.get('note_id') or url.path != proof.get('path')):
            raise Stopped('api_retry_request_evidence_invalid')
        task = self.store.db.execute('SELECT * FROM tasks WHERE note_id=?', (req.note,)).fetchone()
        if task is None:
            raise Stopped('api_retry_task_missing')
        ledger, chain, _ = resume_seed(self.store, req.note)
        if req.kind != 'detail':
            target = ledger.primary if req.kind == 'comments' else ledger.replies.get(req.parent)
            cursor = target.next_cursor() if target is not None else None
            if cursor is None:
                raise Stopped('api_retry_page_not_pending')
            # The same chain can have advanced since its old refusal (for
            # example from inline replies on later primary pages). Resume the
            # verified current cursor, never fetch an already-committed page.
            req = replace(req, cursor=cursor)
        data = (json.loads(meta['post_data']) if method == 'POST'
                else dict(parse_qsl(url.query, keep_blank_values=True)))
        if req.kind != 'detail':
            data['cursor'] = req.cursor
        attempt = self.store.begin(req.note)
        self.store.control(chain_key(req.note), encode(chain + [attempt]).decode())
        result = {k: task[k] for k in ('body', 'comments', 'media', 'comment_media')}
        result.update(reason='api_retry_not_completed', strategy='http_api')
        try:
            self.message = 'API 人工请求的一次性恢复：只重试上次被拒分页'
            self.recovery_request_active = True
            try:
                payload, observed, event = await self.fetch(attempt, req.note, method, url.path, data)
            finally:
                self.recovery_request_active = False
            if req.kind == 'detail':
                note = normalized_note(payload, req.note)
                self.store.save(attempt, req.note, 'note_object', encode(note),
                                {'verified_note_id': req.note, 'transport': 'http_api'})
                result['body'] = 'saved'
            else:
                ledger.observe(observed, payload)
                register_comments(self.store, req.note, payload, observed, event)
                record = ledger.record()
                self.store.save(attempt, req.note, 'comment_progress', encode(record),
                                {'raw_event': event}, progress=record)
                if ledger.blocked or ledger.gaps or ledger.primary.problems or any(c.problems for c in ledger.replies.values()):
                    raise ApiFailure('api_retry_pagination_invalid', {'event_id': event})
                result['comments'] = 'complete_observed' if ledger.complete() else 'partial'
                if self.store.db.execute("SELECT 1 FROM media_files WHERE note_id=? AND comment_id!='' AND status!='saved' LIMIT 1",
                                         (req.note,)).fetchone():
                    result['comment_media'] = 'partial'
            result['reason'] = 'api_retry_page_saved'
        except (ApiFailure, RequestException) as exc:
            result['reason'] = exc.reason if isinstance(exc, ApiFailure) else type(exc).__name__
            if isinstance(exc, ApiRestricted) and req.kind != 'detail':
                result['comments'] = 'blocked'
            if not isinstance(exc, ApiRestricted):
                self.store.save(attempt, req.note, 'api_failure', encode({
                    'reason': result['reason'], 'evidence': getattr(exc, 'evidence', {})}))
            raise Stopped('api_retry_failed:' + result['reason']) from None
        finally:
            self.store.finish(attempt, result, 'paused')
        receipt = {'previous_event_id': row['id'], 'response_event_id': event, 'note_id': req.note,
                   'cursor_rebased_to_checkpoint': req.cursor != (meta.get('cursor') or ''),
                   'path': url.path, 'reason': ('user_confirmed_login_retry' if confirmed_login_event_id is not None else
                                               'user_confirmed_access_retry' if confirmed_event_id is not None else 'user_requested_single_retry'),
                   'manual_verification_claimed': confirmed_event_id is not None,
                   'manual_login_claimed': confirmed_login_event_id is not None}
        self.store.save(None, req.note, 'api_resume', encode(receipt))
        self.store.control('api_resume_evidence', encode(receipt).decode())
        self.store.control('workspace_blocked', '')
        scoped = json.loads(self.store.control('api_reply_blocked') or '{}')
        if scoped.get('event_id') == row['id']:
            self.store.control('api_reply_blocked', '')
            self.store.control('api_primary_stage_complete', '')

    def tasks(self):
        rows = [dict(r) for r in self.store.db.execute("""SELECT * FROM tasks WHERE body!='unavailable'
            AND NOT (body='saved' AND comments='complete_observed' AND media='success' AND comment_media='success')
            ORDER BY rowid""")]
        if self.store.run.get('api_options', {}).get('defer_replies'):
            def primary_work_remains(row):
                if row['body'] != 'saved' or row['media'] != 'success' or row['comment_media'] != 'success':
                    return True
                ledger, _, _ = resume_seed(self.store, row['note_id'])
                return not ledger.primary.complete()
            rows = [row for row in rows if primary_work_remains(row)]
        fresh = deque(r for r in rows if r['body'] != 'saved')
        resumed = deque(r for r in rows if r['body'] == 'saved')
        while fresh or resumed:
            for _ in range(3):
                if fresh:
                    yield fresh.popleft()
            if resumed:
                yield resumed.popleft()

    async def run(self, discover=False, retry=False):
        if self.running:
            raise RuntimeError('already_running')
        self.running = True
        self.session_id = uuid4().hex
        pending = set()
        try:
            options = self.store.run.get('api_options', {})
            blocked = self.store.control('workspace_blocked')
            restore_replies = bool(self.store.control('api_reply_blocked') and self.retry_api_once)
            if restore_replies and not blocked:
                blocked = 'api_access_restricted'
            elif self.store.control('api_reply_blocked') and not options.get('defer_replies') and not self.retry_api_once:
                raise Stopped('reply_endpoint_requires_explicit_retry')
            if blocked:
                self.failure = blocked
                if blocked != 'api_access_restricted' or not self.retry_api_once:
                    raise Stopped(blocked)
            until = max(float(self.store.control(k) or 0) for k in ('pacing_cooldown_until', 'pacing_session_rest_until'))
            if until > time.time():
                raise Stopped('existing_cooldown_active')
            searches = json.loads(self.store.control('search_progress') or '[]')
            if discover or any(s['status'] != 'complete' for s in searches):
                raise Stopped('finish_existing_search_scope_before_api_collection')
            if blocked:
                self.retry_evidence()
            if self.transport is None:
                profile = self.store.root / '.credentials/browser-session.json'
                settings = {'interval': float(options.get('interval', 1)), 'concurrency': int(options.get('concurrency', 3)),
                            'comment_interval': float(options.get('comment_interval', options.get('interval', 1)))}
                if options.get('transport', 'direct_http') == 'native_chrome':
                    from .browser_api import BrowserApiTransport
                    self.transport = await BrowserApiTransport.connect(profile, self.checkpoint, cdp=self.config.cdp, **settings)
                elif options.get('transport', 'direct_http') == 'direct_http':
                    self.transport = ApiTransport(profile, self.checkpoint, **settings)
                else:
                    raise Stopped('unsupported_api_transport')
            self.store.save(None, None, 'session_config', encode({**self.config.record(), 'engine': 'http_api',
                            'api_options': options, 'browser': self.transport.identity}))
            if blocked:
                await self.retry_restricted_page()
                if restore_replies and self.store.control('api_reply_blocked'):
                    # A general primary-page refusal and an older reply-scope
                    # refusal require separate successful, durable pages.
                    await self.retry_restricted_page()
                self.failure = None
                if restore_replies:
                    options = {**options, 'defer_replies': False}
                    self.store.run['api_options'] = options
                    atomic_write(self.store.root / 'run.json', encode(self.store.run))
                    self.store.save(None, None, 'api_scopes_restored', encode({'api_options': options,
                                    'enabled_scopes': ['detail', 'comments', 'replies']}))
            if options.get('defer_replies'):
                self.store.control('api_primary_stage_complete', '')
            iterator = iter(self.tasks())
            exhausted = False
            while not exhausted or pending:
                while not exhausted and not self.pause_requested and len(pending) < 16:
                    task = next(iterator, None)
                    if task is None:
                        exhausted = True
                    else:
                        pending.add(asyncio.create_task(self.collect(task)))
                if not pending:
                    break
                done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
                for job in done:
                    job.result()
                if self.pause_requested:
                    exhausted = True
            self.paused = self.pause_requested
            self.message = (f'API 已暂停：{self.failure or "user_pause"}' if self.paused else
                            f'API 本轮结束；完成 {self.completed} 篇，失败部分保留断点')
            if not self.pause_requested and options.get('defer_replies') and self.store.control('api_reply_blocked'):
                complete = next(self.tasks(), None) is None
                self.store.control('api_primary_stage_complete', '1' if complete else '')
                self.paused = True
                self.message = ('正文、一级评论和已知媒体阶段已结束；二级回复仍待恢复，未宣称全量完成' if complete else
                                '本轮正文/一级评论阶段结束，失败部分和二级回复保留待补')
        except ApiClientUnavailable as exc:
            self.stop_client(exc)
            self.paused = True
            self.message = f'API 客户端不可用，已停止：{exc}'
        except Stopped as exc:
            self.paused = True
            self.message = f'API 未发起或已暂停：{exc}'
        finally:
            self.pause_requested = True
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)
            if self.transport:
                self.store.control('api_last_session', encode({'session_id': self.session_id,
                    'at': datetime.now(timezone.utc).isoformat(), 'sent': self.transport.sent,
                    'successful_responses': self.transport.successful, 'completed_posts': self.completed,
                    'stop_reason': self.failure}).decode())
                await self.transport.close()
            self.running = False
