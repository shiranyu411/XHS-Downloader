"""Media associations in the existing archive; bytes use the upstream downloader."""
import asyncio
from functools import lru_cache
import hashlib
import json
from pathlib import Path
from urllib.parse import urlsplit

from .store import encode, now


def variants(picture):
    if not isinstance(picture, dict):
        return []
    urls = []
    for key in ('urlPre', 'url_pre', 'url', 'urlDefault', 'url_default', 'masterUrl', 'master_url'):
        if isinstance(picture.get(key), str) and picture[key]:
            urls.append(picture[key])
    for key in ('infoList', 'info_list', 'infolist'):
        for item in picture.get(key, []) or []:
            if isinstance(item, dict) and isinstance(item.get('url'), str):
                urls.append(item['url'])
    for key in ('backupUrls', 'backup_urls'):
        urls.extend(u for u in picture.get(key, []) or [] if isinstance(u, str))
    return list(dict.fromkeys('https:' + u if u.startswith('//') else u for u in urls))


def picture_rows(comment):
    pictures = comment.get('pictures') or []
    return [asset(p, 'comment_image') for p in pictures if isinstance(p, dict)] if isinstance(pictures, list) else []


def asset(raw, kind):
    urls = variants(raw)
    file_id = str(raw.get('fileId') or raw.get('file_id') or '')
    identity = file_id or (urlsplit(urls[0])._replace(query='', fragment='').geturl().split('!')[0] if urls else encode(raw).decode())
    return {'media_id': hashlib.sha256(identity.encode()).hexdigest(), 'kind': kind,
            'file_id': file_id, 'urls': urls, 'metadata': raw}


def note_assets(note):
    result = []
    images = note.get('imageList', note.get('image_list', [])) or []
    for picture in images:
        if not isinstance(picture, dict):
            continue
        result.append(asset(picture, 'cover' if note.get('type') == 'video' else 'image'))
        for streams in (picture.get('stream') or {}).values():
            if isinstance(streams, list) and streams:
                best = max(streams, key=lambda s: s.get('height', 0))
                result.append(asset(best, 'live_video'))
                break
    if note.get('type') == 'video':
        # The upstream video selector handles origin keys and stream preferences.
        from ..application.video import Video
        from ..expansion import Namespace
        try:
            urls = Video.deal_video_link(Namespace(note))
        except (AttributeError, TypeError, KeyError, IndexError):
            urls = []
        # Keep the upstream preferred URL first, but retain the platform's other
        # renditions/backups: the chosen CDN may return a missing or expired file.
        stream = (note.get('video', {}).get('media') or {}).get('stream') or {}
        renditions = [item for values in stream.values() if isinstance(values, list)
                      for item in values if isinstance(item, dict)]
        for item in sorted(renditions, key=lambda item: item.get('height') or 0, reverse=True):
            urls.extend(variants(item))
        urls = list(dict.fromkeys(urls))
        result.append(asset({'url': urls[0] if urls else '', 'backupUrls': urls[1:]}, 'video'))
    return result


def register(store, note_id, comment_id, items, event_id=None):
    with store.db:
        for item in items:
            previous = store.db.execute('SELECT urls FROM media_files WHERE note_id=? AND comment_id=? AND media_id=?',
                                        (note_id, comment_id, item['media_id'])).fetchone()
            urls = list(dict.fromkeys(item['urls'] + (json.loads(previous[0]) if previous else [])))
            store.db.execute('''INSERT INTO media_files(note_id,comment_id,media_id,kind,file_id,urls,metadata,event_id,updated)
                VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(note_id,comment_id,media_id) DO UPDATE SET
                urls=excluded.urls, metadata=excluded.metadata, event_id=excluded.event_id, updated=excluded.updated''',
                (note_id, comment_id, item['media_id'], item['kind'], item['file_id'], encode(urls).decode(),
                 encode(item['metadata']).decode(), event_id, now()))


def register_comments(store, note_id, payload, req, event_id):
    if req.note != note_id or not isinstance(payload, dict) or payload.get('success') is not True or payload.get('code', 0) != 0:
        return
    data = payload.get('data')
    if not isinstance(data, dict) or data.get('note_id', note_id) != note_id:
        return
    for comment in data.get('comments', []) or []:
        if not isinstance(comment, dict) or not comment.get('id') or comment.get('note_id', note_id) != note_id:
            continue
        register(store, note_id, str(comment['id']), picture_rows(comment), event_id)
        if req.kind == 'comments':
            for child in comment.get('sub_comments', []) or []:
                if isinstance(child, dict) and child.get('id') and child.get('note_id', note_id) == note_id and child.get('root_comment_id', comment['id']) == comment['id']:
                    register(store, note_id, str(child['id']), picture_rows(child), event_id)


@lru_cache(maxsize=2048)
def file_digest(path, fingerprint):
    # The stat fingerprint invalidates the cache on replacement or modification;
    # progress polling must not rehash every saved video every two seconds.
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def verified_file(root, row):
    if row.get('status') != 'saved' or not row.get('path') or not row.get('sha256'):
        return None
    path = (Path(root) / row['path']).resolve()
    if not path.is_relative_to(Path(root).resolve()):
        return None
    try:
        stat = path.stat()
        if not path.is_file() or stat.st_size != row['size'] or not row['size']:
            return None
        fingerprint = (stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns)
        return path if file_digest(path, fingerprint) == row['sha256'] else None
    except OSError:
        return None


def media_rows(db, note_id):
    if not db.execute("SELECT 1 FROM sqlite_master WHERE name='media_files'").fetchone():
        return []
    return [{**dict(r), 'urls': json.loads(r['urls']), 'metadata': json.loads(r['metadata'])}
            for r in db.execute('SELECT * FROM media_files WHERE note_id=? ORDER BY rowid', (note_id,))]


class MediaCollector:
    def __init__(self, store, checkpoint=lambda: None, downloader=None):
        self.store, self.checkpoint, self.downloader = store, checkpoint, downloader
        self.owns_downloader = downloader is None

    async def close(self):
        if self.owns_downloader and self.downloader:
            # Do not run Manager.close's empty-directory cleanup over an archive.
            await self.downloader.manager.request_client.close()
            await self.downloader.manager.download_client.close()
            self.downloader = None

    async def __call__(self, note, url=None):
        id_ = note['noteId']
        row = self.store.db.execute("SELECT id FROM events WHERE note_id=? AND kind='note_object' ORDER BY id DESC LIMIT 1", (id_,)).fetchone()
        items = note_assets(note)
        register(self.store, id_, '', items, row[0] if row else None)
        if not items:
            return 'partial'  # No fabricated success for an unknown media shape.
        return await self.download(id_, comments=False)

    async def comments(self, id_):
        # Re-index verified old blobs too: existing archives gain images without recollecting comments.
        from .replay import load_blob, request_from
        for row in self.store.db.execute("SELECT * FROM events WHERE note_id=? AND kind='network_json' AND valid=1 ORDER BY id", (id_,)).fetchall():
            meta = json.loads(row['metadata'])
            if meta.get('kind') not in {'comments', 'replies'} or meta.get('status') != 200:
                continue
            req = request_from({**meta, 'note': meta.get('note', meta.get('note_id'))})
            register_comments(self.store, id_, json.loads(load_blob(self.store.root, row)), req, row['id'])
        return await self.download(id_, comments=True)

    async def download(self, id_, comments):
        rows = [r for r in media_rows(self.store.db, id_) if bool(r['comment_id']) == comments]
        stopped = False
        async def download_one(row):
            nonlocal stopped
            # Different posts can reference the same file. Serialize that asset,
            # not all downloads, and recheck the shared cache after taking the lock.
            lock = self.store.media_locks.setdefault(row['media_id'], asyncio.Lock())
            async with lock:
                async with self.store.media_slots:
                    if stopped:
                        return False
                    try:
                        return await self.download_row(id_, row)
                    except BaseException:
                        stopped = True
                        raise
        # Drain in-flight downloads before closing their shared HTTP client.
        results = await asyncio.gather(*(download_one(row) for row in rows), return_exceptions=True)
        for result in results:
            if isinstance(result, BaseException):
                raise result
        return 'success' if all(results) else 'partial'

    async def download_row(self, id_, row):
        from ..application.download import Download
        from ..module.access import AccessRestricted
        self.checkpoint()
        if verified_file(self.store.root, row):
            return True
        shared = next((dict(r) for r in self.store.db.execute("SELECT * FROM media_files WHERE media_id=? AND status='saved'", (row['media_id'],))
                       if verified_file(self.store.root, dict(r))), None)
        result = {k: shared[k] for k in ('status', 'path', 'sha256', 'size', 'reason')} if shared else {'status': 'failed', 'reason': 'no_usable_url', 'path': None, 'sha256': None, 'size': None}
        if not shared:
            if self.downloader is None:
                self.downloader = Download.for_collection(self.store.root / 'media')
            for url in row['urls']:
                self.checkpoint()
                parts = urlsplit(url)
                if parts.scheme not in {'http', 'https'} or not any((parts.hostname or '') == d or (parts.hostname or '').endswith('.' + d) for d in ('xhscdn.com', 'xiaohongshu.com', 'rednote.com')):
                    continue
                try:
                    result = await self.downloader.download_file(url, self.store.root / 'media' / 'files', row['media_id'],
                                                                 'mp4' if 'video' in row['kind'] else 'jpeg')
                except AccessRestricted as exc:
                    # A denied CDN object is not evidence that the account/API is
                    # restricted. Keep the missing asset explicit; do not retry
                    # alternate URLs or stop unrelated posts because of it.
                    host = parts.hostname or ''
                    if exc.reason not in {'media_http_401', 'media_http_403'} or not (
                            host == 'xhscdn.com' or host.endswith('.xhscdn.com')):
                        raise
                    result = {'status': 'failed', 'reason': exc.reason,
                              'path': None, 'sha256': None, 'size': None}
                    break
                if result['status'] == 'saved':
                    result['path'] = str(Path(result['path']).relative_to(self.store.root))
                    break
        with self.store.db:
            self.store.db.execute('''UPDATE media_files SET status=?,path=?,sha256=?,size=?,reason=?,updated=?
                WHERE note_id=? AND comment_id=? AND media_id=?''',
                (result['status'], result.get('path'), result.get('sha256'), result.get('size'), result.get('reason'), now(), id_, row['comment_id'], row['media_id']))
        self.store.save(None, id_, 'media_file_result', encode({**result, 'media_id': row['media_id'], 'comment_id': row['comment_id']}))
        return result['status'] == 'saved'
