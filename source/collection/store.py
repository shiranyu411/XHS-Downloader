"""Single-writer archive. Blob rename precedes the SQLite event/progress transaction."""
import asyncio
import gzip
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import time
from datetime import datetime, timezone
from uuid import uuid4
from urllib.parse import parse_qs, urlsplit

from .config import Config, note_id
from .screening import TOOL_REASON


def now():
    return datetime.now(timezone.utc).isoformat()


def encode(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode()


def source_keyword(metadata):
    meta = json.loads(metadata or '{}')
    return meta.get('keyword') or (meta.get('request') or {}).get('keyword')


def atomic_write(path: Path, body: bytes):
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    fd, temp = tempfile.mkstemp(prefix=".writing-", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(body)
            f.flush()
            os.fsync(f.fileno())
        os.replace(temp, path)
        if os.name != "nt":
            fd = os.open(path.parent, os.O_RDONLY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
    finally:
        if os.path.exists(temp):
            os.unlink(temp)


def archive_busy(root):
    """Inspect the existing writer lock; do not create or change an archive."""
    path = Path(root) / '.lock'
    if not path.exists():
        return False
    with path.open('rb') as lock:
        try:
            if os.name == 'nt':
                import msvcrt
                msvcrt.locking(lock.fileno(), msvcrt.LK_NBRLCK, 1)
                msvcrt.locking(lock.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl
                fcntl.flock(lock, fcntl.LOCK_SH | fcntl.LOCK_NB)
                fcntl.flock(lock, fcntl.LOCK_UN)
        except OSError:
            return True
    return False


class Store:
    def __init__(self, root: Path, config: Config | None = None):
        self.media_locks = {}  # per-asset in-process locks shared by all post workers
        self.media_slots = asyncio.Semaphore(12)
        self.last_export = 0.0
        self.root = root.resolve()
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        # Prevent two collectors (or a resume command) from changing the same batch.
        self.lock = (self.root / ".lock").open("a+b")
        try:
            if os.name == "nt":
                import msvcrt
                self.lock.write(b"0")
                self.lock.flush()
                self.lock.seek(0)
                msvcrt.locking(self.lock.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            self.lock.close()
            raise RuntimeError("该批次正在由另一个进程使用") from None
        path = self.root / "run.json"
        if path.exists():
            self.run = json.loads(path.read_text())
            self.config = Config(**{k: v for k, v in self.run["config"].items() if k in Config.__dataclass_fields__})
        else:
            self.config = config or Config()
            self.run = {"id": uuid4().hex, "created": now(), "config": self.config.record()}
            atomic_write(path, encode(self.run))
        self.db = sqlite3.connect(self.root / "state.sqlite")
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.executescript("""
        CREATE TABLE IF NOT EXISTS tasks(
          note_id TEXT PRIMARY KEY, url TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending',
          body TEXT NOT NULL DEFAULT 'partial', comments TEXT NOT NULL DEFAULT 'not_requested',
          media TEXT NOT NULL DEFAULT 'disabled', reason TEXT, updated TEXT);
        CREATE TABLE IF NOT EXISTS sources(
          id INTEGER PRIMARY KEY, note_id TEXT, url TEXT, metadata TEXT, seen TEXT);
        CREATE TABLE IF NOT EXISTS attempts(
          id TEXT PRIMARY KEY, note_id TEXT, started TEXT, ended TEXT, status TEXT,
          result TEXT);
        CREATE INDEX IF NOT EXISTS attempts_note_id ON attempts(note_id);
        CREATE TABLE IF NOT EXISTS events(
          id INTEGER PRIMARY KEY, attempt TEXT, note_id TEXT, kind TEXT,
          seen TEXT, path TEXT, sha256 TEXT, metadata TEXT, valid INTEGER DEFAULT 1);
        CREATE TABLE IF NOT EXISTS progress(
          attempt TEXT PRIMARY KEY, data TEXT);
        CREATE TABLE IF NOT EXISTS controls(key TEXT PRIMARY KEY, value TEXT);
        CREATE TABLE IF NOT EXISTS media_files(
          note_id TEXT NOT NULL, comment_id TEXT NOT NULL DEFAULT '', media_id TEXT NOT NULL,
          kind TEXT NOT NULL, file_id TEXT, urls TEXT NOT NULL, metadata TEXT NOT NULL,
          event_id INTEGER, status TEXT NOT NULL DEFAULT 'pending', path TEXT,
          sha256 TEXT, size INTEGER, reason TEXT, updated TEXT,
          PRIMARY KEY(note_id,comment_id,media_id));
        CREATE INDEX IF NOT EXISTS events_note_kind ON events(note_id,kind);
        CREATE INDEX IF NOT EXISTS media_id_status ON media_files(media_id,status);
        CREATE TABLE IF NOT EXISTS deferred_tasks(
          note_id TEXT PRIMARY KEY, kind TEXT NOT NULL, retry_after REAL NOT NULL, reason TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS task_priority(
          note_id TEXT PRIMARY KEY, priority INTEGER NOT NULL, verdict TEXT, reason TEXT);
        """)
        if 'comment_media' not in {r[1] for r in self.db.execute('PRAGMA table_info(tasks)')}:
            self.db.execute("ALTER TABLE tasks ADD COLUMN comment_media TEXT NOT NULL DEFAULT 'not_requested'")
        if 'title' not in {r[1] for r in self.db.execute('PRAGMA table_info(tasks)')}:
            self.db.execute("ALTER TABLE tasks ADD COLUMN title TEXT NOT NULL DEFAULT ''")
        with self.db:
            self.db.execute("UPDATE tasks SET status='interrupted', reason='process_restart' WHERE status='running'")
            self.db.execute("UPDATE attempts SET status='interrupted', ended=? WHERE status='running'", (now(),))
        self.recovery = self.verify()

    def enqueue(self, urls, source):
        # Validate the whole message before committing; ack only after COMMIT.
        items = [(note_id(url), url) for url in urls]
        before = self.db.total_changes
        with self.db:
            for id_, url in items:
                self.db.execute("INSERT OR IGNORE INTO tasks(note_id,url,updated) VALUES(?,?,?)", (id_, url, now()))
                self.db.execute("INSERT INTO sources(note_id,url,metadata,seen) VALUES(?,?,?,?)", (id_, url, encode(source).decode(), now()))
                # A later bare anchor must not replace a usable observed access URL.
                previous = self.db.execute("SELECT url FROM tasks WHERE note_id=?", (id_,)).fetchone()[0]
                if parse_qs(urlsplit(url).query).get("xsec_token") or not parse_qs(urlsplit(previous).query).get("xsec_token"):
                    self.db.execute("UPDATE tasks SET url=? WHERE note_id=?", (url, id_))
        return {"observations": len(items), "tasks": self.db.execute("SELECT count(*) FROM tasks").fetchone()[0], "committed": self.db.total_changes > before}

    def begin(self, id_):
        attempt = uuid4().hex
        with self.db:
            current = self.db.execute('SELECT status FROM tasks WHERE note_id=?', (id_,)).fetchone()
            if current is None or current[0] == 'running':
                raise RuntimeError('post_already_running_or_not_enqueued')
            if self.config.comment_page_quantum is not None:
                fresh = not self.db.execute("SELECT 1 FROM attempts WHERE note_id=? LIMIT 1", (id_,)).fetchone()
                streak = int(self.control('scheduler_fresh_streak') or 0)
                self.db.execute("INSERT OR REPLACE INTO controls VALUES('scheduler_fresh_streak',?)",
                                (str(streak + 1 if fresh else 0),))
            self.db.execute("UPDATE tasks SET status='running', updated=? WHERE note_id=?", (now(), id_))
            self.db.execute("INSERT INTO attempts VALUES(?,?,?,?,?,?)", (attempt, id_, now(), None, "running", None,))
        return attempt

    def save(self, attempt, id_, kind, body: bytes, metadata=None, progress=None):
        digest = hashlib.sha256(body).hexdigest()
        relative = f"raw/{digest[:2]}/{digest}.gz"
        path = self.root / relative
        if not path.exists():
            atomic_write(path, gzip.compress(body, mtime=0))
        else:
            original = path.read_bytes()
            try:
                intact = hashlib.sha256(gzip.decompress(original)).hexdigest() == digest
            except (OSError, ValueError, EOFError):
                intact = False
            if not intact:
                # Only repair a blob already invalidated by recovery. Preserve its
                # broken bytes; old events remain invalid, new attempts get new events.
                invalid = self.db.execute("SELECT 1 FROM events WHERE path=? AND valid=0 LIMIT 1", (relative,)).fetchone()
                valid = self.db.execute("SELECT 1 FROM events WHERE path=? AND valid=1 LIMIT 1", (relative,)).fetchone()
                if not invalid or valid:
                    raise OSError(f"已存文件校验失败: {relative}")
                backup = self.root / "diagnostics" / "corrupt_blobs" / f"{digest}-{uuid4().hex}.gz"
                atomic_write(backup, original)
                atomic_write(path, gzip.compress(body, mtime=0))
        with self.db:
            cur = self.db.execute(
                "INSERT INTO events(attempt,note_id,kind,seen,path,sha256,metadata) VALUES(?,?,?,?,?,?,?)",
                (attempt, id_, kind, now(), relative, digest, encode(metadata or {}).decode()),
            )
            if progress is not None:
                self.db.execute("INSERT OR REPLACE INTO progress VALUES(?,?)", (attempt, encode(progress).decode()))
            if kind == 'note_object' and id_:
                note = json.loads(body)
                if note.get('noteId') == id_:
                    self.db.execute('UPDATE tasks SET title=? WHERE note_id=?', (str(note.get('title') or ''), id_))
        return cur.lastrowid

    def finish(self, attempt, result, status="finished"):
        with self.db:
            row = self.db.execute("SELECT note_id FROM attempts WHERE id=?", (attempt,)).fetchone()
            self.db.execute("UPDATE attempts SET ended=?,status=?,result=? WHERE id=?", (now(), status, encode(result).decode(), attempt))
            self.update_parts(row[0], result, commit=False)
            self.db.execute("UPDATE tasks SET status=?,reason=?,updated=? WHERE note_id=?",
                            (status, result["reason"], now(), row[0]))
        self.export(force=False)

    def update_parts(self, id_, result, commit=True):
        # An omitted/disabled phase never erases a successful independent phase.
        previous = self.db.execute("SELECT * FROM tasks WHERE note_id=?", (id_,)).fetchone()
        for key in ('body', 'media', 'comments', 'comment_media'):
            value = result.get(key)
            if value is None or value in {'disabled', 'not_requested'}:
                continue
            if key == 'body' and previous[key] == 'saved' and value != 'saved':
                continue
            self.db.execute(f"UPDATE tasks SET {key}=?,updated=? WHERE note_id=?", (value, now(), id_))
        if commit:
            self.db.commit()

    def note_object(self, id_):
        from .replay import load_blob
        row = self.db.execute("SELECT * FROM events WHERE note_id=? AND kind='note_object' AND valid=1 ORDER BY id DESC LIMIT 1", (id_,)).fetchone()
        if row is None:
            return None
        note = json.loads(load_blob(self.root, row))
        if note.get('noteId') != id_:
            raise ValueError('body_ownership_mismatch')
        return note

    def tasks(self):
        return [dict(r) for r in self.db.execute("SELECT * FROM tasks ORDER BY rowid")]

    def keywords(self, id_):
        found = []
        for (metadata,) in self.db.execute("SELECT metadata FROM sources WHERE note_id=? ORDER BY id", (id_,)):
            keyword = source_keyword(metadata)
            if keyword and keyword not in found:
                found.append(keyword)
        return found

    def prioritize(self, rows):
        """(note_id, priority, verdict, reason); lower runs first, negative never runs."""
        with self.db:
            self.db.executemany("INSERT OR REPLACE INTO task_priority VALUES(?,?,?,?)", rows)

    def next_task(self):
        # Unscreened posts (no task_priority row) rank with core 待定.
        eligible = ("t.status IN ('pending','interrupted','paused') AND NOT EXISTS "
                    "(SELECT 1 FROM deferred_tasks d WHERE d.note_id=t.note_id "
                    f"AND d.retry_after>{time.time():.6f}) AND coalesce(p.priority, 1) >= 0")
        if self.config.sample_per_keyword and (row := self.keyword_sample(eligible)):
            return row
        if self.config.comment_page_quantum is not None:
            # Three fresh posts, then one oldest unfinished post. Unlike sorting
            # only by rowid or freshness, neither a huge thread nor new arrivals
            # can starve the other lane. The streak survives process restarts.
            seen = "EXISTS(SELECT 1 FROM attempts a WHERE a.note_id=t.note_id)"
            fresh = self.db.execute(f"SELECT t.* FROM tasks t LEFT JOIN task_priority p USING(note_id) WHERE {eligible} AND NOT {seen} "
                                    "ORDER BY coalesce(p.priority, 1), t.rowid LIMIT 1").fetchone()
            resumed = self.db.execute(f"SELECT t.* FROM tasks t LEFT JOIN task_priority p USING(note_id) WHERE {eligible} AND {seen} "
                                      "ORDER BY coalesce(p.priority, 1), t.updated, t.rowid LIMIT 1").fetchone()
            streak = int(self.control('scheduler_fresh_streak') or 0)
            row = resumed if resumed is not None and (fresh is None or streak >= 3) else fresh
        else:
            row = self.db.execute(f"SELECT t.* FROM tasks t LEFT JOIN task_priority p USING(note_id) WHERE {eligible} "
                                  "ORDER BY coalesce(p.priority, 1), t.rowid LIMIT 1").fetchone()
        return dict(row) if row else None

    def keyword_sample(self, eligible):
        """While a searched keyword is short of sample_per_keyword on-topic posts, the one with the
        fewest settled posts goes next: its started posts first, then a fixed pseudo-random order
        with AI-tool titles last. Slang-only posts whose titles show no AI link (priority 3) wait
        for the priority order."""
        size = self.db.execute('SELECT count(*), max(id) FROM sources').fetchone()
        if getattr(self, 'keyword_cache', (None,))[0] != tuple(size):
            by_note = {}
            for id_, metadata in self.db.execute('SELECT note_id, metadata FROM sources'):
                if keyword := source_keyword(metadata):
                    by_note.setdefault(id_, set()).add(keyword)
            self.keyword_cache = (tuple(size), by_note)
        by_note = self.keyword_cache[1]
        searching = {s['keyword'] for s in json.loads(self.control('search_progress') or '[]') if s.get('status') != 'complete'}
        # A settled post needs no more requests: its comments meet the rule, its body proved
        # off-topic, or it is gone. Off-topic opens settle too, so they cannot starve the rest.
        settled = {keyword: 0 for keywords in by_note.values() for keyword in keywords}
        on_topic = dict(settled)
        for id_, status, body, comments in self.db.execute('SELECT note_id, status, body, comments FROM tasks'):
            done = status == 'running' or body == 'unavailable' or comments in ('complete_observed', 'sampled', 'skipped')
            for keyword in by_note.get(id_, ()):
                settled[keyword] += done
                on_topic[keyword] += status == 'running' or body == 'saved' and comments != 'skipped'
        candidates = {}
        for id_, body, reason in self.db.execute("SELECT t.note_id, t.body, coalesce(p.reason, '') FROM tasks t LEFT JOIN "
                                                 f"task_priority p USING(note_id) WHERE {eligible} AND coalesce(p.priority, 1) < 3"):
            for keyword in by_note.get(id_, ()):
                if keyword not in searching and on_topic[keyword] < self.config.sample_per_keyword:
                    candidates.setdefault(keyword, []).append(
                        (body != 'saved', reason.startswith(TOOL_REASON), hashlib.sha1(id_.encode()).hexdigest(), id_))
        if not candidates:
            return None
        keyword = min(candidates, key=lambda k: (settled[k], k))
        return dict(self.db.execute('SELECT * FROM tasks WHERE note_id=?', (min(candidates[keyword])[-1],)).fetchone())

    def defer_task(self, id_, kind, retry_after, reason):
        with self.db:
            self.db.execute('INSERT OR REPLACE INTO deferred_tasks VALUES(?,?,?,?)',
                            (id_, kind, retry_after, reason))

    def next_retry_time(self):
        return self.db.execute("""SELECT min(d.retry_after) FROM deferred_tasks d
            JOIN tasks t USING(note_id) WHERE t.status IN ('pending','interrupted','paused')
            AND d.retry_after>?""", (time.time(),)).fetchone()[0]

    def retry_incomplete(self):
        with self.db:
            self.db.execute("""UPDATE tasks SET status='pending' WHERE status='finished' AND body != 'unavailable' AND
            (body NOT IN ('saved','unavailable') OR comments IN ('partial','blocked','failed') OR media IN ('partial','failed','pending')
            OR comment_media IN ('partial','failed','pending'))""")
            if self.config.media:
                self.db.execute("UPDATE tasks SET status='pending' WHERE status='finished' AND body!='unavailable' AND (media='disabled' OR comment_media='not_requested')")

    def control(self, key, value=None):
        if value is not None:
            with self.db:
                self.db.execute("INSERT OR REPLACE INTO controls VALUES(?,?)", (key, str(value)))
        row = self.db.execute("SELECT value FROM controls WHERE key=?", (key,)).fetchone()
        return row[0] if row else None

    def verify(self):
        known, bad = set(), []
        for row in self.db.execute("SELECT * FROM events WHERE valid=1").fetchall():
            known.add(row["path"])
            try:
                body = gzip.decompress((self.root / row["path"]).read_bytes())
                if hashlib.sha256(body).hexdigest() != row["sha256"]:
                    raise ValueError("hash mismatch")
            except (OSError, ValueError, EOFError):
                bad.append(row["id"])
                with self.db:
                    self.db.execute("UPDATE events SET valid=0 WHERE id=?", (row["id"],))
                    self.db.execute("UPDATE tasks SET status='interrupted',body='partial',comments='partial',reason='archive_corrupt' WHERE note_id=?", (row["note_id"],))
                    if self.db.execute("SELECT 1 FROM sqlite_master WHERE name='workspace_jobs'").fetchone():
                        self.db.execute("UPDATE workspace_jobs SET state='partial',reason='archive_corrupt',updated=? WHERE note_id=? AND kind='comments' AND state='success'", (now(), row["note_id"]))
        from .media import verified_file
        for row in self.db.execute("SELECT * FROM media_files WHERE status='saved'").fetchall():
            if not verified_file(self.root, dict(row)):
                with self.db:
                    self.db.execute("UPDATE media_files SET status='missing',reason='file_verification_failed' WHERE note_id=? AND comment_id=? AND media_id=?", (row['note_id'], row['comment_id'], row['media_id']))
                    part = 'comment_media' if row['comment_id'] else 'media'
                    self.db.execute(f"UPDATE tasks SET {part}='partial',status='interrupted',reason='file_verification_failed' WHERE note_id=?", (row['note_id'],))
        orphans = [str(p.relative_to(self.root)) for p in (self.root / "raw").glob("*/*.gz") if str(p.relative_to(self.root)) not in known]
        result = {"checked": now(), "invalid_events": bad, "unindexed_files": orphans}
        atomic_write(self.root / "diagnostics" / "recovery.json", encode(result))
        return result

    def export(self, force=True):
        # Manifest is derived, not a second source of truth; recover by re-exporting.
        # SQLite and raw blobs are already durable. Batch derived-file rewrites
        # during collection; close()/explicit exports still flush immediately.
        # The full rewrite is ~16 MB on a large batch; every 5 minutes is enough.
        if not force and time.monotonic() - self.last_export < 300:
            return
        lines = []
        for r in self.db.execute("SELECT * FROM events ORDER BY id"):
            item = dict(r)
            item["metadata"] = json.loads(item["metadata"])
            lines.append(encode({"run": self.run["id"], **item}))
        atomic_write(self.root / "manifest.jsonl", b"\n".join(lines) + (b"\n" if lines else b""))
        atomic_write(self.root / "report.json", encode({"run": self.run["id"], "tasks": self.tasks(), "recovery": self.recovery,
                     "search": json.loads(self.control('search_progress') or '[]')}))
        self.last_export = time.monotonic()

    def close(self):
        try:
            self.export()
        finally:
            self.db.close()
            self.lock.close()
