"""Single-writer archive. Blob rename precedes the SQLite event/progress transaction."""
import gzip
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import tempfile
from datetime import datetime, timezone
from uuid import uuid4

from .config import Config, note_id


def now():
    return datetime.now(timezone.utc).isoformat()


def encode(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode()


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


class Store:
    def __init__(self, root: Path, config: Config | None = None):
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
        CREATE TABLE IF NOT EXISTS events(
          id INTEGER PRIMARY KEY, attempt TEXT, note_id TEXT, kind TEXT,
          seen TEXT, path TEXT, sha256 TEXT, metadata TEXT, valid INTEGER DEFAULT 1);
        CREATE TABLE IF NOT EXISTS progress(
          attempt TEXT PRIMARY KEY, data TEXT);
        CREATE TABLE IF NOT EXISTS controls(key TEXT PRIMARY KEY, value TEXT);
        """)
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
                # Keep the most recently observed real URL; never invent a token.
                self.db.execute("UPDATE tasks SET url=? WHERE note_id=?", (url, id_))
        return {"observations": len(items), "tasks": self.db.execute("SELECT count(*) FROM tasks").fetchone()[0], "committed": self.db.total_changes > before}

    def begin(self, id_):
        attempt = uuid4().hex
        with self.db:
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
            if hashlib.sha256(gzip.decompress(path.read_bytes())).hexdigest() != digest:
                raise OSError(f"已存文件校验失败: {relative}")
        with self.db:
            cur = self.db.execute(
                "INSERT INTO events(attempt,note_id,kind,seen,path,sha256,metadata) VALUES(?,?,?,?,?,?,?)",
                (attempt, id_, kind, now(), relative, digest, encode(metadata or {}).decode()),
            )
            if progress is not None:
                self.db.execute("INSERT OR REPLACE INTO progress VALUES(?,?)", (attempt, encode(progress).decode()))
        return cur.lastrowid

    def finish(self, attempt, result, status="finished"):
        with self.db:
            row = self.db.execute("SELECT note_id FROM attempts WHERE id=?", (attempt,)).fetchone()
            self.db.execute("UPDATE attempts SET ended=?,status=?,result=? WHERE id=?", (now(), status, encode(result).decode(), attempt))
            self.db.execute(
                "UPDATE tasks SET status=?,body=?,comments=?,media=?,reason=?,updated=? WHERE note_id=?",
                (status, result["body"], result["comments"], result["media"], result["reason"], now(), row[0]),
            )
        self.export()

    def tasks(self):
        return [dict(r) for r in self.db.execute("SELECT * FROM tasks ORDER BY rowid")]

    def next_task(self):
        row = self.db.execute("SELECT * FROM tasks WHERE status IN ('pending','interrupted','paused') ORDER BY rowid LIMIT 1").fetchone()
        return dict(row) if row else None

    def retry_incomplete(self):
        with self.db:
            self.db.execute("""UPDATE tasks SET status='pending' WHERE status='finished' AND body != 'unavailable' AND
            (body NOT IN ('saved','unavailable') OR comments IN ('partial','blocked','failed') OR media IN ('partial','failed'))""")

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
        orphans = [str(p.relative_to(self.root)) for p in (self.root / "raw").glob("*/*.gz") if str(p.relative_to(self.root)) not in known]
        result = {"checked": now(), "invalid_events": bad, "unindexed_files": orphans}
        atomic_write(self.root / "diagnostics" / "recovery.json", encode(result))
        return result

    def export(self):
        # Manifest is derived, not a second source of truth; recover by re-exporting.
        lines = []
        for r in self.db.execute("SELECT * FROM events ORDER BY id"):
            item = dict(r)
            item["metadata"] = json.loads(item["metadata"])
            lines.append(encode({"run": self.run["id"], **item}))
        atomic_write(self.root / "manifest.jsonl", b"\n".join(lines) + (b"\n" if lines else b""))
        atomic_write(self.root / "report.json", encode({"run": self.run["id"], "tasks": self.tasks(), "recovery": self.recovery}))

    def close(self):
        try:
            self.export()
        finally:
            self.db.close()
            self.lock.close()
