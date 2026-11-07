"""Additive batch workbench tables; a single Store remains the archive writer."""

import gzip
import json
from urllib.parse import urlsplit, parse_qs
from uuid import uuid4

from .store import Store, now


class Workspace(Store):
    def __init__(self, root, config=None):
        super().__init__(root, config)
        self.db.executescript("""
        CREATE TABLE IF NOT EXISTS workspace_items(
          note_id TEXT PRIMARY KEY, title TEXT NOT NULL DEFAULT '', hidden INTEGER NOT NULL DEFAULT 0);
        CREATE TABLE IF NOT EXISTS workspace_jobs(
          note_id TEXT NOT NULL, kind TEXT NOT NULL, state TEXT NOT NULL,
          task_id TEXT NOT NULL, updated TEXT NOT NULL, reason TEXT,
          PRIMARY KEY(note_id,kind));
        """)
        with self.db:
            self.db.execute(
                "INSERT OR IGNORE INTO workspace_items(note_id) SELECT note_id FROM tasks"
            )
            self.db.execute(
                "UPDATE workspace_jobs SET state='interrupted',reason='process_restart' WHERE state='running'"
            )
            # Reconcile batches already invalidated by an older version of Store.verify().
            self.db.execute("""UPDATE workspace_jobs SET state='partial',reason='archive_corrupt'
                WHERE kind='comments' AND state='success' AND note_id IN
                (SELECT note_id FROM tasks WHERE reason='archive_corrupt' AND comments!='complete_observed')""")
        self.db.execute("CREATE TABLE IF NOT EXISTS workspace_dismissed(task_id TEXT PRIMARY KEY)")
        # Derived observations are rebuilt on open, not on every UI poll.
        from .observations import ObservationIndex
        ObservationIndex.rebuild(self.db, self.root).persist(self)
        # Imported archives retain existing attempts/results and are never auto-scheduled.

    def enqueue(self, urls, source):
        from .config import note_id
        urls = list(urls)
        ids = list(dict.fromkeys(note_id(url) for url in urls))
        existing = {r[0] for r in self.db.execute("SELECT note_id FROM tasks")}
        result = super().enqueue(urls, source)
        with self.db:
            self.db.execute(
                "INSERT OR IGNORE INTO workspace_items(note_id) SELECT note_id FROM tasks"
            )
        return {**result, "note_ids": ids, "new_ids": [i for i in ids if i not in existing],
                "existing_ids": [i for i in ids if i in existing]}

    def operation_rows(self):
        """Read-only facts used by both scope preview and its confirmation."""
        return {r["note_id"]: dict(r) for r in self.db.execute("""SELECT t.*, w.hidden,
            c.state AS comments_state,c.task_id AS comments_task_id,c.updated AS comments_updated,
            d.state AS download_state,d.task_id AS download_task_id,d.updated AS download_updated
            FROM tasks t JOIN workspace_items w USING(note_id)
            LEFT JOIN workspace_jobs c ON c.note_id=t.note_id AND c.kind='comments'
            LEFT JOIN workspace_jobs d ON d.note_id=t.note_id AND d.kind='download'""")}

    def hide_preview(self, ids):
        ids = self.validate_ids(ids)
        chosen = set(ids)
        jobs = [dict(r) for r in self.db.execute("""SELECT note_id,kind,state,task_id
            FROM workspace_jobs WHERE state IN ('pending','paused','interrupted','running')""")
                if r["note_id"] in chosen]
        from .workflow import signature
        return {"note_ids": ids, "count": len(ids), "cancelled_jobs": len(jobs),
                "token": signature(["hide", str(self.root), ids, jobs])}

    def validate_ids(self, ids):
        if (
            not isinstance(ids, list)
            or not ids
            or len(ids) > 5000
            or any(not isinstance(i, str) for i in ids)
        ):
            raise ValueError("请选择 1–5000 篇作品。")
        ids = list(dict.fromkeys(ids))
        visible = {
            r[0]
            for r in self.db.execute(
                "SELECT note_id FROM workspace_items WHERE hidden=0"
            )
        }
        if not set(ids) <= visible:
            raise ValueError("选择包含已隐藏或不属于当前批次的作品，请刷新列表。")
        return ids

    def submit(self, ids, kind, retry=False):
        ids = self.validate_ids(ids)
        if kind not in {"comments", "download"}:
            raise ValueError("非法任务类型")
        added = []
        with self.db:
            for id_ in ids:
                job = self.db.execute(
                    "SELECT state FROM workspace_jobs WHERE note_id=? AND kind=?",
                    (id_, kind),
                ).fetchone()
                if job and (job[0] in {"pending", "running", "success"} or not retry):
                    continue
                task = self.db.execute(
                    "SELECT * FROM tasks WHERE note_id=?", (id_,)
                ).fetchone()
                if (kind == "comments" and task["comments"] == "complete_observed") or (
                    kind == "download" and task["media"] == "success"
                ):
                    continue
                task_id = uuid4().hex
                self.db.execute(
                    "INSERT OR REPLACE INTO workspace_jobs VALUES(?,?,?,?,?,NULL)",
                    (id_, kind, "pending", task_id, now()),
                )
                added.append(task_id)
        return added

    def dismiss_downloads(self, task_ids):
        with self.db:
            self.db.executemany("INSERT OR IGNORE INTO workspace_dismissed VALUES(?)", [(i,) for i in task_ids])

    def pending(self, kind, task_ids=None):
        rows = [
            dict(r)
            for r in self.db.execute(
                """SELECT t.*, j.task_id FROM workspace_jobs j
            JOIN tasks t USING(note_id) JOIN workspace_items w USING(note_id)
            WHERE j.kind=? AND j.state IN ('pending','interrupted','paused') AND w.hidden=0 ORDER BY j.rowid""",
                (kind,),
            )
        ]
        return rows if task_ids is None else [r for r in rows if r["task_id"] in task_ids]

    def job_state(self, id_, kind, state, reason=None):
        with self.db:
            self.db.execute(
                "UPDATE workspace_jobs SET state=?,reason=?,updated=? WHERE note_id=? AND kind=?",
                (state, reason, now(), id_, kind),
            )

    def hide(self, ids):
        ids = self.validate_ids(ids)
        with self.db:
            for id_ in ids:
                if self.db.execute(
                    "SELECT 1 FROM workspace_jobs WHERE note_id=? AND state='running'",
                    (id_,),
                ).fetchone():
                    raise ValueError("请先安全暂停正在处理的作品。")
            self.db.executemany(
                "UPDATE workspace_items SET hidden=1 WHERE note_id=?",
                [(i,) for i in ids],
            )
            self.db.executemany(
                "UPDATE workspace_jobs SET state='cancelled' WHERE note_id=? AND state IN ('pending','paused','interrupted')",
                [(i,) for i in ids],
            )

    def restore_hidden(self):
        with self.db:
            count = self.db.execute(
                "UPDATE workspace_items SET hidden=0 WHERE hidden=1"
            ).rowcount
        return count  # Does not re-enqueue cancelled work or change archived results.

    def links_for(self, ids):
        ids = self.validate_ids(ids)
        return [
            self.db.execute("SELECT url FROM tasks WHERE note_id=?", (i,)).fetchone()[0]
            for i in ids
        ]

    def note_object(self, id_):
        row = self.db.execute(
            """SELECT path,sha256 FROM events WHERE note_id=? AND kind='note_object'
            AND valid=1 ORDER BY id DESC LIMIT 1""",
            (id_,),
        ).fetchone()
        if not row:
            return None
        import hashlib

        body = gzip.decompress((self.root / row["path"]).read_bytes())
        if hashlib.sha256(body).hexdigest() != row["sha256"]:
            raise OSError("正文存档校验失败")
        note = json.loads(body)
        if note.get("noteId") != id_:
            raise ValueError("正文归属不匹配")
        return note

    def finish(self, attempt, result, status="finished"):
        id_ = self.db.execute(
            "SELECT note_id FROM attempts WHERE id=?", (attempt,)
        ).fetchone()[0]
        previous_media = self.db.execute(
            "SELECT media FROM tasks WHERE note_id=?", (id_,)
        ).fetchone()[0]
        super().finish(attempt, result, status)
        if result["media"] == "disabled" and previous_media != "disabled":
            with self.db:
                self.db.execute(
                    "UPDATE tasks SET media=? WHERE note_id=?", (previous_media, id_)
                )
            self.export()
        note = self.note_object(id_)
        if note:
            with self.db:
                self.db.execute(
                    "UPDATE workspace_items SET title=? WHERE note_id=?",
                    (str(note.get("title", ""))[:500], id_),
                )

    def overview(self):
        rows = [
            dict(r)
            for r in self.db.execute("""SELECT t.note_id,t.status,t.body,t.comments,t.media,t.reason,t.updated,
            w.title, w.hidden,
            (SELECT state FROM workspace_jobs j WHERE j.note_id=t.note_id AND kind='comments') AS collection_job,
            (SELECT state FROM workspace_jobs j WHERE j.note_id=t.note_id AND kind='download') AS download_job,
            (SELECT reason FROM workspace_jobs j WHERE j.note_id=t.note_id AND kind='download') AS media_reason
            FROM tasks t JOIN workspace_items w USING(note_id) WHERE w.hidden=0 ORDER BY t.rowid""")
        ]
        sources, observations = {}, {}
        for row in self.db.execute("SELECT note_id,metadata FROM sources"):
            id_ = row["note_id"]
            observations[id_] = observations.get(id_, 0) + 1
            try:
                meta = json.loads(row["metadata"])
                url = urlsplit(meta.get("url", ""))
                keyword = parse_qs(url.query).get("keyword", [""])[0]
                name = keyword or (
                    "作者主页"
                    if url.path.startswith("/user/profile/")
                    else meta.get("mode", "其他来源")
                )
                sources.setdefault(row["note_id"], set()).add(str(name)[:100])
            except (ValueError, TypeError, AttributeError):
                continue
        observed = {r[0]: json.loads(r[1]) for r in self.db.execute("SELECT note_id,data FROM passive_notes")}
        for row in rows:
            row["observed"] = observed.get(row["note_id"])
            row["observations"] = observations.get(row["note_id"], 0)
            row["sources"] = sorted(sources.get(row["note_id"], set()))
        return rows
