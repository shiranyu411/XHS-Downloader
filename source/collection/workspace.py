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
        # Imported archives retain existing attempts/results and are never auto-scheduled.

    def enqueue(self, urls, source):
        result = super().enqueue(urls, source)
        with self.db:
            self.db.execute(
                "INSERT OR IGNORE INTO workspace_items(note_id) SELECT note_id FROM tasks"
            )
        return result

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

    def pending(self, kind):
        return [
            dict(r)
            for r in self.db.execute(
                """SELECT t.*, j.task_id FROM workspace_jobs j
            JOIN tasks t USING(note_id) JOIN workspace_items w USING(note_id)
            WHERE j.kind=? AND j.state IN ('pending','interrupted','paused') AND w.hidden=0 ORDER BY j.rowid""",
                (kind,),
            )
        ]

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
            (SELECT state FROM workspace_jobs j WHERE j.note_id=t.note_id AND kind='download') AS download_job
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
        for row in rows:
            row["observations"] = observations.get(row["note_id"], 0)
            row["sources"] = sorted(sources.get(row["note_id"], set()))
        return rows
