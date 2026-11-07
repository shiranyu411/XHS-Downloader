"""Rebuildable passive observations, never a claim of collection completeness."""
import gzip
import hashlib
import json
import re

from .protocol import Request, restricted
from .store import encode

NOTE_ID = re.compile(r"^[a-fA-F0-9]{24}$")


def note_items(value):
    if isinstance(value, dict):
        id_ = value.get("id") or value.get("note_id")
        token = value.get("xsec_token")
        if isinstance(id_, str) and NOTE_ID.fullmatch(id_) and isinstance(token, str) and token:
            yield id_.lower(), token
        for child in value.values():
            yield from note_items(child)
    elif isinstance(value, list):
        for child in value:
            yield from note_items(child)


def comment_count(value):
    if isinstance(value, dict):
        info = value.get("interact_info")
        if isinstance(info, dict):
            raw = info.get("comment_count")
            if type(raw) is int and raw >= 0:
                return raw
            if isinstance(raw, str) and raw.isdecimal():
                return int(raw)
        children = value.values()
    elif isinstance(value, list):
        children = value
    else:
        return None
    for child in children:
        found = comment_count(child)
        if found is not None:
            return found
    return None


class Coverage:
    def __init__(self):
        self.body = False
        self.top = {}
        self.replies = {}
        self.unassigned = set()
        self.problems = set()
        self.advertised = {}
        self.advertised_total = None
        self.end_seen = False
        self.blocked = False

    def _reply(self, comment, parent):
        id_ = str(comment["id"])
        claimed = comment.get("root_comment_id")
        if not parent or (claimed and str(claimed) != parent):
            self.unassigned.add(id_)
            self.problems.add("reply_parent_unconfirmed" if not parent else "reply_parent_mismatch")
            return
        if any(id_ in ids and root != parent for root, ids in self.replies.items()):
            self.problems.add("reply_parent_changed")
            self.unassigned.add(id_)
            for ids in self.replies.values():
                ids.discard(id_)
            return
        if id_ not in self.unassigned:
            self.replies.setdefault(parent, set()).add(id_)

    def observe(self, req, payload):
        # Retain the small public helper API; recording always passes full Request.
        req = Request(req, None) if isinstance(req, str) else req
        kind = req.kind
        if not isinstance(payload, dict) or payload.get("success") is not True or payload.get("code", 0) != 0:
            return 0
        if kind == "detail":
            self.body = True
            total = comment_count(payload)
            if total is not None:
                self.advertised_total = total
            return 0
        data = payload.get("data")
        if kind not in ("comments", "replies") or not isinstance(data, dict) or not isinstance(data.get("comments"), list):
            return 0
        if req.note and data.get("note_id", req.note) != req.note:
            self.problems.add("note_ownership_mismatch")
            return 0
        before = self.count()
        for comment in data["comments"]:
            if not isinstance(comment, dict) or not comment.get("id"):
                continue
            if req.note and comment.get("note_id", req.note) != req.note:
                self.problems.add("note_ownership_mismatch")
                continue
            id_ = str(comment["id"])
            if kind == "comments":
                self.top[id_] = True
                count = comment.get("sub_comment_count")
                if isinstance(count, str) and count.isdecimal():
                    count = int(count)
                if type(count) is int and count >= 0:
                    self.advertised[id_] = count
                self.replies.setdefault(id_, set())
                for child in comment.get("sub_comments") or []:
                    if isinstance(child, dict) and child.get("id"):
                        self._reply(child, id_)
            else:
                parent = req.parent or data.get("root_comment_id") or comment.get("root_comment_id")
                if req.parent and data.get("root_comment_id", req.parent) != req.parent:
                    self.problems.add("reply_parent_mismatch")
                    self.unassigned.add(id_)
                else:
                    self._reply(comment, str(parent) if parent else None)
        if kind == "comments" and data.get("has_more") is False:
            self.end_seen = True
        return self.count() - before

    def count(self):
        return len(set(self.top) | self.unassigned | set().union(*self.replies.values()))

    def record(self):
        advertised = sum(self.advertised.values())
        reply_ids = set().union(*self.replies.values())
        return {
            "body_saved": self.body,
            "top_level_comments": len(self.top),
            "replies": len(reply_ids),
            "comments_total": self.count(),
            "unassigned_replies": len(self.unassigned),
            "problems": sorted(self.problems),
            "advertised_note_comments": self.advertised_total,
            "reply_coverage": {"advertised": advertised, "captured": len(reply_ids),
                               "ratio": round(len(reply_ids) / advertised, 4) if advertised else None},
            "threads_partially_seen": sorted(k for k, v in self.advertised.items() if len(self.replies.get(k, ())) < v),
            "primary_end_seen": self.end_seen,
            "access_restricted": self.blocked,
        }


class ObservationIndex:
    def __init__(self):
        self.coverage = {}
        self.discovered = set()
        self.events = 0
        self.gaps = 0
        self.access_restricted = False

    def observe(self, req, body, status):
        self.events += 1
        try:
            payload = json.loads(body) if req.kind != "document" else None
        except (ValueError, TypeError):
            payload = None
        if restricted(payload, status):
            self.access_restricted = True
            if req.note:
                self.coverage.setdefault(req.note, Coverage()).blocked = True
            return []
        successful = status == 200 and isinstance(payload, dict) and payload.get("success") is True and payload.get("code", 0) == 0
        found = list(dict(note_items(payload)).items()) if successful and req.kind in ("search", "detail") else []
        new = [(i, t) for i, t in found if i not in self.discovered]
        self.discovered.update(i for i, _ in found)
        if req.note:
            coverage = self.coverage.setdefault(req.note, Coverage())
            if req.kind == "document" and status == 200:
                coverage.body = True
            elif successful:
                coverage.observe(req, payload)
        return new

    def summary(self):
        return {"events": self.events, "discovered": len(self.discovered),
                "notes_with_comments": sum(bool(c.count()) for c in self.coverage.values()),
                "comments_total": sum(c.count() for c in self.coverage.values()),
                "capture_gaps": self.gaps, "access_restricted": self.access_restricted}

    def report(self):
        return {"mode": "passive", "scope": "batch_cumulative", "schema_version": 2,
                **self.summary(), "notes": {k: v.record() for k, v in sorted(self.coverage.items())}}

    @classmethod
    def rebuild(cls, db, root):
        index = cls()
        for row in db.execute("SELECT * FROM events WHERE kind IN ('network_json','document_html','passive_gap') ORDER BY id"):
            meta = json.loads(row["metadata"])
            if meta.get("mode") != "passive":
                continue
            if row["kind"] == "passive_gap" or not row["valid"]:
                index.gaps += 1
                if restricted(None, meta.get("status")):
                    index.access_restricted = True
                    note = meta.get("note_id", meta.get("note"))
                    if note:
                        index.coverage.setdefault(note, Coverage()).blocked = True
                continue
            try:
                body = gzip.decompress((root / row["path"]).read_bytes())
                if hashlib.sha256(body).hexdigest() != row["sha256"]:
                    raise ValueError("hash mismatch")
            except (OSError, ValueError, EOFError):
                index.gaps += 1
                continue
            req = Request(meta["kind"], meta.get("note_id", meta.get("note")),
                          meta.get("root_comment_id", meta.get("parent")), meta.get("cursor") or "")
            index.observe(req, body, meta.get("status"))
        return index

    def persist(self, store):
        # A derived UI projection, never used by Ledger or by resume_seed().
        store.db.execute("CREATE TABLE IF NOT EXISTS passive_notes(note_id TEXT PRIMARY KEY,data TEXT NOT NULL)")
        with store.db:
            store.db.execute("DELETE FROM passive_notes")
            store.db.executemany("INSERT INTO passive_notes VALUES(?,?)",
                                 [(n, encode(c.record()).decode()) for n, c in self.coverage.items()])
        store.control("passive_summary", encode(self.summary()).decode())
