"""Conservative, attempt-local completeness from observed successful response chains."""
from dataclasses import dataclass, field
import json
from urllib.parse import parse_qs, urlsplit


def restricted(payload, status=200):
    if status in (401, 403, 429, 461, 471):
        return True
    if not isinstance(payload, dict):
        return False
    message = str(payload.get("msg", payload.get("message", ""))).lower()
    return payload.get("code") in (-100, -101, -102, -10000, 300012, 300013) or any(
        word in message for word in ("登录", "验证", "频繁", "访问限制", "captcha", "rate limit")
    )


@dataclass(frozen=True)
class Request:
    kind: str
    note: str | None
    parent: str | None = None
    cursor: str = ""
    path: str = ""
    scope: tuple = ()


def classify(url, post_data=None, resource_type=None):
    u = urlsplit(url)
    host = u.hostname or ""
    if not any(host == d or host.endswith("." + d) for d in ("xiaohongshu.com", "rednote.com")):
        return None
    q = {k: v[0] for k, v in parse_qs(u.query, keep_blank_values=True).items()}
    if post_data:
        try:
            data = json.loads(post_data)
            if isinstance(data, dict):
                q.update(data)
        except (ValueError, TypeError):
            pass
    note = q.get("note_id") or q.get("source_note_id")
    parent = q.get("root_comment_id")
    if u.path.endswith("/comment/sub/page"):
        kind = "replies"
    elif u.path.endswith("/comment/page"):
        kind = "comments"
    elif u.path.endswith("/feed"):
        kind = "detail"
    elif "/search/" in u.path:
        kind = "search"
    elif "/comment/" in u.path:
        kind = "unknown_comment"
    elif resource_type == "Document" and any(p in u.path for p in ("/explore/", "/discovery/item/")):
        kind = "document"
        note = u.path.rstrip("/").rsplit("/", 1)[-1]
    else:
        return None
    return Request(kind, str(note) if note else None, str(parent) if parent else None,
                   str(q.get("cursor") or ""), u.path,
                   tuple((k, str(q.get(k, ""))) for k in ("sort", "sort_type", "order", "top_comment_id")))


@dataclass
class Chain:
    # request cursor -> (response cursor, has_more). End only counts if reachable.
    edges: dict = field(default_factory=dict)
    ids: set = field(default_factory=set)
    problems: list = field(default_factory=list)
    page_ids: dict = field(default_factory=dict)

    def observe(self, cursor, data, ids):
        ids = set(ids)
        if cursor in self.page_ids and self.page_ids[cursor] != ids:
            self.problems.append("page_contents_changed")
        self.page_ids[cursor] = ids
        self.ids.update(ids)
        more = data.get("has_more")
        next_ = data.get("cursor")
        if type(more) is not bool or (more and (not isinstance(next_, str) or not next_ or next_ == cursor)):
            self.problems.append("missing_or_stalled_pagination")
            return
        edge = (next_, more)
        if cursor in self.edges and self.edges[cursor] != edge:
            self.problems.append("pagination_changed")
        self.edges[cursor] = edge
        current, seen = "", set()
        while current in self.edges:
            if current in seen:
                self.problems.append("cursor_cycle")
                break
            seen.add(current)
            current, more = self.edges[current]
            if more is False:
                break

    def next_cursor(self):
        if self.problems:
            return None
        cursor, seen = "", set()
        while cursor in self.edges:
            if cursor in seen:
                return None
            seen.add(cursor)
            cursor, more = self.edges[cursor]
            if more is False:
                return None
        return cursor

    def complete(self):
        cursor, seen = "", set()
        while cursor in self.edges and cursor not in seen:
            seen.add(cursor)
            cursor, more = self.edges[cursor]
            if more is False:
                return not self.problems
        return False

    def record(self):
        return {"pages": self.edges, "ids": sorted(self.ids), "problems": self.problems, "complete_observed": self.complete()}


class Ledger:
    def __init__(self, note):
        self.note = note
        self.primary = Chain()
        self.replies = {}
        self.gaps = []
        self.blocked = False
        self.version = 0
        self.comments = {}
        self.scopes = {}
        self.reply_counts = {}

    def index(self, comment, parent=None):
        # Full, unknown fields remain in raw responses. This is a relationship index.
        id_ = str(comment["id"])
        user = comment.get("user_info")
        entry = {"user_id": user.get("user_id") if isinstance(user, dict) else None,
                 "root_comment_id": parent, "target_comment": comment.get("target_comment")}
        previous = self.comments.get(id_)
        if previous is not None and previous != entry:
            self.gap("comment_relationship_changed")
        self.comments[id_] = entry

    def next_request(self):
        if self.blocked or self.gaps or self.primary.problems or any(c.problems for c in self.replies.values()):
            return None
        # Finish each discovered thread without a default reply-count limit.
        for parent, chain in self.replies.items():
            cursor = chain.next_cursor()
            if cursor is not None:
                return Request("replies", self.note, parent, cursor)
        cursor = self.primary.next_cursor()
        return Request("comments", self.note, cursor=cursor) if cursor is not None else None

    def gap(self, reason):
        self.gaps.append(reason)
        self.version += 1

    def observe(self, req: Request, payload, status=200):
        self.version += 1
        if restricted(payload, status):
            self.blocked = True
        if not isinstance(payload, dict):
            self.gap("invalid_json_structure")
            return
        if status != 200 or payload.get("success") is not True or payload.get("code", 0) != 0:
            self.gap("business_or_http_error")
            return
        if req.note != self.note or (req.kind == "replies" and not req.parent):
            self.gap("unconfirmed_request_ownership")
            return
        scope_key = (req.kind, req.parent)
        previous_scope = self.scopes.setdefault(scope_key, req.scope)
        if previous_scope != req.scope:
            self.gap("comment_query_scope_changed")
            return
        data = payload.get("data")
        if not isinstance(data, dict) or not isinstance(data.get("comments"), list):
            self.gap("unknown_comments_structure")
            return
        if data.get("note_id", self.note) != self.note or (req.kind == "replies" and data.get("root_comment_id", req.parent) != req.parent):
            self.gap("response_ownership_mismatch")
            return
        ids = []
        for c in data["comments"]:
            if not isinstance(c, dict) or not c.get("id") or c.get("note_id", self.note) != self.note:
                self.gap("comment_ownership_or_id_missing")
                continue
            id_ = str(c["id"])
            if req.kind == "replies":
                if c.get("root_comment_id", req.parent) != req.parent:
                    self.gap("reply_parent_mismatch")
                    continue
            ids.append(id_)
            self.index(c, req.parent if req.kind == "replies" else None)
            if req.kind == "comments":
                chain = self.replies.setdefault(id_, Chain())
                count = c.get("sub_comment_count")
                if isinstance(count, str) and count.isdecimal():
                    count = int(count)
                if type(count) is int and count >= 0:
                    if id_ in self.reply_counts and self.reply_counts[id_] != count:
                        self.gap("reply_count_changed")
                    self.reply_counts[id_] = count
                children = c.get("sub_comments")
                if not isinstance(children, list):
                    self.gap("missing_embedded_replies")
                    continue
                child_ids = []
                for child in children:
                    if not isinstance(child, dict) or not child.get("id") or child.get("note_id", self.note) != self.note or child.get("root_comment_id", id_) != id_:
                        self.gap("embedded_reply_mismatch")
                    else:
                        child_ids.append(str(child["id"]))
                        self.index(child, id_)
                chain.observe("", {"has_more": c.get("sub_comment_has_more"), "cursor": c.get("sub_comment_cursor")}, child_ids)
        chain = self.primary if req.kind == "comments" else self.replies.setdefault(req.parent, Chain())
        chain.observe(req.cursor, data, ids)
        for parent, expected in self.reply_counts.items():
            replies = self.replies[parent]
            if replies.complete() and len(replies.ids) != expected:
                self.gap("reply_count_mismatch")

    def complete(self):
        return not self.blocked and not self.gaps and self.primary.complete() and all(c.complete() for c in self.replies.values())

    def record(self):
        return {"note_id": self.note, "primary": self.primary.record(), "replies": {k: v.record() for k, v in self.replies.items()},
                "comments": self.comments, "advertised_reply_counts": self.reply_counts,
                "unresolved_targets": sorted({str(target["id"]) for c in self.comments.values()
                    if isinstance(target := c.get("target_comment"), dict) and target.get("id")
                    and str(target["id"]) not in self.comments}),
                "gaps": self.gaps, "blocked": self.blocked, "complete_observed": self.complete()}
