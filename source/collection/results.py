"""Offline, read-only content projections over the existing archive.

No Store is opened, no HTML is executed, no remote resource is fetched, and no
projection writes back completeness. Each response remains a dated observation.
"""
from contextlib import closing
import csv
from datetime import datetime
import gzip
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import sqlite3
import tempfile
from urllib.parse import urlsplit
from uuid import uuid4

from .protocol import restricted
from .replay import chain_key, rebuild
from .store import atomic_write, encode
from ..module.note_info import NoteGenerator

SOURCES = {"current", "active", "passive", "history", "all"}
BODY_FIELDS = ["note_id", "title", "text", "author_id", "author_name", "published_at", "updated_at", "type", "tags",
               "like_count", "collected_count", "comment_count", "share_count"]
COMMENT_FIELDS = ["note_id", "comment_id", "kind", "root_comment_id", "target_comment_id", "target_user_id", "target_user_name",
                  "text", "user_id", "user_name", "created_at", "like_count", "relationship_issue", "pictures", "platform_emojis", "image_paths"]
EVIDENCE_FIELDS = ["source", "observed_at", "event_id", "attempt", "session_id", "missing"]
SUMMARY_FIELDS = ["note_id", "title", "body", "comments", "media", "reason", "body_observations", "comment_observations", "warnings"]


def string(value):
    return str(value) if isinstance(value, (str, int, float)) and not isinstance(value, bool) else ""


def obj(value):
    return value if isinstance(value, dict) else {}


def value(mapping, *keys):
    for key in keys:
        if key in mapping and mapping[key] is not None:
            return mapping[key]
    return None


def safe_cell(value_, field=""):
    text = json.dumps(value_, ensure_ascii=False) if isinstance(value_, (list, dict)) else string(value_)
    # Quoting CSV is not enough for spreadsheets, especially numeric identifiers.
    if text and (field.endswith("_id") or field in {"attempt", "session_id"} or text.lstrip(" \t\r\n").startswith(("=", "+", "-", "@")) or text.startswith(("\t", "\r", "\n"))):
        return "'" + text
    return text


def open_snapshot(root):
    db = sqlite3.connect((root / "state.sqlite").as_uri() + "?mode=ro", uri=True)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA query_only=ON")
    db.execute("BEGIN")
    return db


def blob(root, row):
    if not row["valid"]:
        raise ValueError("invalidated_event")
    path = (root / row["path"]).resolve()
    raw_root = (root / "raw").resolve()
    if not raw_root.is_relative_to(root) or not path.is_relative_to(raw_root):
        raise ValueError("invalid_blob_path")
    with gzip.open(path, "rb") as stream:
        body = stream.read(64 * 1024 * 1024 + 1)
    if len(body) > 64 * 1024 * 1024:
        raise ValueError("blob_too_large_for_content_view")
    if hashlib.sha256(body).hexdigest() != row["sha256"]:
        raise ValueError("hash_mismatch")
    return body


def body_row(note, id_, evidence):
    user = obj(note.get("user"))
    info = obj(value(note, "interactInfo", "interact_info"))
    tags = value(note, "tagList", "tag_list")
    result = {
        "note_id": id_, "title": string(note.get("title")), "text": string(value(note, "desc", "description")),
        "author_id": string(value(user, "userId", "user_id")),
        "author_name": string(value(user, "nickname", "nickName", "nick_name")),
        "published_at": string(note.get("time")), "updated_at": string(value(note, "lastUpdateTime", "last_update_time")),
        "type": string(note.get("type")),
        "tags": " / ".join(string(t.get("name")) for t in tags if isinstance(t, dict)) if isinstance(tags, list) else "",
        **{k: string(value(info, k, camel)) for k, camel in [("like_count", "likedCount"), ("collected_count", "collectedCount"),
                                                           ("comment_count", "commentCount"), ("share_count", "shareCount")]},
        **evidence,
    }
    if not result["like_count"]:
        result["like_count"] = string(info.get("liked_count"))
    result["missing"] = ",".join(k for k in ("title", "text", "author_id", "published_at") if not result[k])
    return result


def comment_row(comment, id_, kind, parent, evidence, issue=""):
    user = obj(comment.get("user_info"))
    target = obj(comment.get("target_comment"))
    claimed = string(comment.get("root_comment_id"))
    if kind == "reply" and (not parent or claimed and claimed != parent):
        issue = issue or ("reply_parent_mismatch" if parent else "reply_parent_unconfirmed")
        parent = None
    result = {"note_id": id_, "comment_id": string(comment.get("id")), "kind": kind,
              "root_comment_id": parent or "", "target_comment_id": string(target.get("id")),
              "target_user_id": string(obj(target.get("user_info")).get("user_id")),
              "target_user_name": string(value(obj(target.get("user_info")), "nickname", "nick_name")),
              "text": string(comment.get("content")), "user_id": string(user.get("user_id")),
              "user_name": string(value(user, "nickname", "nick_name")),
              "created_at": string(comment.get("create_time")), "like_count": string(comment.get("like_count")),
              "relationship_issue": issue, **evidence}
    from .media import picture_rows
    result["pictures"] = picture_rows(comment)
    result["platform_emojis"] = re.findall(r"\[[^\[\]\r\n]+R\]", result["text"])
    result["image_paths"] = []
    result["missing"] = ",".join(k for k in ("text", "user_id", "created_at") if not result[k] and not (k == "text" and result["pictures"]))
    return result


def task_row(db, id_):
    if db.execute("SELECT 1 FROM sqlite_master WHERE name='workspace_items'").fetchone():
        return db.execute("SELECT t.*,w.title FROM tasks t JOIN workspace_items w USING(note_id) WHERE t.note_id=? AND w.hidden=0", (id_,)).fetchone()
    return db.execute("SELECT *,'' AS title FROM tasks WHERE note_id=?", (id_,)).fetchone()


def projection(db, root, id_):
    task = task_row(db, id_)
    if task is None:
        raise ValueError("作品已隐藏或不属于当前资料夹，请刷新。")
    attempts = [dict(r) for r in db.execute("SELECT id,started,ended,status,result FROM attempts WHERE note_id=? ORDER BY rowid", (id_,))]
    raw_chain = db.execute("SELECT value FROM controls WHERE key=?", (chain_key(id_),)).fetchone()
    warnings = []
    try:
        chain = json.loads(raw_chain[0]) if raw_chain else [a["id"] for a in attempts[-1:]]
        if not isinstance(chain, list) or any(not isinstance(a, str) for a in chain):
            raise ValueError("invalid chain")
        if not set(chain) <= {a["id"] for a in attempts}:
            raise ValueError("foreign attempt")
    except ValueError:
        chain = []
        warnings.append("invalid_attempt_chain")
    active_attempts = set(chain)
    bodies, comments, media = [], [], []
    ledger, _ = rebuild(db, root, id_, chain)
    html_seen = False
    for row in db.execute("SELECT * FROM events WHERE note_id=? ORDER BY id", (id_,)):
        if row["kind"] not in {"note_object", "network_json", "document_html", "media_result"}:
            continue
        active = row["attempt"] in active_attempts
        try:
            meta = obj(json.loads(row["metadata"]))
            # Background search suggestions are not this note's pagination evidence.
            if row["kind"] == "network_json" and meta.get("kind") not in {"detail", "comments", "replies"}:
                continue
            origin = "passive" if meta.get("mode") == "passive" else "active" if active else "history"
            evidence = {"source": origin, "observed_at": row["seen"], "event_id": row["id"],
                        "attempt": row["attempt"] or "", "session_id": string(meta.get("session_id"))}
            body = blob(root, row)
            if row["kind"] == "document_html":
                html_seen = True
                continue  # Never execute or inject captured HTML, even to extract a title.
            payload = json.loads(body)
            if not isinstance(payload, dict):
                raise ValueError("unknown_content_shape")
            if row["kind"] == "media_result":
                media.append({**payload, "observed_at": row["seen"], "event_id": row["id"]})
                continue
            if row["kind"] == "note_object":
                if payload.get("noteId") != id_:
                    raise ValueError("body_ownership_mismatch")
                bodies.append(body_row(payload, id_, evidence))
                continue
            req_note = meta.get("note", meta.get("note_id"))
            if req_note != id_:
                raise ValueError("request_ownership_unconfirmed")
            kind = meta.get("kind")
            if restricted(payload, meta.get("status")) or meta.get("status") != 200 or payload.get("success") is not True or payload.get("code", 0) != 0:
                warnings.append(f"event:{row['id']}:unsuccessful_response")
                continue
            if kind == "detail":
                data = obj(payload.get("data"))
                for item in data.get("items", []) if isinstance(data.get("items"), list) else []:
                    if isinstance(item, dict) and string(item.get("id")) == id_:
                        note = obj(value(item, "note_card", "noteCard"))
                        if note:
                            claimed_id = value(note, "noteId", "note_id", "id")
                            if claimed_id is not None and string(claimed_id) != id_:
                                raise ValueError("body_ownership_mismatch")
                            bodies.append(body_row(note, id_, evidence))
                continue
            if kind not in {"comments", "replies"}:
                continue
            data = obj(payload.get("data"))
            if data.get("note_id", id_) != id_ or not isinstance(data.get("comments"), list):
                raise ValueError("response_ownership_or_shape_mismatch")
            parent = string(meta.get("parent", meta.get("root_comment_id"))) or string(data.get("root_comment_id"))
            parent_conflict = bool(kind == "replies" and parent and data.get("root_comment_id", parent) != parent)
            for comment in data["comments"]:
                if not isinstance(comment, dict) or not comment.get("id") or comment.get("note_id", id_) != id_:
                    warnings.append(f"event:{row['id']}:comment_ownership_or_id_missing")
                    continue
                comment_kind = "primary" if kind == "comments" else "reply"
                issue = "reply_parent_mismatch" if parent_conflict else ""
                comments.append(comment_row(comment, id_, comment_kind, None if parent_conflict or kind == "comments" else parent, evidence, issue))
                if kind == "comments":
                    for child in comment.get("sub_comments", []) if isinstance(comment.get("sub_comments"), list) else []:
                        if isinstance(child, dict) and child.get("id") and child.get("note_id", id_) == id_:
                            comments.append(comment_row(child, id_, "reply", string(comment["id"]), evidence))
                        else:
                            warnings.append(f"event:{row['id']}:embedded_reply_ownership_or_id_missing")
        except (OSError, ValueError, EOFError, KeyError, TypeError) as exc:
            warnings.append(f"event:{row['id']}:{type(exc).__name__}")
            if active:
                ledger.gap("archive_unreadable_or_invalid")
    if html_seen and not bodies:
        warnings.append("html_only_no_readable_body")
    if not bodies:
        warnings.append("no_readable_body")
    verified = task["comments"] == "complete_observed" and ledger.complete() and any(b["source"] == "active" for b in bodies)
    if task["comments"] == "complete_observed" and not verified:
        warnings.append("active_completeness_not_verified")
    return {"note_id": id_, "title": task["title"], "status": {k: task[k] for k in ("body", "comments", "media", "reason", "comment_media") if k in task.keys()},
            "active_complete_verified": verified, "active_chain": chain,
            "bodies": bodies, "comments": comments, "attempts": attempts, "media": media[-1] if media else None,
            "warnings": warnings, "verification_gaps": sorted(set(ledger.gaps))}


def in_source(row, source):
    return source == "all" or row["source"] in ({"active", "passive"} if source == "current" else {source})


def comment_count_evidence(body, comments, pagination_complete):
    """Do not mistake an observed pagination end for a reconciled exact total.

    A platform counter can include deleted/hidden comments or be stale. A
    mismatch is therefore unresolved evidence, not proof of collector data loss.
    Approximate/missing counters cannot establish an exact difference.
    """
    text = string((body or {}).get('comment_count')).strip()
    exact = int(text.replace(',', '')) if re.fullmatch(r'(?:[0-9]+|[0-9]{1,3}(?:,[0-9]{3})+)', text) else None
    observed = len(comments)
    status = ('not_exact' if exact is None else 'in_progress' if not pagination_complete
              else 'matched' if exact == observed else 'mismatch')
    return {'advertised_text': text, 'advertised_exact': exact, 'observed_unique': observed,
            'difference': exact - observed if exact is not None else None, 'status': status,
            'pagination_complete': bool(pagination_complete)}


def complete_post(db, root, id_):
    """One user-facing post, with ID-deduplicated comments and verified local media."""
    from .media import media_rows, verified_file
    result = projection(db, root, id_)
    files = media_rows(db, id_)
    known = {(m['comment_id'], m['media_id']) for m in files}
    for comment in result['comments']:
        for picture in comment['pictures']:
            key = (comment['comment_id'], picture['media_id'])
            if key not in known:
                files.append({**picture, 'note_id': id_, 'comment_id': comment['comment_id'], 'status': 'pending',
                              'path': None, 'size': None, 'sha256': None, 'reason': 'not_downloaded', 'event_id': comment['event_id']})
                known.add(key)
    for item in files:
        item['verified'] = bool(verified_file(root, item))
        if item['status'] == 'saved' and not item['verified']:
            item['status'], item['reason'] = 'missing', 'file_verification_failed'
    comments = {}
    for row in result['comments']:
        # Keep the evidence timeline in projection(); the normal view is one row per ID.
        previous = comments.get(row['comment_id'])
        if previous and not row['pictures']:
            row['pictures'] = previous['pictures']
        row['media'] = [m for m in files if m['comment_id'] == row['comment_id']]
        row['image_paths'] = [m['path'] for m in row['media'] if m['verified']]
        comments[row['comment_id']] = row
    users = {c['user_id']: c['user_name'] for c in comments.values() if c['user_name']}
    for row in comments.values():
        target = comments.get(row['target_comment_id'], {})
        row['target_user_name'] = row['target_user_name'] or target.get('user_name') or users.get(row['target_user_id'], '')
    body = result['bodies'][-1] if result['bodies'] else None
    status = dict(result['status'])
    if status.get('body') == 'saved' and not body:
        status['body'] = 'partial'
    if status.get('comments') == 'complete_observed' and not result['active_complete_verified']:
        status['comments'] = 'partial'
    for key, selected in [('media', [m for m in files if not m['comment_id']]),
                          ('comment_media', [m for m in files if m['comment_id']])]:
        if any(not m['verified'] for m in selected) or (key == 'media' and status.get(key) == 'success' and not selected):
            status[key] = 'partial'
    count_evidence = comment_count_evidence(body, comments, result['active_complete_verified'])
    if count_evidence['status'] == 'mismatch':
        result['warnings'].append('advertised_comment_total_mismatch')
    complete = (status.get('body') == 'saved' and result['active_complete_verified']
                and status.get('media') == 'success' and status.get('comment_media') == 'success'
                and count_evidence['status'] != 'mismatch')
    return {**result, 'body': body, 'title': (body or {}).get('title') or result['title'] or id_,
            'comments': list(comments.values()), 'media_files': files, 'status': status,
            'comment_count_evidence': count_evidence,
            # sampled: done under the primary-only sampling rule; skipped: body judged off-topic.
            'outcome': 'complete' if complete else 'restricted' if status.get('comments') == 'blocked' else
                       status['comments'] if status.get('body') == 'saved' and status.get('comments') in {'sampled', 'skipped'} else 'partial'}


def read_post(root, id_):
    root = Path(root).resolve()
    with closing(open_snapshot(root)) as db:
        return complete_post(db, root, id_)


def collection_status(root):
    root = Path(root).resolve()
    with closing(open_snapshot(root)) as db:
        tasks = [{k: r[k] for k in r.keys() if k != 'url'} for r in db.execute('SELECT * FROM tasks ORDER BY rowid')
                 if task_row(db, r['note_id']) is not None]
        counts = {'discovered': len(tasks), 'complete': 0, 'sampled': 0, 'skipped': 0, 'partial': 0, 'primary': 0, 'replies': 0, 'media': 0}
        saved_paths = set()
        for task in tasks:
            post = complete_post(db, root, task['note_id'])
            task.update(post['status'], title=post['title'], outcome=post['outcome'],
                        comment_count_evidence=post['comment_count_evidence'])
            for outcome in ('complete', 'sampled', 'skipped'):
                counts[outcome] += post['outcome'] == outcome
            counts['partial'] += post['outcome'] not in {'complete', 'sampled', 'skipped'} and task['status'] == 'finished'
            counts['primary'] += sum(c['kind'] == 'primary' for c in post['comments'])
            counts['replies'] += sum(c['kind'] == 'reply' for c in post['comments'])
            saved_paths.update(m['path'] for m in post['media_files'] if m['verified'])
            task['missing'] = [k for k, expected in [('body', 'saved'), ('media', 'success'), ('comments', 'complete_observed'), ('comment_media', 'success')] if task.get(k) != expected]
            if post['comment_count_evidence']['status'] == 'mismatch':
                task['missing'].append('comment_count_reconciliation')
        counts['media'] = len(saved_paths)
        search_row = db.execute("SELECT value FROM controls WHERE key='search_progress'").fetchone()
        searches = json.loads(search_row[0]) if search_row else []
        search = [{k: v for k, v in s.items() if k not in {'template', 'search_id', 'pages'}} | {
            'pages': len(s['pages']), 'discovered': len({i for p in s['pages'] for i in p['ids']}),
            'end_event': s.get('end_event')} for s in searches]
        blocked = db.execute("SELECT value FROM controls WHERE key='workspace_blocked'").fetchone()
        return {'tasks': tasks, 'counts': counts, 'search': search, 'access_restricted': bool(blocked and blocked[0]),
                'current': next((t['note_id'] for t in tasks if t['status'] == 'running'), None),
                'outcome': 'restricted' if blocked and blocked[0] else 'complete' if tasks and counts['complete'] == len(tasks) and all(s['status'] == 'complete' for s in search) else 'partial'}


def export_complete(root, ids=None):
    """Portable CSV/files export over the same SQLite snapshot; original blobs stay intact."""
    root = Path(root).resolve()
    parent = root / 'exports'
    parent.mkdir(exist_ok=True)
    draft = Path(tempfile.mkdtemp(prefix='.export-', dir=parent))
    final = parent / (datetime.now().strftime('%Y%m%d-%H%M%S-') + uuid4().hex[:8])
    media_fields = ['note_id', 'comment_id', 'media_id', 'kind', 'file_id', 'urls', 'path', 'status', 'reason', 'sha256', 'size', 'event_id', 'metadata']
    def write_csv(name, rows, fields):
        with (draft / name).open('w', encoding='utf-8-sig', newline='') as stream:
            writer = csv.writer(stream)
            writer.writerow(fields)
            writer.writerows([[safe_cell(row.get(k, ''), k) for k in fields] for row in rows])
    try:
        with closing(open_snapshot(root)) as db:
            ids = list(dict.fromkeys(ids)) if ids is not None else [r[0] for r in db.execute('SELECT note_id FROM tasks')]
            posts, comments, files, outcomes = [], [], [], []
            for id_ in ids:
                if task_row(db, id_) is None:
                    continue
                result = complete_post(db, root, id_)
                posts.append({**(result['body'] or {'note_id': id_}), **result['status'],
                              'media_paths': [m['path'] for m in result['media_files'] if not m['comment_id'] and m['verified']],
                              'outcome': result['outcome'],
                              'observed_comment_count': result['comment_count_evidence']['observed_unique'],
                              'comment_count_status': result['comment_count_evidence']['status'],
                              'comment_count_difference': result['comment_count_evidence']['difference']})
                comments.extend(result['comments'])
                files.extend(result['media_files'])
                outcomes.append({'note_id': id_, 'outcome': result['outcome'], 'status': result['status'],
                                 'warnings': result['warnings'], 'verification_gaps': result['verification_gaps'],
                                 'comment_count_evidence': result['comment_count_evidence']})
            for row in files:
                if row['verified']:
                    target = draft / row['path']
                    if not target.exists():
                        target.parent.mkdir(parents=True, exist_ok=True)
                        shutil.copyfile(root / row['path'], target)
            write_csv('posts.csv', posts, BODY_FIELDS + EVIDENCE_FIELDS + ['body', 'comments', 'media', 'comment_media', 'media_paths', 'outcome', 'reason',
                                                                         'observed_comment_count', 'comment_count_status', 'comment_count_difference'])
            write_csv('comments.csv', comments, COMMENT_FIELDS + EVIDENCE_FIELDS)
            write_csv('comment_media.csv', [r for r in files if r['comment_id']], media_fields)
            write_csv('post_media.csv', [r for r in files if not r['comment_id']], media_fields)
            search = db.execute("SELECT value FROM controls WHERE key='search_progress'").fetchone()
            summary = {'archive': str(root), 'cutoff_event_id': db.execute('SELECT coalesce(max(id),0) FROM events').fetchone()[0],
                       'posts': len(posts), 'comments': len(comments), 'saved_files': sum(r['verified'] for r in files),
                       'search': json.loads(search[0]) if search else [], 'results': outcomes}
            atomic_write(draft / 'summary.json', encode(summary))
        os.replace(draft, final)
        return {'folder': str(final), 'posts': len(posts), 'comments': len(comments), 'saved_files': summary['saved_files']}
    finally:
        if draft.exists():
            shutil.rmtree(draft)


def read_detail(root, id_, page=1, source="current"):
    if source not in SOURCES:
        raise ValueError("未知内容来源")
    if not isinstance(page, int) or isinstance(page, bool) or page < 1:
        raise ValueError("页码必须为正整数")
    root = Path(root).resolve()
    with closing(open_snapshot(root)) as db:
        cutoff = db.execute("SELECT coalesce(max(id),0) FROM events").fetchone()[0]
        result = projection(db, root, id_)
    comments = [c for c in result["comments"] if in_source(c, source)]
    bodies = [b for b in result["bodies"] if in_source(b, source)]
    page = min(page, max(1, (len(comments) + 49) // 50))
    return {**result, "bodies": bodies[-20:], "body_total": len(bodies), "comments": comments[(page - 1) * 50:page * 50],
            "comment_total": len(comments), "page": page, "source": source, "cutoff": cutoff,
            "attempts": result["attempts"][-20:], "warnings": result["warnings"][:100],
            "warning_count": len(result["warnings"])}


def formatted_note(row, markdown=False):
    def present(key):
        text = row.get(key) or "未记录"
        if markdown:
            text = text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            text = re.sub(r"([\\`*{}\[\]()#+.!_|~\-])", r"\\\1", text)
        return text
    data = {cn: present(en) for cn, en in {
        "作品ID": "note_id", "作品标题": "title", "作品描述": "text", "作品类型": "type", "作品标签": "tags",
        "发布时间": "published_at", "最后更新时间": "updated_at", "点赞数量": "like_count", "收藏数量": "collected_count",
        "评论数量": "comment_count", "分享数量": "share_count", "作者昵称": "author_name", "作者ID": "author_id",
    }.items()}
    data["作者链接"] = "未导出（本地内容投影，不加载远程资源）"
    prefix = f"观察来源：{row['source']}\n观察时间：{row['observed_at']}\n证据事件：{row['event_id']}\n缺失字段：{row['missing'] or '无'}\n\n"
    return prefix + (NoteGenerator.generate_md(data) if markdown else NoteGenerator.generate_txt(data))


def export_archive(root, ids, format_, expected_cutoff):
    """Publish a complete export directory atomically. Partial exports stay invisible."""
    root = Path(root).resolve()
    ids = list(dict.fromkeys(ids))
    if not ids or format_ not in {"summary", "content", "txt", "md", "links", "full_links"}:
        raise ValueError("无效的导出范围或格式")
    parent = root / "exports"
    parent.mkdir(exist_ok=True, mode=0o700)
    draft = Path(tempfile.mkdtemp(prefix=".export-", dir=parent))
    final = parent / (datetime.now().strftime("%Y%m%d-%H%M%S-") + uuid4().hex[:8])
    totals = {"notes": len(ids), "body_observations": 0, "comment_observations": 0, "warnings": 0}
    streams = []
    try:
        with closing(open_snapshot(root)) as db:
            cutoff = db.execute("SELECT coalesce(max(id),0) FROM events").fetchone()[0]
            if cutoff != expected_cutoff:
                raise ValueError("证据截止点已变化，请重新预览导出。")
            def writer(filename, fields):
                stream = (draft / filename).open("w", encoding="utf-8-sig", newline="")
                streams.append(stream)
                csv_writer = csv.writer(stream)
                csv_writer.writerow(fields)
                return lambda row: csv_writer.writerow([safe_cell(row.get(f, ""), f) for f in fields])
            summary = writer("summary.csv", SUMMARY_FIELDS)
            body_write = writer("notes.csv", BODY_FIELDS + EVIDENCE_FIELDS) if format_ == "content" else None
            comment_write = writer("comments.csv", COMMENT_FIELDS + EVIDENCE_FIELDS) if format_ == "content" else None
            links = []
            for id_ in ids:
                task = task_row(db, id_)
                if task is None:
                    raise ValueError("导出包含已隐藏或未知作品，请重新预览。")
                if format_ in {"links", "full_links"}:
                    url = task["url"]
                    links.append(url if format_ == "full_links" else urlsplit(url)._replace(query="", fragment="").geturl())
                if format_ in {"summary", "links", "full_links"}:
                    summary(dict(task))
                    continue
                result = projection(db, root, id_)
                # Export current active chain + dated passive observations. Historical attempts remain viewable separately.
                bodies = [b for b in result["bodies"] if in_source(b, "current")]
                comments = [c for c in result["comments"] if in_source(c, "current")]
                summary({**dict(task), "body_observations": len(bodies), "comment_observations": len(comments),
                         "warnings": ";".join(result["warnings"])})
                totals["body_observations"] += len(bodies)
                totals["comment_observations"] += len(comments)
                totals["warnings"] += len(result["warnings"])
                if body_write:
                    for row in bodies:
                        body_write(row)
                    for row in comments:
                        comment_write(row)
                if format_ in {"txt", "md"}:
                    for row in bodies:
                        atomic_write(draft / f"{id_}-event-{row['event_id']}.{format_}", formatted_note(row, format_ == "md").encode())
            if links:
                atomic_write(draft / "links.txt", ("\n".join(links) + "\n").encode())
            metadata = {"format": format_, "cutoff_event_id": cutoff, "note_ids": ids, **totals}
            atomic_write(draft / "export.json", encode(metadata))
            atomic_write(draft / "README.txt", (
                "本地只读导出；没有重新请求平台。正文/评论来自当前主动尝试链和各次被动观察。\n"
                "每行是一次已存观察，同一 ID 可能出现多次；行数不代表去重数量或完整快照。\n"
                "active / passive / history 分别表示当前主动尝试链 / 被动浏览 / 历史尝试。\n"
                "summary.csv 保留每篇的缺失/校验提示；原始存档与完整性判据未更改。\n"
                "标识字段及可能被表格当作公式的文本前加单引号，避免长 ID 失真和公式执行。\n"
                "时间保留已存原值；published_at / created_at 可能为平台毫秒时间戳，未推测缺失值。\n"
                "媒体处理记录不等于文件仍在原位置。完整访问链接含访问参数，请勿随意公开。\n"
            ).encode())
        for stream in streams:
            stream.flush()
            os.fsync(stream.fileno())
            stream.close()
        os.replace(draft, final)
        return {"folder": str(final), "cutoff": cutoff, **totals}
    finally:
        for stream in streams:
            if not stream.closed:
                stream.close()
        if draft.exists():
            shutil.rmtree(draft)
