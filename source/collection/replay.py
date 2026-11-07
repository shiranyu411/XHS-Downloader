"""Offline Ledger reconstruction from archived raw responses.

Used for (a) task-level resume from verified cursors and (b) integrity audits.
Only `network_json` events whose blob hash matches are replayed; the Ledger
applies exactly the same validation as during live capture.
"""
import gzip
import hashlib
import json

from .protocol import Ledger, Request, restricted


def chain_key(note):
    return f"resume_chain:{note}"


def request_from(meta):
    return Request(meta["kind"], meta.get("note"), meta.get("parent"), meta.get("cursor") or "",
                   meta.get("path") or "", tuple(tuple(x) for x in meta.get("scope") or ()))


def load_blob(root, row):
    body = gzip.decompress((root / row["path"]).read_bytes())
    if hashlib.sha256(body).hexdigest() != row["sha256"]:
        raise ValueError("hash mismatch")
    return body


def rebuild(db, root, note, attempts):
    """Replay comment responses of `attempts` (in order). Returns (ledger, stats)."""
    ledger = Ledger(note)
    stats = {"replayed": 0, "skipped_restricted": 0, "skipped_error": 0, "unreadable": 0}
    if not attempts:
        return ledger, stats
    marks = ",".join("?" * len(attempts))
    rows = db.execute(
        f"SELECT * FROM events WHERE kind='network_json' AND attempt IN ({marks}) ORDER BY id",
        list(attempts)).fetchall()
    for row in rows:
        meta = json.loads(row["metadata"])
        if meta.get("kind") not in ("comments", "replies") or meta.get("note") != note:
            continue
        try:
            if not row["valid"]:
                raise ValueError("invalidated archive evidence")
            payload = json.loads(load_blob(root, row))
        except (OSError, ValueError, EOFError):
            stats["unreadable"] += 1
            ledger.gap("archive_unreadable")
            continue
        status = meta.get("status") or 0
        # A restricted or failed page never produced an edge; the cursor is simply
        # requested again later. It must not become a permanent replayed gap.
        if restricted(payload, status):
            stats["skipped_restricted"] += 1
            continue
        if status != 200 or not isinstance(payload, dict) or payload.get("success") is not True or payload.get("code", 0) != 0:
            stats["skipped_error"] += 1
            continue
        req = request_from(meta)
        target = ledger.primary if req.kind == 'comments' else ledger.replies.get(req.parent)
        # A navigation on resume re-fetches the first page. Keep it as an observation,
        # not as a replacement for the committed cursor chain from the earlier attempt.
        if meta.get('pagination_role') == 'refresh_observation' or (
            row['attempt'] != attempts[0] and not meta.get('operation_id') and target and req.cursor in target.edges
        ):
            continue
        ledger.observe(req, payload, status)
        stats["replayed"] += 1
    return ledger, stats


def resumable(ledger):
    """A rebuilt ledger may seed a new attempt only if it is internally consistent."""
    return (not ledger.blocked and not ledger.gaps and not ledger.primary.problems
            and not any(c.problems for c in ledger.replies.values()) and bool(ledger.primary.edges))


def resume_seed(store, note):
    """Ledger + attempt chain for a new attempt, or (fresh Ledger, []) when unsafe."""
    raw = store.control(chain_key(note))
    chain = json.loads(raw) if raw else []
    if not chain:
        return Ledger(note), [], None
    ledger, stats = rebuild(store.db, store.root, note, chain)
    if not resumable(ledger):
        return Ledger(note), [], stats
    return ledger, chain, stats
