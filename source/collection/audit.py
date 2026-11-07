"""Read-only batch audit: archive integrity, offline replay and acceptance metrics.

Output contains no URLs, tokens, note/user IDs or comment text, so it can be
pasted into reports. Notes are labelled by queue order (N1, N2, ...).
"""
from datetime import datetime
import json
from pathlib import Path
import sqlite3

from .protocol import restricted
from .replay import chain_key, load_blob, rebuild


def ts(value):
    return datetime.fromisoformat(value).timestamp()


def audit(root):
    root = Path(root).resolve()
    db = sqlite3.connect(f"file:{root / 'state.sqlite'}?mode=ro", uri=True)
    db.row_factory = sqlite3.Row
    try:
        return _audit(db, root)
    finally:
        db.close()


def _audit(db, root):
    controls = {r["key"]: r["value"] for r in db.execute("SELECT key,value FROM controls")}
    tasks = [dict(r) for r in db.execute("SELECT * FROM tasks ORDER BY rowid")]
    integrity = {"events": 0, "hash_failures": 0, "flagged_invalid": 0}
    for row in db.execute("SELECT * FROM events"):
        integrity["events"] += 1
        if not row["valid"]:
            integrity["flagged_invalid"] += 1
            continue
        try:
            load_blob(root, row)
        except (OSError, ValueError, EOFError):
            integrity["hash_failures"] += 1

    # Controlled comment/reply traffic, ordered in time.
    traffic = []
    for row in db.execute("SELECT * FROM events WHERE kind='network_json' AND valid=1 ORDER BY id"):
        meta = json.loads(row["metadata"])
        if meta.get("kind") not in ("comments", "replies"):
            continue
        try:
            payload = json.loads(load_blob(root, row))
        except (OSError, ValueError, EOFError):
            payload = None
        traffic.append({"t": ts(row["seen"]), "kind": meta["kind"], "note": row["note_id"],
                        "restricted": restricted(payload, meta.get("status") or 0), "status": meta.get("status"),
                        "ids": [str(c.get("id")) for c in (payload or {}).get("data", {}).get("comments", []) or []
                                if isinstance(c, dict)] if isinstance(payload, dict) and isinstance(payload.get("data"), dict) else []})

    notes, resumed, resumed_ok, archived_ids, final_ids = [], 0, 0, 0, 0
    for index, task in enumerate(tasks, 1):
        id_ = task["note_id"]
        raw = controls.get(chain_key(id_))
        chain = json.loads(raw) if raw else [r[0] for r in db.execute(
            "SELECT id FROM attempts WHERE note_id=? ORDER BY rowid DESC LIMIT 1", (id_,))]
        ledger, stats = rebuild(db, root, id_, chain)
        record = ledger.record()
        primary = len(record["primary"]["ids"])
        replies = sum(len(c["ids"]) for c in record["replies"].values())
        # Every ID seen in any archived page for this note (all attempts).
        seen = set()
        for item in traffic:
            if item["note"] == id_ and not item["restricted"]:
                seen.update(item["ids"])
        archived_ids += len(seen)
        final_ids += len(seen & set(ledger.comments))
        replay_complete = ledger.complete()
        consistent = (task["comments"] == "complete_observed") == replay_complete or task["comments"] == "not_requested"
        if len(chain) > 1:
            resumed += 1
            resumed_ok += replay_complete
        notes.append({"label": f"N{index}", "status": task["status"], "comments": task["comments"],
                      "replay_complete": replay_complete, "replay_consistent_with_task": consistent,
                      "primary": primary, "replies": replies, "unique_comments": len(ledger.comments),
                      "advertised_replies": sum(record["advertised_reply_counts"].values()),
                      "gaps": sorted(set(record["gaps"])), "attempts_in_chain": len(chain), **stats})

    duration = (traffic[-1]["t"] - traffic[0]["t"]) if len(traffic) > 1 else 0.0
    restricted_count = sum(t["restricted"] for t in traffic)
    first = next((i for i, t in enumerate(traffic) if t["restricted"]), None)
    pre = None
    if first is not None:
        t0 = traffic[first]["t"]
        gaps = [b["t"] - a["t"] for a, b in zip(traffic[:first], traffic[1:first + 1])]
        pre = {"index": first + 1, "kind": traffic[first]["kind"], "status": traffic[first]["status"],
               "requests_prev_10s": sum(t0 - 10 <= t["t"] < t0 for t in traffic),
               "requests_prev_60s": sum(t0 - 60 <= t["t"] < t0 for t in traffic),
               "median_gap_s": round(sorted(gaps)[len(gaps) // 2], 3) if gaps else None}
    effective = sum(n["unique_comments"] for n in notes)
    requested = [n for n in notes if n["comments"] != "not_requested"]
    return {
        "integrity": integrity,
        "traffic": {"comment_requests": sum(t["kind"] == "comments" for t in traffic),
                    "reply_requests": sum(t["kind"] == "replies" for t in traffic),
                    "restricted_responses": restricted_count, "first_restriction": pre,
                    "active_span_s": round(duration, 1)},
        "metrics": {
            "effective_comments_per_hour": round(effective / duration * 3600, 1) if duration else None,
            "complete_note_ratio": round(sum(n["replay_complete"] for n in requested) / len(requested), 3) if requested else None,
            "restriction_rate_per_100_requests": round(100 * restricted_count / len(traffic), 2) if traffic else None,
            "data_loss_ratio": round(1 - final_ids / archived_ids, 4) if archived_ids else None,
            "resume_success_ratio": round(resumed_ok / resumed, 3) if resumed else None,
            "replay_consistency": all(n["replay_consistent_with_task"] for n in notes),
        },
        "pacing": {k: controls[k] for k in ("pacing_factor", "pacing_restrictions", "pacing_cooldown_reason") if k in controls},
        "notes": notes,
    }


def main(argv=None):
    import argparse
    parser = argparse.ArgumentParser(description="只读审计采集批次（不联网）")
    parser.add_argument("batch", type=Path, nargs="+")
    args = parser.parse_args(argv)
    for batch in args.batch:
        print(json.dumps({"batch": batch.name, **audit(batch)}, ensure_ascii=False, indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
