"""Read-only operation vocabulary. No queues, plans or execution state live here."""
import hashlib
import json
import re
from urllib.parse import urlsplit

from .config import note_id

INTENTS = frozenset({"start", "resume", "retry"})
KINDS = frozenset({"comments", "download"})
CATEGORIES = {
    "new": "新作品", "ready": "已登记未处理", "pending": "已在待处理队列",
    "resume": "旧暂停 / 中断，需明确继续", "retry": "部分 / 失败 / 跳过，需明确重试",
    "complete": "已有处理结果", "running": "正在处理", "hidden": "已隐藏",
}
ELIGIBLE = {"start": {"new", "ready", "pending"}, "resume": {"pending", "resume"}, "retry": {"retry"}}
URL_PATTERN = re.compile(r"https?://[^\s\"<>\\^`{|}，。；！？、【】《》‘’“”]+", re.I)


def parse_input(content):
    """Extract complete links from share text, without ever resolving short links."""
    if not isinstance(content, str) or len(content) > 2_000_000:
        raise ValueError("链接内容过长，请分批提交。")
    urls, shorts, invalid = [], [], []
    for raw in dict.fromkeys(m.group().rstrip(".,;!?)）]】") for m in URL_PATTERN.finditer(content)):
        if len(urls) + len(shorts) + len(invalid) >= 5000:
            raise ValueError("一次最多识别 5000 条链接，请分批提交。")
        try:
            parsed = urlsplit(raw)
            if parsed.hostname in {"xhslink.com", "xhslink.cn"} and not parsed.username and not parsed.port:
                shorts.append(raw)
            else:
                note_id(raw)
                urls.append(raw)
        except ValueError:
            invalid.append(raw)
    return {"urls": urls, "short_links": shorts, "invalid_count": len(invalid),
            "note_ids": list(dict.fromkeys(note_id(url) for url in urls))}


def category(row, kind):
    if row is None:
        return "new"
    if row["hidden"]:
        return "hidden"
    job = row.get(kind + "_state")
    if job == "running":
        return "running"
    complete = row["comments"] == "complete_observed" if kind == "comments" else row["media"] == "success"
    if complete or job == "success":
        return "complete"
    if job in {"paused", "interrupted"}:
        return "resume"
    if job == "pending":
        return "pending"
    result = row["comments"] if kind == "comments" else row["media"]
    if job in {"partial", "failed", "cancelled", "skipped"} or result in {"partial", "failed", "blocked", "skipped"}:
        return "retry"
    return "ready"


def signature(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def filter_rows(rows, query="", filter_="all", source="", ids=None):
    """One scope definition for pagination, selection and exports (never page-only)."""
    if filter_ not in {"all", "pending", "partial", "complete", "downloaded"}:
        raise ValueError("未知状态筛选")
    wanted = set(ids) if ids is not None else None
    needle = str(query).casefold()
    return [r for r in rows
            if (wanted is None or r["note_id"] in wanted)
            and needle in (r["note_id"] + " " + r["title"]).casefold()
            and (not source or source in r["sources"])
            and (filter_ == "all"
                 or filter_ == "pending" and r["comments"] == "not_requested"
                 or filter_ == "partial" and r["comments"] in {"partial", "blocked", "failed"}
                 or filter_ == "complete" and r["comments"] == "complete_observed"
                 or filter_ == "downloaded" and r["media"] == "success")]
