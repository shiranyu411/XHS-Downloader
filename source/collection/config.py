from dataclasses import asdict, dataclass
from urllib.parse import urlsplit
import re

from . import UPSTREAM, VERSION


@dataclass(frozen=True)
class Config:
    cdp: str = "http://127.0.0.1:9223"
    # Seconds between page views (post opens and search result pages), times the pacing factor.
    open_interval: float = 60
    page_interval: float = 15
    max_scrolls: int | None = 50  # None means no count limit; 0 retains snapshot-only mode.
    note_timeout: float = 300
    operation_timeout: float = 8
    max_operations: int | None = 500  # None traverses until end evidence or a recorded stop.
    no_progress_limit: int = 3
    # Cooperative time slicing, not a total-comment limit. None keeps legacy flow.
    comment_page_quantum: int | None = None
    post_concurrency: int = 1  # shared pacer and SQLite writer, up to six posts
    media: bool = True
    comments: bool = True
    strategy: str = "auto"
    # Batch-wide pacing for controlled comment/reply requests (0 disables a cap).
    max_requests_per_minute: int = 4
    session_request_budget: int = 100
    session_rest: float = 1800
    restriction_cooldown: float = 21600
    # 'primary' samples primary comments (with the replies shown inline) and
    # stops at valid_comment_cap substantive comments; 'all' expands threads.
    comment_scope: str = "all"
    valid_comment_cap: int | None = None
    # Skip comments/media of posts whose body misses the batch topic-profile.json (no profile: keep all).
    topic_gate: bool = False
    # Before the priority order, searched keywords take turns (fewest settled posts first)
    # until each has this many on-topic posts saved; 0 turns it off.
    sample_per_keyword: int = 0

    def __post_init__(self):
        u = urlsplit(self.cdp)
        if u.scheme != "http" or u.hostname not in {"localhost", "127.0.0.1", "::1"}:
            raise ValueError("CDP 必须是本机 http 地址")
        for key in ("open_interval", "page_interval", "max_scrolls", "note_timeout", "operation_timeout",
                    "max_operations", "no_progress_limit", "comment_page_quantum", "max_requests_per_minute",
                    "session_request_budget", "session_rest", "restriction_cooldown", "sample_per_keyword"):
            value = getattr(self, key)
            if key in {'max_scrolls', 'max_operations', 'comment_page_quantum'} and value is None:
                continue
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not 0 <= value <= 86400:
                raise ValueError(f"非法配置: {key}")
        if min(self.note_timeout, self.operation_timeout, self.no_progress_limit) <= 0 or self.max_operations == 0:
            raise ValueError("超时与操作上限必须大于零；无限操作使用 None")
        if isinstance(self.post_concurrency, bool) or not isinstance(self.post_concurrency, int) or not 1 <= self.post_concurrency <= 6:
            raise ValueError("帖子并发数必须是 1–6 的整数")
        if self.comment_page_quantum == 0:
            raise ValueError("评论分片页数必须大于零；不分片使用 None")
        for key in ("max_scrolls", "max_operations", "no_progress_limit", "comment_page_quantum", "max_requests_per_minute",
                    "session_request_budget", "sample_per_keyword"):
            if key in {'max_scrolls', 'max_operations', 'comment_page_quantum'} and getattr(self, key) is None:
                continue
            if not isinstance(getattr(self, key), int):
                raise ValueError(f"{key} 必须为整数")
        if self.comment_scope not in {"all", "primary"}:
            raise ValueError("comment_scope 必须为 all/primary")
        if self.valid_comment_cap is not None and (isinstance(self.valid_comment_cap, bool) or not isinstance(self.valid_comment_cap, int)
                                                   or not 1 <= self.valid_comment_cap <= 100000):
            raise ValueError("valid_comment_cap 必须为正整数或 None")
        if not isinstance(self.topic_gate, bool):
            raise ValueError("topic_gate 必须为布尔值")
        if self.strategy not in {"auto", "request", "ui"}:
            raise ValueError("strategy 必须为 auto/request/ui")
        if not isinstance(self.media, bool) or not isinstance(self.comments, bool):
            raise ValueError("media/comments 必须为布尔值")

    def record(self):
        return {**asdict(self), "adapter": VERSION, "upstream": UPSTREAM, "path": "browser_cdp"}


def note_id(url: str) -> str:
    u = urlsplit(url)
    if u.scheme != "https" or u.hostname not in {"www.xiaohongshu.com", "www.rednote.com"} or u.username or u.port:
        raise ValueError("需要小红书完整 HTTPS 笔记链接（含原有访问参数）")
    m = re.fullmatch(r"/(?:explore|discovery/item|search_result)/([a-fA-F0-9]{24})/?", u.path)
    if not m:
        raise ValueError("仅接受笔记链接；作者页请在浏览器中打开后发现作品")
    return m[1].lower()
