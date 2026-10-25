from dataclasses import asdict, dataclass
from urllib.parse import urlsplit
import re

from . import UPSTREAM, VERSION


@dataclass(frozen=True)
class Config:
    cdp: str = "http://127.0.0.1:9222"
    open_interval: float = 20
    page_interval: float = 0
    max_scrolls: int = 50
    note_timeout: float = 300
    operation_timeout: float = 8
    max_operations: int = 500
    no_progress_limit: int = 3
    media: bool = False
    comments: bool = True
    strategy: str = "auto"

    def __post_init__(self):
        u = urlsplit(self.cdp)
        if u.scheme != "http" or u.hostname not in {"localhost", "127.0.0.1", "::1"}:
            raise ValueError("CDP 必须是本机 http 地址")
        for key in ("open_interval", "page_interval", "max_scrolls", "note_timeout", "operation_timeout",
                    "max_operations", "no_progress_limit"):
            value = getattr(self, key)
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not 0 <= value <= 86400:
                raise ValueError(f"非法配置: {key}")
        if min(self.note_timeout, self.operation_timeout, self.max_operations, self.no_progress_limit) <= 0:
            raise ValueError("超时与操作上限必须大于零")
        for key in ("max_scrolls", "max_operations", "no_progress_limit"):
            if not isinstance(getattr(self, key), int):
                raise ValueError(f"{key} 必须为整数")
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
    m = re.fullmatch(r"/(?:explore|discovery/item)/([a-fA-F0-9]{24})/?", u.path)
    if not m:
        raise ValueError("仅接受笔记链接；作者页请在浏览器中打开后发现作品")
    return m[1].lower()
