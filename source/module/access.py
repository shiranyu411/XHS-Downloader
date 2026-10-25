"""Recognize explicit access refusals; never retry them as transport failures."""

import json
import re
from html import unescape
from urllib.parse import urlsplit


class AccessRestricted(RuntimeError):
    def __init__(self, reason="access_restricted"):
        self.reason = reason
        super().__init__(
            "平台拒绝访问，已暂停自动操作；请按平台提示处理，不会自动重试。"
        )


WORDS = (
    "账号状态异常",
    "帐号状态异常",
    "账号异常",
    "帐号异常",
    "访问频繁",
    "操作频繁",
    "访问限制",
    "安全验证",
    "请先登录",
    "captcha",
    "rate limit",
)


def refusal_reason(status, text, url):
    host = urlsplit(str(url)).hostname or ""
    if not any(
        host == domain or host.endswith("." + domain)
        for domain in ("xiaohongshu.com", "rednote.com")
    ):
        return None
    if status in (401, 403, 429, 461, 471):
        return f"http_{status}"
    content = text.lstrip()
    if content.startswith("{"):
        try:
            data = json.loads(content)
            if isinstance(data, dict):
                message = str(data.get("msg", data.get("message", ""))).lower()
                if data.get("code") in (
                    -100,
                    -101,
                    -102,
                    -10000,
                    300012,
                    300013,
                ) or any(w in message for w in WORDS):
                    return "business_access_restricted"
        except ValueError:
            pass
    # Only page-level errors, never arbitrary note/comment text mentioning restrictions.
    titles = re.findall(r"<title\b[^>]*>(.*?)</title>", text[:200000], re.I | re.S)
    if any(any(word in unescape(title).lower() for word in WORDS) for title in titles):
        return "account_abnormal"
    return None
