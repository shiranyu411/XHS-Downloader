"""Optional topic screening, driven by a per-batch JSON profile.

Without a profile nothing is screened: every search title is kept at
priority 0 and the body gate keeps every post, so the collector behaves like
a plain keyword crawler.

To screen, put ``topic-profile.json`` in the batch directory (see
``docs/topic-profile.example.json``). Every field is optional:

- ``core_keywords``: search keywords whose posts are never body-gated and rank first
- ``include``: regex, the title/body clearly names the topic
- ``context``: regex, strengthens ``include``; with a core keyword alone → 待定
- ``tool``: regex, ``include`` + ``tool`` without context → 待定 (opened last)
- ``exclude``: regex, a clear homonym collision → title 剔除 / body dropped
- ``rescue``: regex, overrides ``exclude`` to 待定 and strengthens ``include``
- ``hint``: regex, a weaker body hint, trusted only after the ``exclude`` check

Inclusive by design: only clear collisions are dropped; anything unsure is
opened and judged from the body.
"""
import json
from pathlib import Path
import re

PROFILE_NAME = 'topic-profile.json'
FIELDS = ('include', 'context', 'tool', 'exclude', 'rescue', 'hint')
TOOL_REASON = '主题+工具类'
_cache = {}


class Profile:
    def __init__(self, data):
        if not isinstance(data, dict):
            raise ValueError('topic profile 必须是 JSON 对象')
        self.core = {str(k) for k in data.get('core_keywords') or []}
        for field in FIELDS:
            value = data.get(field)
            setattr(self, field, re.compile(value, re.IGNORECASE) if value else None)

    def search(self, field, text):
        pattern = getattr(self, field)
        return pattern.search(text) if pattern else None


def load_profile(path):
    return Profile(json.loads(Path(path).read_text(encoding='utf-8')))


def profile_for(root):
    """The batch's profile, re-read when the file changes; None when absent."""
    path = Path(root) / PROFILE_NAME
    try:
        stamp = path.stat().st_mtime_ns
    except FileNotFoundError:
        _cache.pop(path, None)
        return None
    cached = _cache.get(path)
    if not cached or cached[0] != stamp:
        cached = _cache[path] = (stamp, load_profile(path))
    return cached[1]


def classify_title(title, keywords, profile=None):
    """Search-card title → (保留|待定|剔除, reason). Only 剔除 is ever skipped."""
    if profile is None:
        return '保留', '未配置主题筛选'
    s = profile.search
    a, c, t, g, m = (s(f, title) for f in ('include', 'context', 'tool', 'exclude', 'rescue'))
    if a and (c or m):
        return '保留', f'主题相关:{a[0]}+{(c or m)[0]}'
    if a and t:
        return '待定', f'{TOOL_REASON}:{a[0]}+{t[0]}，打开正文后判断'
    if a:
        return '保留', f'主题相关:{a[0]}'
    if g and m:
        return '待定', f'疑似无关:{g[0]}，但有线索:{m[0]}，打开正文后判断'
    if g:
        return '剔除', f'明显无关:{g[0]}'
    if not title:
        return '待定', '标题为空，打开正文后判断'
    if c and set(keywords) & profile.core:
        return '待定', f'核心词+语境词:{c[0]}，打开正文后判断'
    return '待定', '标题看不出，打开正文后判断'


def priority(verdict, keywords, profile=None):
    """0 core 保留, 1 core 待定, 2 ambiguous 保留, 3 ambiguous 待定; -1 never collected."""
    if verdict == '剔除':
        return -1
    core = profile is None or not keywords or bool(set(keywords) & profile.core)
    return (0 if verdict == '保留' else 1) + (0 if core else 2)


def note_text(note):
    tags = [t.get('name') or '' for t in note.get('tagList') or [] if isinstance(t, dict)]
    return ' '.join([note.get('title') or '', note.get('desc') or '', *tags])


def relevant_note(note, keywords, profile=None):
    """Body/tag gate before spending comment requests → (keep, reason).

    Core-keyword posts are never dropped here (the topic may only be in
    images). Other posts are kept when the body or tags name the topic, or
    hint at it without a clear collision.
    """
    if profile is None:
        return True, '未配置主题筛选'
    if not keywords or set(keywords) & profile.core:
        return True, '核心词帖子，不按正文筛'
    text = note_text(note)
    if m := profile.search('include', text):
        return True, f'正文/标签含主题词:{m[0]}'
    if m := profile.search('exclude', text):
        return False, f'正文无主题词且明显无关:{m[0]}'
    if m := profile.search('hint', text) or profile.search('rescue', text):
        return True, f'正文有主题线索:{m[0]}'
    return False, '正文和标签都没有主题线索'


STRIP = re.compile(r'\[[^\[\]]{1,8}R?\]|@\S+|[\s\W_]+|[\U0001F000-\U0001FAFF☀-➿]', re.UNICODE)
JUNK = re.compile(r'(?i)^(蹲+.{0,3}|同问.{0,3}|求.{0,5}|d+|滴+|顶+|\+?1+|插眼|码住|马住|mark|先赞.{0,3}|赞+|收藏了?|已关注|互关.{0,2}|路过|打卡|沙发|前排|支持+|加油+|哈+|啊+|嗯+|呜+|6+|好+的?|我也是?|是的|确实|真的|对+|来了|来啦|太好了|笑死|绝了|牛+|可爱+|好可爱|好好笑|好好看|我也想要|想要|链接|求链接)$')


def valid_comment(comment):
    """Substantive comment: a picture, or ≥4 characters that are not a stock reaction."""
    if comment.get('pictures'):
        return True  # screenshots often carry the content
    text = STRIP.sub('', str(comment.get('content') or ''))
    return len(text) >= 4 and not JUNK.match(text)
