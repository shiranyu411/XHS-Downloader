"""Build read-only requests; observed page parameters take precedence over defaults."""
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit


def request_url(req, templates, note_url):
    template = templates.get(req.kind)
    if template:
        parts = urlsplit(template)
        query = dict(parse_qsl(parts.query, keep_blank_values=True))
        origin, path = parts.netloc, parts.path
    else:
        origin = "edith.xiaohongshu.com"
        path = "/api/sns/web/v2/comment/" + ("sub/page" if req.kind == "replies" else "page")
        query = {"count": "20", "image_formats": "jpg,webp,avif"}
        # This is a page size, never a total-comment limit. Defaults are pinned
        # upstream parameters, not a claim that the current website is compatible.
        primary = dict(parse_qsl(urlsplit(templates.get("comments", note_url)).query, keep_blank_values=True))
        for key in ("xsec_token", "image_formats"):
            if key in primary:
                query[key] = primary[key]
        if req.kind == "comments":
            query["top_comment_id"] = ""
    if origin != "edith.xiaohongshu.com" or req.kind not in ("comments", "replies"):
        raise ValueError("unsupported comment origin or kind")
    query.update(note_id=req.note, cursor=req.cursor)
    if req.kind == "replies":
        query["root_comment_id"] = req.parent
    return urlunsplit(("https", origin, path, urlencode(query), ""))
