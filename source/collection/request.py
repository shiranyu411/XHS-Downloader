"""Build read-only requests; observed page parameters take precedence over defaults."""
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit


def navigation_url(url):
    """Open every note at /explore/{id}, preserving its token and query exactly.

    This direct open ran 10 hours without a refusal. Search-card links
    (/search_result/{id}) are rewritten to it; a refused open stops the batch
    instead of being retried through the site's router or another route.
    """
    from .config import note_id
    return urlunsplit(urlsplit(url)._replace(path='/explore/' + note_id(url)))


def request_url(req, templates, note_url):
    template = templates.get(req.kind)
    if template:
        parts = urlsplit(template)
        query = dict(parse_qsl(parts.query, keep_blank_values=True))
        origin, path = parts.netloc, parts.path
    else:
        origin = "edith.xiaohongshu.com"
        path = "/api/sns/web/v2/comment/" + ("sub/page" if req.kind == "replies" else "page")
        query = {"image_formats": "jpg,webp,avif"}
        # Reply pagination uses the site's `num` parameter. `count` is not a
        # synonym and silently fell back to much smaller reply pages.
        if req.kind == "replies":
            query["num"] = "10"
        else:
            query["count"] = "20"
        # These are page sizes, never total-comment limits; an observed template
        # above always retains its own parameters.
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
