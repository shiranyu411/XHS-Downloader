import asyncio
import base64
from copy import deepcopy
from dataclasses import asdict
import json
from urllib.parse import parse_qs, urlsplit
from uuid import uuid4

from .protocol import classify, restricted
from .store import encode


class Capture:
    """Per-page CDP archive. Progress is published only after durable writes."""

    def __init__(self, store, attempt, note, ledger):
        self.store, self.attempt, self.note, self.ledger = store, attempt, note, ledger
        self.session_id = uuid4().hex
        self.requests = {}
        self.jobs = set()
        self.fatal = None
        self.cdp = None
        self.stopped = False
        self.documents = []
        self.templates = {}
        self.operation = None

    async def start(self, page):
        self.cdp = await page.context.new_cdp_session(page)
        self.cdp.on("Network.requestWillBeSent", self.request)
        self.cdp.on("Network.responseReceived", self.response)
        self.cdp.on("Network.loadingFinished", self.finished)
        self.cdp.on("Network.loadingFailed", self.failed)
        await self.cdp.send("Network.enable", {"maxTotalBufferSize": 100_000_000, "maxResourceBufferSize": 20_000_000})
        # Keep normal browser caching; unreadable cached responses are explicit gaps.

    def begin_operation(self, url):
        self.check()
        if self.operation is not None:
            raise RuntimeError("comment operation already in flight")
        req = classify(url)
        if not req or req.kind not in ("comments", "replies") or req.note != self.note:
            raise ValueError("invalid controlled request")
        op = {"id": uuid4().hex, "url": url, "request": req, "request_id": None,
              "done": asyncio.get_running_loop().create_future()}
        self.store.save(self.attempt, self.note, "operation_started", encode({"operation_id": op["id"], "url": url, **asdict(req)}))
        self.operation = op
        return op

    def end_operation(self, reason=None):
        if self.operation is not None:
            if reason:
                self.gap(self.operation["request_id"], reason)
            if not self.operation["done"].done():
                self.operation["done"].cancel()
            self.operation = None
        self.check()

    def request(self, event):
        r = event["request"]
        req = classify(r["url"], r.get("postData"), event.get("type"))
        if req is None or self.stopped:
            return
        key = event["requestId"]
        if key in self.requests:
            self.gap(key, "redirected_relevant_request")
        item = {"request": req, "url": r["url"], "method": r["method"], "status": None, "operation_id": None}
        self.requests[key] = item
        op = self.operation
        if op and req == op["request"]:
            expected = parse_qs(urlsplit(op["url"]).query, keep_blank_values=True)
            actual = parse_qs(urlsplit(r["url"]).query, keep_blank_values=True)
            if r["method"] == "GET" and all(actual.get(k) == v for k, v in expected.items()):
                if op["request_id"] is not None:
                    self.gap(key, "ambiguous_operation_request")
                else:
                    op["request_id"] = key
                    item["operation_id"] = op["id"]

    def response(self, event):
        if r := self.requests.get(event["requestId"]):
            response = event["response"]
            r["status"] = response["status"]
            r["cache"] = {k: response.get(k, False) for k in ("fromDiskCache", "fromServiceWorker", "fromPrefetchCache")}
            r["mime"] = response.get("mimeType")
            if r["status"] in (401, 403, 429, 461, 471):
                self.ledger.blocked = True

    def metadata(self, key, r):
        return {**asdict(r["request"]), "request_id": key, "session_id": self.session_id,
                "operation_id": r["operation_id"], "url": r["url"], "status": r["status"],
                "method": r["method"], "cache": r.get("cache", {}), "mime": r.get("mime")}

    def publish(self, candidate, raw_event=None):
        progress = candidate.record()
        self.store.save(self.attempt, self.note, "comment_progress", encode(progress),
                        {"raw_event": raw_event}, progress)
        self.ledger.__dict__.update(candidate.__dict__)

    def gap(self, key, reason):
        r = self.requests.get(key)
        try:
            event = self.store.save(self.attempt, self.note, "capture_gap", encode({"reason": reason}),
                                    self.metadata(key, r) if r else {})
            candidate = deepcopy(self.ledger)
            candidate.gap(reason)
            self.publish(candidate, event)
        except Exception as e:
            self.fatal = e

    def failed(self, event):
        key = event["requestId"]
        if key in self.requests:
            self.gap(key, "network_loading_failed")
            self.requests.pop(key, None)

    def finished(self, event):
        key = event["requestId"]
        if key in self.requests:
            task = asyncio.create_task(self.read_body(key))
            self.jobs.add(task)
            task.add_done_callback(self.jobs.discard)

    async def read_body(self, key):
        r = self.requests.get(key)
        if r is None:
            return
        try:
            response = await self.cdp.send("Network.getResponseBody", {"requestId": key})
            body = base64.b64decode(response["body"]) if response.get("base64Encoded") else response["body"].encode()
        except Exception:
            self.gap(key, "response_body_unreadable")
            self.requests.pop(key, None)
            return
        try:
            req = r["request"]
            kind = "document_html" if req.kind == "document" else "network_json"
            # Preserve the response even if parsing fails. Never acknowledge before COMMIT.
            event = self.store.save(self.attempt, self.note, kind, body, self.metadata(key, r))
            if req.kind in ("comments", "replies", "unknown_comment"):
                candidate = deepcopy(self.ledger)
                if req.kind == "unknown_comment":
                    candidate.gap("unknown_comment_endpoint")
                else:
                    try:
                        candidate.observe(req, json.loads(body), r["status"])
                    except (ValueError, TypeError, KeyError):
                        candidate.gap("unparseable_comment_response")
                self.publish(candidate, event)
                if req.note == self.note and r["method"] == "GET" and not candidate.gaps and not candidate.blocked:
                    self.templates.setdefault(req.kind, r["url"])
            if req.kind in ("search", "detail"):
                try:
                    payload = json.loads(body)
                except (ValueError, TypeError):
                    payload = None
                if restricted(payload, r["status"]):
                    candidate = deepcopy(self.ledger)
                    candidate.blocked = True
                    self.publish(candidate, event)
            if req.kind == "document" and req.note == self.note:
                self.documents.append(body)
            op = self.operation
            if op and r["operation_id"] == op["id"] and not op["done"].done():
                op["done"].set_result(event)
        except Exception as e:
            self.fatal = e
        finally:
            self.requests.pop(key, None)

    def check(self):
        if self.fatal:
            raise self.fatal

    async def drain(self, timeout=10):
        end = asyncio.get_running_loop().time() + timeout
        while (self.requests or self.jobs) and asyncio.get_running_loop().time() < end:
            self.check()
            await asyncio.sleep(0.05)
        self.check()
        return not self.requests and not self.jobs

    async def stop(self):
        try:
            await self.drain()
            self.stopped = True
            for key in list(self.requests):
                self.gap(key, "capture_drain_timeout")
            if self.jobs:
                jobs = list(self.jobs)
                for task in jobs:
                    task.cancel()
                await asyncio.gather(*jobs, return_exceptions=True)
            self.check()
        finally:
            self.stopped = True
            if self.cdp:
                await self.cdp.detach()
