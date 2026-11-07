"""One batch owner, independent durable queues, conservative serial execution."""

import asyncio
from dataclasses import replace
from datetime import datetime
import csv
import io
import json
from pathlib import Path
import re
import time
from collections import deque
from contextlib import AsyncExitStack
from ..module.access import AccessRestricted

from ..collection.config import Config
from ..collection.store import atomic_write, encode
from ..collection.workspace import Workspace
from ..collection.service import CollectionService
from ..collection.workflow import CATEGORIES, ELIGIBLE, INTENTS, KINDS, category, filter_rows, parse_input, signature


CONFIG_FIELDS = frozenset({"open_interval", "page_interval", "max_operations", "note_timeout",
                           "max_requests_per_minute", "session_request_budget", "session_rest", "restriction_cooldown"})


class WorkbenchService:
    def __init__(self, root, backend, host):
        self.root, self.backend, self.host = Path(root), backend, host
        self.registry = self.root / ".workbench-ui.json"
        self.access_path = self.root / ".workbench-access.json"
        self.access_reason = ""
        self.access_cooldown = 0.0
        try:
            access = json.loads(self.access_path.read_text())
            self.access_reason = str(access.get("reason") or "")
            self.access_cooldown = float(access.get("cooldown_until") or 0)
        except FileNotFoundError:
            pass
        except (OSError, ValueError, AttributeError):
            self.access_reason = "access_state_unreadable"
        self.store = None
        self.job = None
        self.runner = None
        self.passive = None
        self.mode = None
        self.stopping = False
        self.closing = False
        self.active = self.last_scope = self.handoff = self.last_export = None
        self._discovery_start = None
        self.message = "添加或选择作品即可开始；不会自动运行已有队列。"
        self.logs = deque(maxlen=80)
        self.lock = asyncio.Lock()
        self._snapshot_key = None
        self._snapshot_data = None
        try:
            data = json.loads(self.registry.read_text())
            path = Path(data["batch"])
            if (path / "state.sqlite").is_file():
                self.store = Workspace(path)
                self.message = "已恢复批次；未自动开始任何任务。"
        except FileNotFoundError:
            pass
        except (OSError, ValueError, KeyError, RuntimeError):
            self.message = "未能恢复上次批次，请手动打开；不会自动创建或覆盖原数据。"
        self.sync_download_tasks()

    def sync_download_tasks(self):
        if not self.store:
            return
        from .backend import TaskState

        rows = list(self.store.db.execute("""SELECT t.note_id,t.url,j.task_id,j.state,j.reason FROM workspace_jobs j
                JOIN tasks t USING(note_id) WHERE j.kind='download'
                AND NOT EXISTS(SELECT 1 FROM workspace_dismissed d WHERE d.task_id=j.task_id)"""))
        visible = {r["task_id"] for r in rows}
        removed = {id_ for id_, task in self.backend.tasks.items()
                   if task.source == "workbench" and id_ not in visible}
        for id_ in removed:
            self.backend.tasks.pop(id_, None)
        for id_, item in list(self.backend.files.items()):
            if item.get("task_id") in removed:
                self.backend.files.pop(id_, None)
        for row in rows:
            state = {"running": "processing", "partial": "failed"}.get(row["state"], row["state"])
            task = self.backend.tasks.get(row["task_id"])
            if task:
                task.state = state
                task.reason = row["reason"] or ""
            else:
                self.backend.tasks[row["task_id"]] = TaskState(
                    row["task_id"],
                    row["url"],
                    source="workbench",
                    state=state,
                    display_text=row["note_id"],
                    reason=row["reason"] or "",
                )

    def cancel_download(self, task_id):
        if not self.store:
            return False
        with self.store.db:
            count = self.store.db.execute(
                """UPDATE workspace_jobs SET state='cancelled' WHERE task_id=?
                AND kind='download' AND state IN ('pending','paused','interrupted')""",
                (task_id,),
            ).rowcount
        self.sync_download_tasks()
        return bool(count)

    @property
    def busy(self):
        return self.job is not None and not self.job.done()

    @property
    def blocked(self):
        return bool(self.access_reason) or bool(
            self.store
            and (
                self.store.control("workspace_blocked")
                or self.store.db.execute(
                    "SELECT 1 FROM tasks WHERE comments='blocked' LIMIT 1"
                ).fetchone() is not None
                and self.store.control("workspace_restriction_ack") != "yes"
            )
        )

    def log(self, message):
        self.message = message
        self.logs.append(
            {"time": datetime.now().strftime("%H:%M:%S"), "message": message}
        )

    def idle(self):
        if self.busy:
            raise ValueError("后台任务运行中，请先安全暂停；切换流程标签不受影响。")

    def restrict(self, reason="access_restricted"):
        if self.access_reason == reason:
            return
        self.access_reason = reason
        if self.store:
            from ..collection.pacing import Pacer
            self.access_cooldown = max(self.access_cooldown, Pacer(self.store).restricted())
        else:
            self.access_cooldown = max(self.access_cooldown, time.time() + Config().restriction_cooldown)
        atomic_write(self.access_path, encode({"reason": reason, "cooldown_until": self.access_cooldown}))
        if self.store:
            self.store.control("workspace_blocked", reason)
            self.store.control("workspace_restriction_ack", "no")
        self.pause()
        self.log("平台访问已暂停：" + reason + "；不会自动重试。")

    def ensure_store(self):
        if self.store is None:
            path = self.root / "collections" / "默认工作区"
            self.select(path, create=not path.exists(), name="默认工作区")
        return self.store

    def permission(self, mode):
        from ..collection.pacing import Pacer
        until = 0.0
        reason, code = "", "ready"
        if self.closing:
            reason, code = "正在安全关闭", "closing"
        elif not self.backend.settings.get("disclaimer_accepted"):
            reason, code = "请先完成首次使用确认", "consent_required"
        elif self.busy:
            reason, code = ("正在保存已收到内容，请等待暂停完成", "stopping") if self.stopping else ("后台作业正在运行，请先安全暂停", "busy")
        elif self.blocked:
            reason, code = "访问限制尚未人工确认恢复", "access_restricted"
        elif mode != "passive":
            until = max(self.access_cooldown, Pacer(self.store).cooldown_until() if self.store else 0)
            if until > time.time():
                reason = "冷却/预算休息尚未结束"
                code = self.store.control("pacing_cooldown_reason") if self.store else None
                code = code or "cooldown_active"
        return {"can_start": not reason, "reason": reason, "reason_code": code, "until": until}

    def check_access(self, mode=None):
        allowed = self.permission(mode)
        if not allowed["can_start"]:
            suffix = time.strftime("，可重试时间 %m-%d %H:%M", time.localtime(allowed["until"])) if allowed["until"] else ""
            raise ValueError(allowed["reason"] + suffix + "；不会自动开始或更换路径。")

    def require_store(self):
        if self.store is None:
            raise ValueError(
                "当前还没有作品；可以直接下载、发现或导入，程序会自动管理默认工作区。"
            )
        return self.store

    def select(self, path, create=False, name=None, values=None):
        self.idle()
        if self.blocked and not self.access_reason:
            self.restrict("access_restricted")
        path = Path(path).expanduser().resolve()
        if create:
            if values is not None and (
                not isinstance(values, dict)
                or set(values) - CONFIG_FIELDS
            ):
                raise ValueError("非法批次参数")
            config = Config(
                **{
                    "cdp": self.host.endpoint,
                    "operation_timeout": 15,
                    **(values or {}),
                }
            )
            path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            if path.exists():
                raise ValueError("同名批次目录已存在，请更换名称或打开原批次。")
            path.mkdir(mode=0o700, exist_ok=False)
        else:
            config = None
            if (
                not (path / "state.sqlite").is_file()
                or not (path / "run.json").is_file()
            ):
                raise ValueError("请选择已有批次目录，不会在此操作中创建新批次。")
        if self.store and self.store.root == path:
            return
        # Acquire the new store before releasing the current one.
        next_store = Workspace(path, config)
        if create:
            next_store.run.update(label=name or path.name, source_mode="workbench")
            atomic_write(path / "run.json", encode(next_store.run))
        if self.store:
            self.store.close()
        self.store = next_store
        self.active = self.last_scope = self.handoff = self.last_export = None
        self._discovery_start = None
        atomic_write(self.registry, encode({"batch": str(path)}))
        self.backend.tasks.clear()
        self.backend.files.clear()
        self.backend.last_input_result = None
        self.sync_download_tasks()
        self.log("已打开批次：" + path.name)

    def update_config(self, values, expected_revision=None):
        self.idle()
        store = self.require_store()
        if not isinstance(values, dict) or not values or set(values) - CONFIG_FIELDS:
            raise ValueError("非法批次参数")
        if expected_revision is not None and expected_revision != store.run.get("config_revision", 0):
            raise ValueError("配置已变化，请重新载入当前参数。")
        config = replace(store.config, **values)
        from uuid import uuid4
        revision = uuid4().hex
        before = dict(store.run)
        updated = {**before, "config": config.record(), "config_revision": revision}
        record = {"revision": revision, "changed_at": datetime.now().isoformat(),
                  "before": before, "after": updated}
        atomic_write(store.root / "config_revisions" / f"{revision}.json", encode(record))
        atomic_write(store.root / "run.json", encode(updated))
        try:
            store.save(None, None, "config_revision", encode(record))
        except Exception:
            atomic_write(store.root / "run.json", encode(before))
            raise
        store.config, store.run = config, updated
        self.log("当前批次参数已修订，仅后续操作生效；限制、冷却和已用预算均保留。")

    def pause(self):
        self.stopping = True
        if self.runner:
            self.runner.pause()
        if self.passive:
            self.passive.stop()
        if self.busy:
            self.log("安全暂停中：等待在途响应/当前下载作品收尾，不再领取下一项。")

    async def close(self):
        self.closing = True
        self.pause()
        async with self.lock:
            # A pending export / short-link resolution must finish or refuse on closing
            # before the single archive owner releases its database and file lock.
            if self.busy:
                try:
                    await asyncio.wait_for(asyncio.shield(self.job), 40)
                except asyncio.TimeoutError:
                    self.job.cancel()
                    try:
                        await self.job
                    except asyncio.CancelledError:
                        pass
            if self.store:
                self.store.close()
                self.store = None

    def media_settings(self):
        """Only non-secret, user-facing settings; never return Cookie in summaries."""
        manager = getattr(getattr(self.backend, "xhs", None), "manager", None)
        keys = ("image_download", "video_download", "video_cover_download", "live_download", "avg_delay")
        values = {k: getattr(manager, k, self.backend.settings.get(k)) for k in keys}
        folder = getattr(manager, "folder", None)
        values["folder"] = str(folder) if folder else None
        return values

    def operation_preview(self, payload, input_=False):
        kind, intent = payload.get("kind", "comments"), payload.get("intent", "start")
        if kind not in KINDS or intent not in INTENTS:
            raise ValueError("请选择评论或媒体，以及开始 / 继续 / 重试意图。")
        parsed = parse_input(payload.get("links", "")) if input_ else None
        ids = parsed["note_ids"] if input_ else self.require_store().validate_ids(payload.get("ids"))
        rows = self.store.operation_rows() if self.store else {}
        classified = [{"note_id": i, "category": category(rows.get(i), kind)} for i in ids]
        counts = {k: sum(r["category"] == k for r in classified) for k in CATEGORIES}
        eligible = [r["note_id"] for r in classified if r["category"] in ELIGIBLE[intent]]
        config = self.store.config.record() if self.store else Config(cdp=self.host.endpoint, operation_timeout=15).record()
        identity = [str(self.store.root), self.store.run.get("id"), self.store.run.get("config_revision", 0)] if self.store else None
        token = signature(["input-v2" if input_ else "selection-v2", identity, kind, intent,
                           payload.get("links") if input_ else ids, [rows.get(i) for i in ids],
                           config, self.backend.settings if kind == "download" else None])
        problem = ""
        if parsed:
            if parsed["short_links"]:
                problem = f"有 {len(parsed['short_links'])} 条短链接：请明确解析后重新预览，或改用完整链接。"
            elif parsed["invalid_count"]:
                problem = f"有 {parsed['invalid_count']} 条非完整笔记链接，请移除或修正后重新预览。"
            elif not ids:
                problem = "未识别到完整笔记链接；支持粘贴包含 HTTPS 链接的分享文字。"
        permission = self.permission(kind)
        return {"mode": kind, "intent": intent, "scope": "input" if input_ else "selected",
                "note_ids": ids, "eligible_ids": eligible, "count": len(eligible), "total": len(ids),
                "counts": counts, "items": classified, "token": token, "config": config,
                "media_settings": self.media_settings() if kind == "download" else None,
                "short_count": len(parsed["short_links"]) if parsed else 0,
                "invalid_count": parsed["invalid_count"] if parsed else 0,
                **permission, "can_start": permission["can_start"] and not problem,
                "reason": problem or permission["reason"]}

    def execute_operation(self, payload, input_=False):
        preview = self.operation_preview(payload, input_)
        if payload.get("preview_token") != preview["token"]:
            raise ValueError("作品、作业或配置已变化，请重新预览；不会扩大本次范围。")
        if not preview["can_start"]:
            raise ValueError(preview["reason"])
        if not preview["count"]:
            self.log("本次没有需要执行的作品：已有结果可直接查看，暂停或失败项需明确继续 / 重试。")
            return {"state": "nothing_to_do", **preview}
        store = self.ensure_store()
        if input_:
            imported = store.enqueue(parse_input(payload["links"])["urls"], {"mode": "manual"})
            self.handoff = {"kind": "input", **imported}
        ids, kind = preview["eligible_ids"], preview["mode"]
        added = store.submit(ids, kind, retry=preview["intent"] == "retry")
        wanted = set(ids)
        task_ids = [r["task_id"] for r in store.pending(kind) if r["note_id"] in wanted]
        try:
            self.start(kind, task_ids=task_ids)
        except Exception as exc:
            # Submission is durable. Do not pretend it rolled back, or delete evidence.
            reason = str(exc) if isinstance(exc, ValueError) else type(exc).__name__
            self.log("已加入待处理，尚未执行：" + reason + "；请刷新后预览继续。")
            self.sync_download_tasks()
            store.export()
            return {"state": "queued_not_started", "note_ids": ids, "task_ids": task_ids, "reason": reason}
        return {"state": "started", "note_ids": ids, "task_ids": task_ids, "added": len(added)}

    def discovery_handoff(self):
        if self._discovery_start is None or not self.store:
            return self.handoff
        after, before = self._discovery_start
        ids = list(dict.fromkeys(r["note_id"] for r in self.store.db.execute(
            "SELECT note_id FROM sources WHERE id>? ORDER BY id", (after,))))
        return {"kind": "discover", "note_ids": ids, "new_ids": [i for i in ids if i not in before],
                "existing_ids": [i for i in ids if i in before], "finished": not self.busy}

    def export_preview(self, payload):
        store = self.require_store()
        scope, format_ = payload.get("scope", "selected"), payload.get("format", "content")
        if scope not in {"selected", "filtered", "all"} or format_ not in {"summary", "content", "txt", "md", "links", "full_links"}:
            raise ValueError("请选择明确的导出范围和类型。")
        if scope == "selected":
            ids = store.validate_ids(payload.get("ids"))
        else:
            rows = self._database_snapshot()["all_rows"]
            if scope == "filtered":
                rows = filter_rows(rows, payload.get("query", ""), payload.get("filter", "all"),
                                   payload.get("source", ""), payload.get("view_ids"))
            ids = [r["note_id"] for r in rows]
        cutoff = store.db.execute("SELECT coalesce(max(id),0) FROM events").fetchone()[0]
        chosen = set(ids)
        token = signature(["export-v2", str(store.root), store.run["id"], scope, format_, ids, cutoff,
                           [r for r in self._database_snapshot()["all_rows"] if r["note_id"] in chosen]])
        return {"scope": scope, "format": format_, "note_ids": ids, "count": len(ids),
                "cutoff": cutoff, "token": token, "can_export": bool(ids) and (not self.busy or format_ == "summary"),
                "reason": "内容导出请先暂停并等待收尾，避免混合不同时间的结果。" if self.busy and format_ != "summary" else ""}

    def preview(self, mode):
        if mode not in {"comments", "download", "discover", "passive"}:
            raise ValueError("非法操作模式")
        scope = self.store.pending(mode) if self.store and mode in {"comments", "download"} else []
        resumed = 0
        if self.store:
            resumed = self.store.db.execute("SELECT count(*) FROM workspace_jobs WHERE kind=? AND state IN ('paused','interrupted')", (mode,)).fetchone()[0]
        rows = self.store.operation_rows() if self.store else {}
        token = signature(["queue-v2", str(self.store.root) if self.store else None, mode,
                           [(r["task_id"], rows.get(r["note_id"])) for r in scope],
                           self.store.config.record() if self.store else None,
                           self.store.run.get("config_revision", 0) if self.store else None,
                           self.backend.settings if mode == "download" else None])
        return {"mode": mode, "count": len(scope), "resumed": resumed,
                "note_ids": [r["note_id"] for r in scope], "token": token, **self.permission(mode)}

    def start(self, mode, scrolls=None, task_ids=None, preview_token=None):
        self.check_access(mode)
        if mode not in {"discover", "comments", "download", "passive"}:
            raise ValueError("非法操作模式")
        # Passive recording issues no requests, so it needs a batch but never a queue.
        queueless = mode in {"discover", "passive"}
        store = self.ensure_store() if queueless else self.require_store()
        if not queueless and not store.pending(mode):
            raise ValueError("队列为空：请先在链接池提交作品，或明确重试失败项。")
        if mode == "discover" and (
            not isinstance(scrolls, int) or not 0 <= scrolls <= 500
        ):
            raise ValueError("滚动次数应为 0–500；0 表示仅获取当前已加载链接。")
        scope = [] if queueless else store.pending(mode, task_ids)
        if not queueless and not scope:
            raise ValueError("本次执行范围为空，不会恢复其他任务。")
        if preview_token is not None and preview_token != self.preview(mode)["token"]:
            raise ValueError("队列已变化，请重新预览执行范围。")
        self.stopping, self.mode = False, mode
        self.active = {"mode": mode, "note_ids": [r["note_id"] for r in scope], "count": len(scope),
                       "index": 0, "current": None, "stage": "preparing"}
        if mode == "discover":
            self._discovery_start = (store.db.execute("SELECT coalesce(max(id),0) FROM sources").fetchone()[0],
                                     {r["note_id"] for r in store.tasks()})
        self.job = asyncio.create_task(self._run(mode, scope, scrolls))

    async def _run(self, mode, scope, scrolls):
        from playwright.async_api import async_playwright
        from ..collection.runner import Runner, Paused
        from ..collection.pacing import Pacer, PacingPause
        from .backend import TaskState

        store = self.store
        self.runner = (
            Runner(store, host=self.host) if mode not in {"download", "passive"} else None
        )
        if self.runner:
            self.runner.config = replace(
                store.config,
                cdp=self.host.endpoint,
                media=True,
                comments=mode == "comments",
                max_scrolls=scrolls or 0,
            )
            # Batch-wide pacing shared with the CLI runner (persisted in controls).
            self.runner.pacer = Pacer(store)
        current = None
        structural_errors = 0
        try:
            self.log(
                {
                    "discover": "正在发现；链接直接保存，不会自动采评论。",
                    "comments": "开始处理选定评论队列。",
                    "download": "开始处理选定媒体队列。",
                    "passive": "被动录制：只存档你手动浏览产生的响应，不注入脚本、不主动请求。",
                }[mode]
            )
            pacer = self.runner.pacer if self.runner else Pacer(store)
            if mode != "passive":
                pacer.check_cooldown()  # Refuse before connecting to any browser.
            async with AsyncExitStack() as stack:
                context = None
                if self.runner or mode == "passive":
                    p = await stack.enter_async_context(async_playwright())
                    if self.runner:
                        _, context = await self.runner.connect(p)
                    else:
                        browser = await self.host.connect(p)
                        context = browser[1]
                        store.control("active_cdp", self.host.endpoint)
                if self.stopping:
                    raise Paused("user_pause")
                if self.runner:
                    try:
                        # Acknowledging a restriction does not shorten the cooldown.
                        self.runner.pacer.check_cooldown()
                    except PacingPause as exc:
                        minutes = max(1, round((exc.until - time.time()) / 60))
                        raise Paused(f"{exc.reason}_cooldown_{minutes}min") from None
                if mode == "passive":
                    await self._record(context)
                elif mode == "discover":
                    await self.runner.discover(context)
                else:
                    for index, task in enumerate(scope, 1):
                        if self.blocked:
                            raise Paused("access_restricted")
                        if self.stopping:
                            raise Paused("user_pause")
                        current = task["note_id"]
                        self.active.update(index=index, current=current, stage="processing")
                        job_state = store.db.execute(
                            "SELECT state FROM workspace_jobs WHERE note_id=? AND kind=?",
                            (current, mode),
                        ).fetchone()[0]
                        if job_state == "cancelled":
                            current = None
                            continue
                        store.job_state(current, mode, "running")
                        self.sync_download_tasks()
                        self.log(f"{index}/{len(scope)}：处理 {current}")
                        if mode == "comments":
                            result = await self.runner.collect(context, task)
                            if (
                                self.runner.config.strategy == "auto"
                                and result["reason"] == "adapter_unavailable"
                            ):
                                result = await self.runner.collect(
                                    context, task, strategy="ui"
                                )
                            state = (
                                "success"
                                if result["comments"] == "complete_observed"
                                else "partial"
                            )
                            store.job_state(current, mode, state, result["reason"])
                            structural_errors = (
                                structural_errors + 1
                                if result["body"] == "partial"
                                else 0
                            )
                            if structural_errors >= 3:
                                raise Paused("consecutive_page_failures")
                        else:
                            def checkpoint():
                                if self.stopping:
                                    raise Paused("user_pause")
                            await pacer.wait(checkpoint)
                            pacer.record()
                            # Media uses the upstream downloader, with one budgeted action per note.
                            note = store.note_object(current)
                            if self.stopping:
                                raise Paused("user_pause")
                            download = TaskState(
                                task["task_id"],
                                task["url"],
                                source="workbench",
                                script_data=note,
                                display_text=current,
                            )
                            self.backend.tasks[download.task_id] = download
                            async with self.backend.download_lock:
                                await self.backend._process_task(download)
                            state = download.state
                            media = "success" if state == "success" else "skipped" if state == "skipped" else "failed"
                            reason = download.reason or ("download_completed_unverified" if media == "success" else "skip_unknown" if media == "skipped" else state)
                            store.save(None, current, "media_result", encode({
                                "task_id": task["task_id"], "state": state, "reason": reason,
                                "evidence": download.evidence,
                            }))
                            with store.db:
                                store.db.execute(
                                    "UPDATE tasks SET media=? WHERE note_id=?",
                                    (media, current),
                                )
                            store.job_state(current, mode, media, reason)
                            if media == "failed":
                                raise ValueError(
                                    "下载未成功，已保守停止后续作品；请先检查失败原因。"
                                )
                        current = None
            if mode != "passive":
                self.log(
                    "本次发现结束，结果已进入链接池。"
                    if mode == "discover"
                    else "本次固定队列已处理结束，请查看各项状态。"
                )
        except AccessRestricted as exc:
            self.restrict(exc.reason)
            if current:
                store.job_state(current, mode, "paused", exc.reason)
        except PacingPause as exc:
            if current:
                store.job_state(current, mode, "paused", exc.reason)
            self.log("已暂停：" + exc.reason + "；休息结束后需明确继续。")
        except Paused as exc:
            reason = str(exc)
            if reason in {"access_restricted", "login_or_verification_required"}:
                self.restrict(reason)
            if current:
                store.job_state(current, mode, "paused", reason)
            self.log("已暂停：" + reason)
        except asyncio.CancelledError:
            if current:
                store.job_state(current, mode, "interrupted", "shutdown")
            self.log("程序关闭，未完成任务已保留；不会自动续跑。")
            raise
        except Exception as exc:
            if current:
                store.job_state(current, mode, "failed", type(exc).__name__)
            self.log(
                "操作已停止："
                + (str(exc) if isinstance(exc, ValueError) else type(exc).__name__)
            )
        finally:
            if mode == "discover":
                self.handoff = {**self.discovery_handoff(), "finished": True}
                self._discovery_start = None
            if self.active:
                self.last_scope = {**self.active, "stage": "stopped" if self.stopping else "finished"}
            self.active = None
            self.runner = None
            self.passive = None
            self.mode = None
            self.sync_download_tasks()
            store.export()

    async def _record(self, context):
        """Archive manual browsing. Issues no requests, so pacing never applies."""
        from ..collection.passive import PassiveRecorder

        recorder = PassiveRecorder(self.store, self.host.endpoint, on_event=self.log,
                                   on_restriction=self.restrict)
        self.passive = recorder
        before = {r["note_id"] for r in self.store.tasks()}
        try:
            await recorder.run(status_interval=1.0, context=context)
        finally:
            self.passive = None
            ids = sorted(set(recorder.session.discovered) | set(recorder.session.coverage))
            ids = [i for i in ids if re.fullmatch(r"[a-f0-9]{24}", i)]
            # Register observed-only notes after their raw responses are durable. No fetching.
            self.store.enqueue([f"https://www.xiaohongshu.com/explore/{i}" for i in ids if i not in before],
                               {"mode": "passive_observation", "session_id": recorder.session_id})
            self.handoff = {"kind": "passive", "note_ids": ids, "new_ids": [i for i in ids if i not in before],
                            "existing_ids": [i for i in ids if i in before], "finished": True}
        report = recorder.summary()
        self.log(
            f"录制结束：清单 {report['discovered']} 篇 · 已见评论 {report['comments_total']} 条 ·"
            f" 报告 passive_report.json"
        )

    def _database_snapshot(self):
        # All accesses run on the backend loop. Invalidate for writes through this
        # connection (including Runner) and commits through other connections.
        store = self.store
        key = (
            store,
            store.db.total_changes,
            store.db.execute("PRAGMA data_version").fetchone()[0],
        ) if store else (None,)
        if key == self._snapshot_key:
            return self._snapshot_data
        rows = store.overview() if store else []
        progress = {"primary": 0, "replies": 0}
        progress_context = None
        queues = {"comments": 0, "download": 0}
        queue_states = {kind: {} for kind in KINDS}
        if store:
            row = store.db.execute(
                "SELECT p.data,p.attempt,a.note_id FROM progress p LEFT JOIN attempts a ON a.id=p.attempt ORDER BY p.rowid DESC LIMIT 1"
            ).fetchone()
            if row:
                progress_context = {"note_id": row["note_id"], "attempt": row["attempt"]}
                ledger = json.loads(row[0])
                progress = {
                    "primary": len(ledger["primary"]["ids"]),
                    "replies": sum(len(c["ids"]) for c in ledger["replies"].values()),
                }
            for kind, count in store.db.execute("""SELECT j.kind,count(*)
                FROM workspace_jobs j JOIN workspace_items w USING(note_id)
                WHERE j.state IN ('pending','interrupted','paused') AND w.hidden=0
                GROUP BY j.kind"""):
                if kind in queues:
                    queues[kind] = count
            for kind, state, count in store.db.execute("""SELECT j.kind,j.state,count(*) FROM workspace_jobs j
                JOIN workspace_items w USING(note_id) WHERE w.hidden=0 GROUP BY j.kind,j.state"""):
                if kind in queue_states:
                    queue_states[kind][state] = count
        data = {
            "all_rows": rows,
            "counts": {
                "total": len(rows),
                "complete": sum(r["comments"] == "complete_observed" for r in rows),
                "partial": sum(r["comments"] in {"partial", "blocked", "failed"} for r in rows),
                "downloaded": sum(r["media"] == "success" for r in rows),
            },
            "sources": sorted({s for r in rows for s in r["sources"]}),
            "progress": progress,
            "progress_context": progress_context,
            "queues": queues,
            "queue_states": queue_states,
        }
        self._snapshot_key, self._snapshot_data = key, data
        return data

    def status_text(self, data):
        if self.closing:
            return "正在安全关闭，等待保存收尾"
        if not self.backend.settings.get("disclaimer_accepted"):
            return "请先完成首次使用确认"
        if self.busy and self.stopping:
            return "平台要求暂停，正在保存已收到内容" if self.blocked else "正在暂停并保存最后响应，请等待"
        if self.busy:
            if self.mode == "passive":
                report = self.passive.summary() if self.passive else {}
                return (f"正在记录浏览：监听 {report['pages_ready']} 个标签页" if report.get("pages_ready")
                        else "正在准备监听，请等就绪后再浏览")
            return {"comments": "正在采集正文与评论，按设定节奏运行", "download": "正在下载媒体，平均等待不是卡住",
                    "discover": "正在收集作品链接，不会自动采评论"}.get(self.mode, "后台正在运行")
        if self.blocked:
            return "自动操作已停止，需要人工处理访问限制"
        permission = self.permission("comments")
        if permission["until"] > time.time():
            return "冷却 / 预算休息至 " + time.strftime("%m-%d %H:%M", time.localtime(permission["until"])) + "，不会自动开始"
        queues = data["queues"]
        if queues["comments"] or queues["download"]:
            return f"尚有评论 {queues['comments']} 篇、媒体 {queues['download']} 篇未完成；未在后台执行"
        retries = sum(q.get(s, 0) for q in data["queue_states"].values() for s in ("partial", "failed", "skipped"))
        if retries:
            return f"有 {retries} 项部分 / 失败 / 跳过结果，请查看后明确补采或重试"
        return "可以添加或选择作品；已有结果可直接查看和导出"

    def summary(self):
        """Small shell payload: no table rows, source list, ledger or log history."""
        data = self._database_snapshot()
        return {
            "phase_text": self.status_text(data),
            "disclaimer_accepted": bool(self.backend.settings.get("disclaimer_accepted")),
            "batch": str(self.store.root) if self.store else None,
            "name": self.store.run.get("label", self.store.root.name)
            if self.store else "默认工作区（按需创建）",
            "busy": self.busy,
            "stopping": self.busy and self.stopping,
            "closing": self.closing,
            "active": dict(self.active) if self.active else None,
            "last_scope": self.last_scope,
            "handoff": self.discovery_handoff(),
            "media_settings": self.media_settings(),
            "mode": self.mode,
            "access_reason": self.access_reason,
            "blocked": self.blocked,
            "message": ("平台访问已暂停：" + self.access_reason + "；不会自动重试。")
            if self.access_reason else self.message,
            "counts": data["counts"],
            "queues": data["queues"],
            "queue_states": data["queue_states"],
            "permissions": {mode: self.permission(mode) for mode in ("discover", "comments", "download", "passive")},
        }

    def snapshot(self, query="", filter_="all", page=1, source="", ids=None):
        data = self._database_snapshot()
        if ids is not None and (not isinstance(ids, list) or len(ids) > 50000 or any(not isinstance(i, str) for i in ids)):
            raise ValueError("查看范围应为不超过 50000 项的 ID 列表；执行仍须每次选择至多 5000 篇。")
        filtered = filter_rows(data["all_rows"], query, filter_, source, ids)
        page = max(1, min(int(page), max(1, (len(filtered) + 49) // 50)))
        return {
            **self.summary(),
            "config": self.store.config.record() if self.store else {},
            "config_revision": self.store.run.get("config_revision", 0) if self.store else 0,
            "sources": data["sources"],
            "rows": filtered[(page - 1) * 50 : page * 50],
            "page": page,
            "filtered": len(filtered),
            "logs": list(self.logs),
            "progress": data["progress"],
            "progress_context": data["progress_context"],
            # Read-only visibility into what is happening right now.
            "passive": self.passive.summary() if self.passive else None,
            "pacing": self.pacing_state(),
            "observed": json.loads(self.store.control("passive_summary") or "{}") if self.store else {},
        }

    def pacing_state(self):
        if not self.store:
            return None
        from ..collection.pacing import Pacer
        state = Pacer(self.store).state()
        state["cooldown_until"] = max(state["cooldown_until"], self.access_cooldown)
        return state

    async def command(self, action, payload=None):
        payload = payload or {}
        if not isinstance(payload, dict):
            raise ValueError("参数必须是对象")
        if action == "report_restriction":
            if payload.get("reason") not in {
                "account_abnormal",
                "verification_required",
            }:
                raise ValueError("未知限制信号")
            self.restrict(payload["reason"])
            return self.snapshot()
        if action == "batches":
            return CollectionService(self.root).batches()
        if action == "summary":
            return self.summary()
        if action == "state":
            return self.snapshot(
                payload.get("query", ""),
                payload.get("filter", "all"),
                payload.get("page", 1),
                payload.get("source", ""),
                payload.get("ids"),
            )
        if action in {"preview_selection", "preview_input"}:
            return self.operation_preview(payload, input_=action == "preview_input")
        if action == "hide_preview":
            return self.require_store().hide_preview(payload.get("ids"))
        if action == "export_preview":
            return self.export_preview(payload)
        if action == "detail":
            from ..collection.results import read_detail
            store = self.require_store()
            id_ = store.validate_ids([payload.get("id")])[0]
            return await asyncio.to_thread(read_detail, store.root, id_, payload.get("page", 1), payload.get("source", "current"))
        if action == "preview":
            return self.preview(payload.get("mode"))
        if action == "pause":
            self.pause()
            return self.snapshot()
        if self.lock.locked():
            raise ValueError("另一个操作正在处理，请稍候。")
        async with self.lock:
            if not self.backend.settings.get("disclaimer_accepted"):
                raise ValueError("请先完成首次使用确认。")
            if self.closing:
                raise ValueError("正在安全关闭，请等待收尾。")
            if action in {"execute_selection", "execute_input"}:
                operation = self.execute_operation(payload, input_=action == "execute_input")
                return {**self.snapshot(), "operation": operation}
            if action == "resolve_input":
                if payload.get("kind", "comments") not in KINDS:
                    raise ValueError("短链接解析仅用于明确的评论或媒体输入。")
                self.check_access(payload.get("kind", "comments"))
                parsed = parse_input(payload.get("links", ""))
                if parsed["invalid_count"]:
                    raise ValueError("请先移除非笔记链接，再解析短链接。")
                if not parsed["short_links"]:
                    return {"links": "\n".join(parsed["urls"]), "resolved": 0}
                xhs = getattr(self.backend, "xhs", None)
                if not xhs:
                    raise ValueError("下载器尚未就绪，不能解析短链接。")
                resolved = await xhs.extract_links("\n".join(parsed["short_links"]))
                self.check_access(payload.get("kind", "comments"))
                verified = CollectionService.links("\n".join(resolved))
                if len(verified) != len(parsed["short_links"]):
                    raise ValueError("部分短链接无法解析，请改用完整笔记链接后重新预览。")
                return {"links": "\n".join(parsed["urls"] + verified), "resolved": len(verified)}
            if action == "export_content":
                from ..collection.results import export_archive
                preview = self.export_preview(payload)
                if preview["token"] != payload.get("preview_token"):
                    raise ValueError("导出范围或证据已变化，请重新预览。")
                if not preview["can_export"]:
                    raise ValueError(preview["reason"] or "导出范围为空。")
                self.last_export = await asyncio.to_thread(export_archive, self.store.root, preview["note_ids"],
                                                          preview["format"], preview["cutoff"])
                self.log(f"已离线导出 {preview['count']} 篇的已存资料；未请求平台。")
                return {**self.snapshot(), "export_result": self.last_export}
            if action == "create":
                name = str(payload.get("name", "")).strip() or datetime.now().strftime(
                    "采集-%Y%m%d-%H%M%S"
                )
                if not re.fullmatch(r"[\w\- ·]{1,80}", name):
                    raise ValueError(
                        "批次名仅允许中英文、数字、空格、短横线或下划线，最长80字。"
                    )
                self.select(
                    self.root / "collections" / name,
                    create=True,
                    name=name,
                    values=payload.get("config"),
                )
            elif action == "update_config":
                self.update_config(payload.get("config"), payload.get("expected_revision"))
            elif action == "load":
                self.select(payload.get("path", ""))
            elif action == "import":
                self.idle()
                parsed = parse_input(payload.get("links", ""))
                if not parsed["urls"] or parsed["short_links"] or parsed["invalid_count"]:
                    raise ValueError("请提供完整 HTTPS 笔记链接或分享文字；短链接需先明确解析。")
                imported = self.ensure_store().enqueue(parsed["urls"], {"mode": "manual"})
                self.handoff = {"kind": "input", **imported}
                self.log(f"本次识别 {len(imported['note_ids'])} 篇：新加入 {len(imported['new_ids'])} 篇，已有 {len(imported['existing_ids'])} 篇；尚未开始采集。")
            elif action == "submit":
                self.idle()
                added = self.require_store().submit(
                    payload.get("ids"),
                    payload.get("kind"),
                    bool(payload.get("retry", False)),
                )
                self.log(f"本次新增 {len(added)} 项；已排队/成功项未重复提交。")
            elif action == "hide":
                self.idle()
                preview = self.require_store().hide_preview(payload.get("ids"))
                if payload.get("preview_token") is not None and payload["preview_token"] != preview["token"]:
                    raise ValueError("隐藏影响的作业已变化，请重新确认。")
                self.store.hide(payload.get("ids"))
                self.log(f"已隐藏 {preview['count']} 篇并取消 {preview['cancelled_jobs']} 项未执行作业；原始存档保留。")
            elif action == "restore_hidden":
                self.idle()
                count = self.require_store().restore_hidden()
                self.log(f"已恢复 {count} 篇隐藏作品；不会自动恢复已取消的队列。")
            elif action == "start":
                self.start(payload.get("mode"), payload.get("scrolls", 0), preview_token=payload.get("preview_token"))
            elif action == "ack_restriction":
                self.idle()
                atomic_write(self.access_path, encode({"reason": "", "cooldown_until": self.access_cooldown}))
                self.access_reason = ""
                if self.store:
                    self.store.control("workspace_blocked", "")
                    self.store.control("workspace_restriction_ack", "yes")
                if getattr(self.backend, "xhs", None):
                    self.backend.xhs.html.access_restriction = None
                self.log("已记录人工恢复确认；尚未重试，请明确选择下一步操作。")
            elif action in {"copy", "export_links"}:
                links = self.require_store().links_for(payload.get("ids"))
                text = "\n".join(links)
                if action == "copy":
                    await self.host.native("clipboard", text)
                else:
                    atomic_write(self.store.root / "links.txt", (text + "\n").encode())
                self.log(
                    f"已{'复制' if action == 'copy' else '导出'} {len(links)} 条完整链接。"
                )
            elif action == "export":
                store = self.require_store()
                output = io.StringIO(newline="")
                fields = ["note_id", "title", "body", "comments", "media", "reason"]
                writer = csv.writer(output)
                writer.writerow(fields)
                for row in store.overview():
                    values = [str(row.get(k) or "") for k in fields]
                    writer.writerow(
                        [
                            "'" + v if v.startswith(("=", "+", "-", "@")) else v
                            for v in values
                        ]
                    )
                atomic_write(
                    store.root / "summary.csv", output.getvalue().encode("utf-8-sig")
                )
            elif action == "folder":
                await self.host.native("folder", str(self.require_store().root))
            elif action == "export_folder":
                path = Path(self.last_export["folder"]) if self.last_export else self.require_store().root / "exports"
                if not path.is_dir():
                    raise ValueError("尚未生成导出文件。")
                await self.host.native("folder", str(path))
            else:
                raise ValueError("不支持的工作台操作")
            if self.store and not self.busy:
                self.sync_download_tasks()
                self.store.export()
            return self.snapshot()
