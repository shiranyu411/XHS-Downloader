"""One batch owner, independent durable queues, conservative serial execution."""

import asyncio
from dataclasses import replace
from datetime import datetime
import csv
import io
import json
from pathlib import Path
import re
from collections import deque
from contextlib import AsyncExitStack
from ..module.access import AccessRestricted

from ..collection.config import Config
from ..collection.store import atomic_write, encode
from ..collection.workspace import Workspace
from ..collection.service import CollectionService


class WorkbenchService:
    def __init__(self, root, backend, host):
        self.root, self.backend, self.host = Path(root), backend, host
        self.registry = self.root / ".workbench-ui.json"
        self.access_path = self.root / ".workbench-access.json"
        self.access_reason = ""
        try:
            self.access_reason = str(
                json.loads(self.access_path.read_text()).get("reason") or ""
            )
        except FileNotFoundError:
            pass
        except (OSError, ValueError, AttributeError):
            self.access_reason = "access_state_unreadable"
        self.store = None
        self.job = None
        self.runner = None
        self.mode = None
        self.stopping = False
        self.message = "可直接粘贴链接下载；需要归档时再选择批次。"
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

        for row in self.store.db.execute("""SELECT t.note_id,t.url,j.task_id,j.state FROM workspace_jobs j
                JOIN tasks t USING(note_id) WHERE j.kind='download'"""):
            state = {
                "running": "processing",
                "interrupted": "pending",
                "paused": "pending",
                "partial": "failed",
            }.get(row["state"], row["state"])
            task = self.backend.tasks.get(row["task_id"])
            if task:
                task.state = state
            else:
                self.backend.tasks[row["task_id"]] = TaskState(
                    row["task_id"],
                    row["url"],
                    source="workbench",
                    state=state,
                    display_text=row["note_id"],
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
        atomic_write(self.access_path, encode({"reason": reason}))
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

    def check_access(self):
        self.idle()
        if self.blocked:
            raise ValueError(
                "平台访问限制尚未人工确认恢复，不会自动重试或更换下载路径。"
            )

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
                or set(values)
                - {"open_interval", "page_interval", "max_operations", "note_timeout"}
            ):
                raise ValueError("非法批次参数")
            config = Config(
                **{
                    "cdp": self.host.endpoint,
                    "page_interval": 3,
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
        atomic_write(self.registry, encode({"batch": str(path)}))
        self.backend.tasks.clear()
        self.backend.files.clear()
        self.sync_download_tasks()
        self.log("已打开批次：" + path.name)

    def pause(self):
        self.stopping = True
        if self.runner:
            self.runner.pause()
        if self.busy:
            self.log("安全暂停中：等待在途响应/当前下载作品收尾，不再领取下一项。")

    async def close(self):
        self.pause()
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

    def start(self, mode, scrolls=None):
        self.check_access()
        store = self.ensure_store() if mode == "discover" else self.require_store()
        if mode not in {"discover", "comments", "download"}:
            raise ValueError("非法操作模式")
        if mode != "discover" and not store.pending(mode):
            raise ValueError("队列为空：请先在链接池提交作品，或明确重试失败项。")
        if mode == "discover" and (
            not isinstance(scrolls, int) or not 0 <= scrolls <= 500
        ):
            raise ValueError("滚动次数应为 0–500；0 表示仅获取当前已加载链接。")
        self.stopping, self.mode = False, mode
        scope = store.pending(mode) if mode != "discover" else []
        self.job = asyncio.create_task(self._run(mode, scope, scrolls))

    async def _run(self, mode, scope, scrolls):
        from playwright.async_api import async_playwright
        from ..collection.runner import Runner, Paused
        from .backend import TaskState

        store = self.store
        self.runner = Runner(store, host=self.host) if mode != "download" else None
        if self.runner:
            self.runner.config = replace(
                store.config,
                cdp=self.host.endpoint,
                media=False,
                comments=mode == "comments",
                max_scrolls=scrolls or 0,
            )
        current = None
        structural_errors = 0
        try:
            self.log(
                {
                    "discover": "正在发现；链接直接保存，不会自动采评论。",
                    "comments": "开始处理选定评论队列。",
                    "download": "开始处理选定媒体队列。",
                }[mode]
            )
            async with AsyncExitStack() as stack:
                context = None
                if self.runner:
                    p = await stack.enter_async_context(async_playwright())
                    _, context = await self.runner.connect(p)
                if self.stopping:
                    raise Paused("user_pause")
                if mode == "discover":
                    await self.runner.discover(context)
                else:
                    for index, task in enumerate(scope, 1):
                        if self.blocked:
                            raise Paused("access_restricted")
                        if self.stopping:
                            raise Paused("user_pause")
                        current = task["note_id"]
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
                            # Restore the upstream media-only path. No Playwright connection,
                            # controlled page, login requirement or body collection is needed.
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
                            media = (
                                "success"
                                if state in {"success", "skipped"}
                                else "failed"
                            )
                            with store.db:
                                store.db.execute(
                                    "UPDATE tasks SET media=? WHERE note_id=?",
                                    (media, current),
                                )
                            store.job_state(
                                current,
                                mode,
                                "success" if media == "success" else "failed",
                                state,
                            )
                            if media != "success":
                                raise ValueError(
                                    "下载未成功，已保守停止后续作品；请先检查失败原因。"
                                )
                        current = None
            self.log(
                "本次发现结束，结果已进入链接池。"
                if mode == "discover"
                else "本次固定队列已处理结束，请查看各项状态。"
            )
        except AccessRestricted as exc:
            self.restrict(exc.reason)
            if current:
                store.job_state(current, mode, "paused", exc.reason)
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
            self.runner = None
            self.mode = None
            self.sync_download_tasks()
            store.export()

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
        queues = {"comments": 0, "download": 0}
        if store:
            row = store.db.execute(
                "SELECT data FROM progress ORDER BY rowid DESC LIMIT 1"
            ).fetchone()
            if row:
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
            "queues": queues,
        }
        self._snapshot_key, self._snapshot_data = key, data
        return data

    def summary(self):
        """Small shell payload: no table rows, source list, ledger or log history."""
        data = self._database_snapshot()
        return {
            "disclaimer_accepted": bool(self.backend.settings.get("disclaimer_accepted")),
            "batch": str(self.store.root) if self.store else None,
            "name": self.store.run.get("label", self.store.root.name)
            if self.store else "默认工作区（按需创建）",
            "busy": self.busy,
            "mode": self.mode,
            "access_reason": self.access_reason,
            "blocked": self.blocked,
            "message": ("平台访问已暂停：" + self.access_reason + "；不会自动重试。")
            if self.access_reason else self.message,
            "counts": data["counts"],
            "queues": data["queues"],
        }

    def snapshot(self, query="", filter_="all", page=1, source=""):
        data = self._database_snapshot()
        needle = str(query).casefold()
        filtered = [
            r for r in data["all_rows"]
            if needle in (r["note_id"] + " " + r["title"]).casefold()
        ]
        if source:
            filtered = [r for r in filtered if source in r["sources"]]
        if filter_ == "pending":
            filtered = [r for r in filtered if r["comments"] == "not_requested"]
        elif filter_ == "partial":
            filtered = [
                r for r in filtered if r["comments"] in {"partial", "blocked", "failed"}
            ]
        elif filter_ == "complete":
            filtered = [r for r in filtered if r["comments"] == "complete_observed"]
        elif filter_ == "downloaded":
            filtered = [r for r in filtered if r["media"] == "success"]
        page = max(1, min(int(page), max(1, (len(filtered) + 49) // 50)))
        return {
            **self.summary(),
            "config": self.store.config.record() if self.store else {},
            "sources": data["sources"],
            "rows": filtered[(page - 1) * 50 : page * 50],
            "page": page,
            "filtered": len(filtered),
            "logs": list(self.logs),
            "progress": data["progress"],
        }

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
            )
        if action == "pause":
            self.pause()
            return self.snapshot()
        if self.lock.locked():
            raise ValueError("另一个操作正在处理，请稍候。")
        async with self.lock:
            if not self.backend.settings.get("disclaimer_accepted"):
                raise ValueError("请先在作品下载页完成首次使用确认。")
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
            elif action == "load":
                self.select(payload.get("path", ""))
            elif action == "import":
                self.idle()
                self.ensure_store().enqueue(
                    CollectionService.links(payload.get("links", "")),
                    {"mode": "manual"},
                )
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
                self.require_store().hide(payload.get("ids"))
            elif action == "restore_hidden":
                self.idle()
                count = self.require_store().restore_hidden()
                self.log(f"已恢复 {count} 篇隐藏作品；不会自动恢复已取消的队列。")
            elif action == "start":
                self.start(payload.get("mode"), payload.get("scrolls", 0))
            elif action == "ack_restriction":
                self.idle()
                atomic_write(self.access_path, encode({"reason": ""}))
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
            else:
                raise ValueError("不支持的工作台操作")
            if self.store and not self.busy:
                self.sync_download_tasks()
                self.store.export()
            return self.snapshot()
