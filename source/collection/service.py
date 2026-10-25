"""GUI controller. All Store/Runner access stays on the GUI's asyncio thread."""
import asyncio
from collections import Counter, deque
import csv
from datetime import datetime
import io
import json
from pathlib import Path
import re
import sqlite3
from contextlib import suppress
from urllib.parse import urlencode, urlsplit

from .config import Config, note_id
from .store import Store, atomic_write, encode


class CollectionService:
    def __init__(self, root, media=None):
        self.root = Path(root).resolve()
        self.registry_path = self.root / ".collection-ui.json"
        self.selected = None
        self.recent = []
        self.store = None
        self.runner = None
        self.job = None
        self.media = media
        self.phase = "idle"
        self.message = "先打开专用浏览器，登录并搜索关键词。"
        self.logs = deque(maxlen=40)
        self.connection = {"status": "unknown", "message": "尚未检查浏览器", "pages": []}
        self.endpoint = "http://127.0.0.1:9223"
        self._report_cache = None
        self._last_runner_message = None
        self.command_lock = asyncio.Lock()
        try:
            saved = json.loads(self.registry_path.read_text())
            self.recent = [p for p in saved.get("recent", []) if isinstance(p, str)][:20]
            selected = Path(saved.get("selected", ""))
            if (selected / "run.json").is_file():
                self.selected = selected.resolve()
                self.message = "已恢复上次批次；点击继续即可处理未完成任务。"
        except (OSError, ValueError, TypeError):
            pass

    @property
    def busy(self):
        return self.job is not None and not self.job.done()

    def _idle(self):
        if self.busy:
            raise ValueError("采集正在运行，请先暂停并等待收尾完成。")

    def _log(self, message, level="info"):
        self.logs.append({"time": datetime.now().strftime("%H:%M:%S"), "message": message, "level": level})

    def _remember(self):
        path = str(self.selected)
        self.recent = [path] + [p for p in self.recent if p != path]
        self.recent = self.recent[:20]
        atomic_write(self.registry_path, encode({"selected": path, "recent": self.recent}))
        self._report_cache = None

    def batches(self):
        paths = list(self.recent)
        base = self.root / "collections"
        if base.is_dir():
            paths += [str(p.parent) for p in sorted(base.glob("*/run.json"), key=lambda p: p.stat().st_mtime, reverse=True)[:50]]
        result = []
        for path in dict.fromkeys(paths):
            try:
                data = json.loads((Path(path) / "run.json").read_text())
                result.append({"path": path, "name": data.get("label") or Path(path).name, "created": data.get("created")})
            except (OSError, ValueError, TypeError):
                continue
        return result

    def load(self, path):
        self._idle()
        selected = Path(path).expanduser().resolve()
        if not (selected / "run.json").is_file() or not (selected / "state.sqlite").is_file():
            raise ValueError("请选择包含 run.json 和 state.sqlite 的采集批次目录。")
        data = json.loads((selected / "run.json").read_text())
        Config(**{k: v for k, v in data["config"].items() if k in Config.__dataclass_fields__})
        self.selected = selected
        self.phase = "ready"
        self.message = "已打开批次。继续会恢复未完成任务；补采会重试不完整结果。"
        self._remember()
        self._log("已打开批次：" + selected.name)

    @staticmethod
    def links(content):
        if not isinstance(content, str) or len(content) > 2_000_000:
            raise ValueError("链接内容过长，请分批提交。")
        urls = list(dict.fromkeys(line.strip() for line in content.splitlines() if line.strip()))
        if len(urls) > 5000:
            raise ValueError("一次最多添加 5000 个链接，请分批提交。")
        for url in urls:
            note_id(url)
        return urls

    async def create_start(self, payload):
        self._idle()
        name = str(payload.get("name", "")).strip()
        if not name:
            name = "采集-" + datetime.now().strftime("%Y%m%d-%H%M%S")
        if len(name) > 80 or not re.fullmatch(r"[\w\- .·\u4e00-\u9fff]+", name) or name in {".", ".."} or name.endswith("."):
            raise ValueError("批次名请使用 1–80 个中英文、数字、空格、短横线或下划线。")
        source = payload.get("source", "search")
        if source not in {"search", "links"}:
            raise ValueError("请选择当前搜索页或粘贴链接。")
        urls = self.links(payload.get("links", "")) if source == "links" else []
        if source == "links" and not urls:
            raise ValueError("请粘贴完整笔记链接，每行一条。")
        values = payload.get("config", {})
        if not isinstance(values, dict):
            raise ValueError("采集参数格式错误。")
        config = Config(**{k: v for k, v in values.items() if k in Config.__dataclass_fields__})
        await self._preflight(config.cdp, source == "search")
        parent = Path(payload.get("output") or self.root / "collections").expanduser().resolve()
        parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        path = parent / name
        if path.exists():
            raise ValueError("同名目录已经存在。请换一个批次名，或打开原批次继续。")
        # Reserve the directory, never reinterpret an existing unrelated folder.
        path.mkdir(parents=True, mode=0o700, exist_ok=False)
        store = Store(path, config)
        try:
            store.run.update(label=name, source_mode=source)
            atomic_write(path / "run.json", encode(store.run))
            if urls:
                store.enqueue(urls, {"mode": "gui_links"})
        finally:
            store.close()
        self.selected = path
        self._remember()
        await self.start("discover" if source == "search" else "resume", preflight=False)

    async def _preflight(self, endpoint, discover):
        connection = await self.check_browser(endpoint)
        if connection["status"] != "connected":
            raise ValueError(connection["message"])
        if any(p.get("logged_in") is False or p.get("blocked") for p in connection["pages"]):
            raise ValueError("请先在专用 Chrome 完成登录或验证，再回到这里开始。")
        if discover and sum(p["discovery"] for p in connection["pages"]) != 1:
            raise ValueError("请在专用 Chrome 只保留一个搜索结果、作者或推荐页面；也可使用“搜索”按钮。")

    async def start(self, mode="resume", preflight=True):
        self._idle()
        if self.selected is None:
            raise ValueError("请先创建或打开一个批次。")
        if mode not in {"resume", "retry", "discover"}:
            raise ValueError("不支持的采集操作。")
        from .runner import Runner

        config_data = json.loads((self.selected / "run.json").read_text())["config"]
        config = Config(**{k: v for k, v in config_data.items() if k in Config.__dataclass_fields__})
        if preflight:
            await self._preflight(config.cdp, mode == "discover")
        store = Store(self.selected)
        try:
            discover = mode == "discover" or (mode == "resume" and store.control("discovery_status") in {"running", "paused"})
            self.runner = Runner(store, self.media)
            self.store = store
            self.phase = "running"
            self.message = "准备采集；浏览器中的登录会话不会被导出。"
            self._last_runner_message = None
            self._log("开始批次：" + self.selected.name)
            self.job = asyncio.create_task(self._run(discover, mode == "retry"))
        except BaseException:
            self.store = None
            store.close()
            raise

    async def _run(self, discover, retry):
        try:
            # A pause can arrive before this scheduled coroutine first runs.
            if self.runner.pause_requested:
                self.phase, self.message = "paused", "已暂停，尚未发起采集。"
            else:
                await self.runner.run(discover=discover, retry=retry)
                self.phase = "paused" if self.runner.paused else "finished"
                self.message = self.runner.message
        except asyncio.CancelledError:
            self.phase, self.message = "paused", "程序关闭，未完成任务将在下次继续时重新检查。"
        except Exception as exc:
            self.phase = "error"
            self.message = f"采集已停止（{type(exc).__name__}）。请检查浏览器连接、磁盘空间或批次诊断。"
        finally:
            try:
                self.store.close()
            except Exception as exc:
                self.phase, self.message = "error", f"保存收尾失败（{type(exc).__name__}），请检查磁盘后重新打开批次。"
            self.store = None
            self._report_cache = None
            self._log(self.message, "warning" if self.phase in {"error", "paused"} else "info")

    def pause(self):
        if self.busy and self.runner:
            self.runner.pause()
            self.phase = "pausing"
            self.message = "正在安全暂停：不再发起新操作，等待在途响应保存。"

    async def close(self):
        if self.busy:
            self.pause()
            try:
                await asyncio.wait_for(asyncio.shield(self.job), timeout=35)
            except asyncio.TimeoutError:
                self.job.cancel()
                with suppress(asyncio.CancelledError):
                    await self.job

    async def check_browser(self, endpoint=None):
        from .cli import version_info
        from .runner import ADAPTER, REQUEST_ADAPTER
        from playwright.async_api import async_playwright

        endpoint = endpoint or self.endpoint
        Config(cdp=endpoint)
        pages = []
        try:
            async with asyncio.timeout(10):
                info = await asyncio.to_thread(version_info, endpoint)
                async with async_playwright() as playwright:
                    browser = await playwright.chromium.connect_over_cdp(endpoint, timeout=4000)
                    for context in browser.contexts:
                        for page in context.pages:
                            parts = urlsplit(page.url)
                            if parts.hostname not in {"www.xiaohongshu.com", "www.rednote.com"}:
                                continue
                            if not await page.evaluate("Boolean(window.__xhsReadOnly)"):
                                await page.add_script_tag(path=str(REQUEST_ADAPTER))
                            if not await page.evaluate("Boolean(window.__xhsCollect)"):
                                await page.add_script_tag(path=str(ADAPTER))
                            data = await page.evaluate("""() => {
                                const raw = window.__INITIAL_STATE__?.user?.loggedIn;
                                const v = raw?._rawValue ?? raw?.value ?? raw;
                                return {logged_in: typeof v === 'boolean' ? v : null,
                                    blocked: window.__xhsCollect.state('').blocked,
                                    adapter: window.__xhsReadOnly.probe()};
                            }""")
                            data.update(path=parts.path, discovery=bool(re.fullmatch(r"/(?:search_result|explore|user/profile/[^/]+/?)", parts.path)))
                            pages.append(data)
                self.connection = {"status": "connected", "message": "浏览器已连接", "version": info.get("Browser"), "pages": pages}
        except Exception as exc:
            self.connection = {"status": "error", "message": f"浏览器连接失败（{type(exc).__name__}）。请打开专用浏览器或检查连接地址。", "pages": []}
        self.endpoint = endpoint
        self.connection.update(endpoint=endpoint, checked_at=datetime.now().strftime("%H:%M:%S"))
        return self.connection

    async def open_browser(self, endpoint=None):
        self._idle()
        endpoint = endpoint or self.endpoint
        Config(cdp=endpoint)
        result = await self.check_browser(endpoint)
        if result["status"] == "connected":
            from playwright.async_api import async_playwright
            async with async_playwright() as p:
                browser = await p.chromium.connect_over_cdp(endpoint, timeout=4000)
                context = browser.contexts[0]
                page = next((page for page in context.pages if urlsplit(page.url).hostname == "www.xiaohongshu.com"), None)
                if page is None:
                    page = await context.new_page()
                    await page.goto("https://www.xiaohongshu.com/", wait_until="domcontentloaded", timeout=15000)
                await page.bring_to_front()
        else:
            from .cli import launch_browser
            port = urlsplit(endpoint).port or 80
            await asyncio.to_thread(launch_browser, self.root / ".collection-chrome", port)
        self.message = "专用浏览器已打开。请登录后搜索目标关键词，再回到采集工作台。"
        self._log(self.message)

    async def search(self, keyword, endpoint=None):
        self._idle()
        keyword = str(keyword).strip()
        if not keyword or len(keyword) > 100:
            raise ValueError("请输入 1–100 字的搜索关键词。")
        endpoint = endpoint or self.endpoint
        await self._preflight(endpoint, False)
        from playwright.async_api import async_playwright
        async with async_playwright() as p:
            browser = await p.chromium.connect_over_cdp(endpoint, timeout=4000)
            context = browser.contexts[0]
            pages = [page for page in context.pages if urlsplit(page.url).hostname == "www.xiaohongshu.com" and
                     urlsplit(page.url).path in {"/search_result", "/explore"}]
            if len(pages) > 1:
                raise ValueError("浏览器中有多个搜索/推荐页面，请只保留一个后再搜索。")
            page = pages[0] if pages else await context.new_page()
            await page.goto("https://www.xiaohongshu.com/search_result?" + urlencode({"keyword": keyword, "source": "web_explore_feed"}),
                            wait_until="domcontentloaded", timeout=15000)
            await page.bring_to_front()
        await self.check_browser(endpoint)
        self.message = "已打开搜索结果。可在浏览器选择排序/筛选，再创建采集批次。"

    def _report(self):
        if self.store:
            return {"tasks": self.store.tasks(), "run": self.store.run}
        if not self.selected:
            return {"tasks": [], "run": {}}
        report = self.selected / "report.json"
        stamp = (str(self.selected), report.stat().st_mtime_ns if report.exists() else 0)
        if not self._report_cache or self._report_cache[0] != stamp:
            data = json.loads(report.read_text()) if report.exists() else {"tasks": []}
            data["run"] = json.loads((self.selected / "run.json").read_text())
            self._report_cache = (stamp, data)
        return self._report_cache[1]

    def snapshot(self):
        error = None
        try:
            report = self._report()
        except (OSError, ValueError, KeyError, TypeError):
            report = {"tasks": [], "run": {}}
            error = "无法读取批次结果，请检查目录、权限或文件完整性。"
        tasks = report["tasks"]
        counts = Counter({"total": len(tasks), "complete": 0, "partial": 0, "pending": 0, "unavailable": 0, "processed": 0})
        for task in tasks:
            if task["body"] == "unavailable":
                counts["unavailable"] += 1
            elif task["body"] == "saved" and task["comments"] in {"complete_observed", "not_requested"} and task["media"] not in {"partial", "failed"}:
                counts["complete"] += 1
            elif task["status"] == "finished":
                counts["partial"] += 1
            else:
                counts["pending"] += 1
            if task["status"] == "finished":
                counts["processed"] += 1
        progress = {"primary": 0, "replies": 0, "responses": 0}
        if self.store:
            row = self.store.db.execute("SELECT p.data FROM progress p JOIN attempts a ON a.id=p.attempt ORDER BY a.rowid DESC LIMIT 1").fetchone()
            if row:
                data = json.loads(row[0])
                progress.update(primary=len(data["primary"]["ids"]), replies=sum(len(c["ids"]) for c in data["replies"].values()))
            progress["responses"] = self.store.db.execute("SELECT count(*) FROM events WHERE kind='network_json'").fetchone()[0]
            current = self.runner.message
            if current != self._last_runner_message:
                self._last_runner_message = current
                self._log(current)
            if self.phase != "pausing":
                self.message = current
        # Never send token-bearing URLs, raw user/comment objects or cookies to UI logs.
        ordered = sorted(enumerate(tasks), key=lambda pair: (pair[1]["status"] != "running", -pair[0]))
        visible = [{k: v for k, v in t.items() if k != "url"} for _, t in ordered[:200]]
        config = report["run"].get("config", {})
        return {"busy": self.busy, "phase": self.phase, "message": error or self.message,
                "batch": str(self.selected) if self.selected else None,
                "name": report["run"].get("label") or (self.selected.name if self.selected else None),
                "config": config, "counts": dict(counts), "tasks": visible, "truncated": len(tasks) > 200,
                "progress": progress, "connection": self.connection, "logs": list(self.logs),
                "output": str(self.root / "collections"), "endpoint": self.endpoint}

    def export_csv(self):
        if self.selected is None:
            raise ValueError("请先打开一个批次。")
        report = self._report()
        fields = ["note_id", "status", "body", "comments", "media", "reason", "updated"]
        text = io.StringIO(newline="")
        writer = csv.writer(text)
        writer.writerow(fields)
        for task in report["tasks"]:
            values = [str(task.get(k) or "") for k in fields]
            writer.writerow(["'" + v if v.startswith(("=", "+", "-", "@")) else v for v in values])
        path = self.selected / "summary.csv"
        atomic_write(path, text.getvalue().encode("utf-8-sig"))
        return str(path)

    async def command(self, action, payload=None):
        if action == "pause":
            return await self._command(action, payload)
        if self.command_lock.locked():
            return {"ok": False, "error": "另一个操作正在处理，请稍候。"}
        async with self.command_lock:
            return await self._command(action, payload)

    async def _command(self, action, payload=None):
        payload = payload or {}
        if not isinstance(payload, dict):
            return {"ok": False, "error": "操作参数格式错误。"}
        try:
            if action == "batches":
                return {"ok": True, "batches": self.batches()}
            if action == "check":
                await self.check_browser(payload.get("endpoint"))
            elif action == "browser":
                await self.open_browser(payload.get("endpoint"))
            elif action == "search":
                await self.search(payload.get("keyword", ""), payload.get("endpoint"))
            elif action == "create":
                await self.create_start(payload)
            elif action == "load":
                self.load(payload.get("path", ""))
            elif action == "start":
                await self.start(payload.get("mode", "resume"))
            elif action == "pause":
                self.pause()
            elif action == "export":
                return {"ok": True, "path": self.export_csv(), "state": self.snapshot()}
            else:
                raise ValueError("不支持的采集操作。")
            return {"ok": True, "state": self.snapshot()}
        except ValueError as exc:
            return {"ok": False, "error": str(exc)}
        except (OSError, sqlite3.Error):
            return {"ok": False, "error": "无法读写批次目录。请检查磁盘空间、目录权限，或是否已在其他进程打开。"}
        except Exception as exc:
            return {"ok": False, "error": f"操作未完成（{type(exc).__name__}）。请检查连接状态与批次目录后重试。"}
