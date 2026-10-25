"""Offline UI/performance probe. Run directly; --baseline records without assertions.

Temporary batch/profile, no downloader and no website navigation. Reports are local
/tmp artifacts. Metrics count state reads and DOM mutations, not platform requests.
"""

import asyncio
from collections import Counter
from concurrent.futures import Future
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import traceback

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from qt_workbench_smoke import TestBackend
from source.GUI.qt_launcher import launch
from source.collection.workspace import Workspace

RESULT = {"passed": False}
BASELINE = "--baseline" in sys.argv


class ProbeBackend(TestBackend):
    async def _initialize(self):
        await super()._initialize()
        self.calls = Counter()
        self.delay_state = False
        self.state_delayed = asyncio.Event()
        store = self.workbench.store = Workspace(Path(self.workbench_root) / "fixture")
        with store.db:
            for i in range(2000):
                note = f"{i:024x}"
                store.db.execute(
                    "INSERT INTO tasks(note_id,url) VALUES(?,?)",
                    (note, f"https://www.xiaohongshu.com/explore/{note}"),
                )
                store.db.execute(
                    "INSERT INTO workspace_items(note_id,title) VALUES(?,?)",
                    (note, f"合成作品 {i} " + "长标题测试" * 20),
                )
                store.db.execute(
                    "INSERT INTO sources(note_id,metadata) VALUES(?,?)",
                    (note, '{"mode":"offline-fixture"}'),
                )
        original = store.overview

        def overview():
            self.calls["overview"] += 1
            return original()

        store.overview = overview

    async def workspace_command(self, action, payload=None):
        self.calls[action] += 1
        if action not in {"state", "summary", "batches"}:
            raise ValueError("本地性能测试窗口，不执行采集或下载")
        result = await super().workspace_command(action, payload)
        if action == 'state' and self.delay_state:
            self.delay_state = False
            self.state_delayed.set()
            await asyncio.sleep(0.4)  # deliver a now-stale snapshot after the filter changes
        return result

    async def snapshot(self):
        self.calls["legacy_state"] += 1
        return await super().snapshot()


def process_metrics():
    """CPU seconds/RSS for this probe's process tree only (macOS/Linux ps)."""
    rows = []
    output = subprocess.check_output(
        ["ps", "-axo", "pid=,ppid=,time=,rss="], text=True
    )
    for line in output.splitlines():
        pid, ppid, cpu, rss = line.split()
        fields = cpu.split(":")
        seconds = float(fields[-1]) + 60 * int(fields[-2])
        if len(fields) > 2:
            seconds += 3600 * int(fields[-3])
        rows.append((int(pid), int(ppid), seconds, int(rss)))
    ids = {os.getpid()}
    for _ in range(10):
        ids |= {p for p, parent, _, _ in rows if parent in ids}
    return {
        "cpu_seconds": round(sum(c for p, _, c, _ in rows if p in ids), 3),
        "rss_mib": round(sum(r for p, _, _, r in rows if p in ids) / 1024, 1),
        "processes": len(ids),
    }


def ready(window):
    from PySide6.QtCore import QObject, Signal, Slot, QTimer, Qt
    from PySide6.QtWebEngineCore import QWebEngineUrlRequestInterceptor

    if not hasattr(window, '_probe_network_guard'):
        class OfflineOnly(QWebEngineUrlRequestInterceptor):
            def interceptRequest(self, info):
                if info.requestUrl().scheme() not in {'file', 'qrc', 'about', 'data'}:
                    RESULT['blocked_remote_requests'] += 1
                    info.block(True)

        RESULT['blocked_remote_requests'] = 0
        window._probe_network_guard = OfflineOnly(window)
        window.host.profile.setUrlRequestInterceptor(window._probe_network_guard)
        window.setWindowTitle('离线 UI 性能测试 · 禁止访问网站 · 约30秒后自动关闭')
        # A benchmark must not accept manual clicks that change tabs/open websites.
        window.centralWidget().setAttribute(Qt.WidgetAttribute.WA_TransparentForMouseEvents, True)
    if not window.backend.ready.is_set():
        QTimer.singleShot(100, lambda: ready(window))
        return

    class MainThread(QObject):
        requested = Signal(object)

        def __init__(self):
            super().__init__(window)
            self.requested.connect(self.perform)

        @Slot(object)
        def perform(self, item):
            operation, future = item
            try:
                future.set_result(operation())
            except Exception as exc:
                future.set_exception(exc)

        async def run(self, operation):
            future = Future()
            self.requested.emit((operation, future))
            return await asyncio.wrap_future(future)

    main = MainThread()

    async def exercise():
        from playwright.async_api import async_playwright

        try:
            async with async_playwright() as p:
                browser, _ = await window.host.connect(p)
                pages = [page for c in browser.contexts for page in c.pages]
                management = next(page for page in pages if "workbench.html" in page.url)
                legacy = next(page for page in pages if "index.html" in page.url)
                errors = []
                for page in (management, legacy):
                    page.on("pageerror", lambda e: errors.append(str(e)))
                    await page.wait_for_function("!!window.pywebview?.api")
                await asyncio.sleep(2)
                await main.run(lambda: window.select(1))
                await management.wait_for_function("document.querySelectorAll('#rows tr').length === 50")

                async def measure(name, page):
                    await asyncio.sleep(1)
                    await page.evaluate("""() => {
                        window.probeObserver?.disconnect();
                        window.probeMutations = 0;
                        window.probeObserver = new MutationObserver(items => window.probeMutations += items.length);
                        window.probeObserver.observe(document.querySelector('main') || document.body, {childList:true, subtree:true, characterData:true});
                        window.probeFocus = document.querySelector('#rows input') || document.querySelector('#urlInput');
                        window.probeFocus?.focus();
                    }""")
                    before = window.backend.calls.copy()
                    resources = process_metrics()
                    start = time.monotonic()
                    await asyncio.sleep(6)
                    elapsed = time.monotonic() - start
                    after = process_metrics()
                    RESULT[name] = {
                        "reads": dict(window.backend.calls - before),
                        "mutations": await page.evaluate("window.probeMutations"),
                        "focus_preserved": await page.evaluate("window.probeFocus === document.activeElement && !!window.probeFocus?.isConnected"),
                        "cpu_percent": round((after["cpu_seconds"] - resources["cpu_seconds"]) / elapsed * 100, 1),
                        "rss_mib": after["rss_mib"],
                    }
                    await page.evaluate("window.probeObserver.disconnect()")
                    expected_index = 1 if name == 'pool_idle' else 2
                    assert await main.run(window.stack.currentIndex) == expected_index, 'benchmark view was changed'
                    assert await main.run(lambda: window.browse.url().toString()) == 'about:blank', 'benchmark website was opened'

                await measure("pool_idle", management)
                await main.run(lambda: window.resize(1050, 680))
                await asyncio.sleep(0.5)
                RESULT["pool_overflow"] = await management.evaluate("document.documentElement.scrollWidth > innerWidth")
                await main.run(lambda: window.grab().save("/tmp/xhs-ui-pool.png"))
                await main.run(lambda: window.legacy_view("new_task"))
                await measure("download_idle", legacy)
                await main.run(lambda: window.grab().save("/tmp/xhs-ui-download.png"))
                await main.run(window.showMinimized)
                await asyncio.sleep(1)
                before = window.backend.calls.copy()
                await asyncio.sleep(6)
                RESULT["minimized_reads"] = dict(window.backend.calls - before)
                await main.run(window.showNormal)
                await main.run(lambda: window.select(1))
                if not BASELINE:
                    # In-flight state responses must not erase a newer filter request.
                    window.backend.delay_state = True
                    await management.evaluate('void window.refreshWorkspace()')
                    await asyncio.wait_for(window.backend.state_delayed.wait(), 5)
                    await management.evaluate("""() => {
                        document.getElementById('query').value = '合成作品 1999 ';
                        document.getElementById('search').click();
                    }""")
                    await management.wait_for_function("document.getElementById('range').textContent.includes('符合筛选 1 篇')", polling=100)
                    assert '合成作品 1999' in await management.locator('#rows').inner_text()
                    await management.evaluate("document.querySelector('#rows input').click()")
                    assert await management.evaluate("document.getElementById('selectPage').checked")
                    await management.evaluate("document.getElementById('clear').click()")
                    assert await management.evaluate("document.querySelector('[data-submit]').disabled")
                    RESULT['inflight_filter_and_selection'] = True
                    assert await legacy.evaluate("getComputedStyle(document.querySelector('.composer-index')).display === 'none'")
                    from PySide6.QtGui import QPalette
                    RESULT['native_button_text_color'] = await main.run(
                        lambda: window.tabs[0].palette().color(QPalette.ColorRole.ButtonText).name()
                    )
                    assert RESULT['native_button_text_color'] == '#334155'
                    async def background_work():
                        await asyncio.sleep(0.5)
                        RESULT['background_completed_while_minimized'] = True
                    window.backend.workbench.job = asyncio.create_task(background_work())
                    await main.run(window.showMinimized)
                    await window.backend.workbench.job
                    await main.run(window.showNormal)
                RESULT["ui_errors"] = errors
                if not BASELINE:
                    assert not errors, errors
                    assert RESULT["pool_idle"]["mutations"] == 0, RESULT
                    assert RESULT["pool_idle"]["focus_preserved"], RESULT
                    assert RESULT["download_idle"]["mutations"] == 0, RESULT
                    assert not RESULT["pool_overflow"], RESULT
                    assert not RESULT["minimized_reads"], RESULT
                    assert not RESULT["download_idle"]["reads"].get("state"), RESULT
                    assert not RESULT["pool_idle"]["reads"].get("legacy_state"), RESULT
                    assert not RESULT["pool_idle"]["reads"].get("overview"), RESULT
                RESULT["passed"] = True
        except BaseException:
            RESULT["error"] = traceback.format_exc()
        finally:
            report = "/tmp/xhs-ui-baseline.json" if BASELINE else "/tmp/xhs-ui-after.json"
            Path(report).write_text(json.dumps(RESULT, ensure_ascii=False, indent=2))
            print(json.dumps(RESULT, ensure_ascii=False, indent=2), flush=True)
            await main.run(window.close)

    asyncio.run_coroutine_threadsafe(exercise(), window.backend.loop)
    QTimer.singleShot(90000, window.close)


if __name__ == "__main__":
    with tempfile.TemporaryDirectory(prefix="xhs-ui-probe-", ignore_cleanup_errors=True) as directory:
        launch(data_root=directory, on_ready=ready, backend_factory=ProbeBackend)
    if not RESULT["passed"]:
        raise SystemExit(1)
