"""Run explicitly: .venv/bin/python tests/qt_workbench_smoke.py

Real Qt/Chromium, synthetic website/API and downloader; no platform traffic.
All profiles and batches live in a temporary directory. Not live acceptance.
"""

import asyncio
from copy import deepcopy
import json
from pathlib import Path
import sys
import tempfile
import traceback
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import test_collection as fixtures
from source.GUI.backend import GuiBackend
from source.GUI.workbench_service import WorkbenchService
from source.GUI.qt_launcher import launch
from source.module import Settings


class TestBackend(GuiBackend):
    async def _initialize(self):
        self.settings = deepcopy(Settings.default)
        self.settings["disclaimer_accepted"] = True
        self.settings_manager = Settings(Path(self.workbench_root))
        self.settings_manager.update(self.settings)
        self.workbench = WorkbenchService(self.workbench_root, self, self.browser_host)

    async def _start_runtime(self):
        pass  # Offline GUI test, never initialize a network downloader.

    async def _process_task(self, task):
        assert task.script_data["noteId"] == fixtures.NOTE
        task.state = "success"


result = {"passed": False}


def ready(window):
    from PySide6.QtCore import QTimer, Qt, QMetaObject

    window.setWindowTitle('离线合成流程测试 · 自动关闭 · 请勿手工操作')
    window.centralWidget().setAttribute(Qt.WidgetAttribute.WA_TransparentForMouseEvents, True)
    if not window.backend.ready.is_set():
        QTimer.singleShot(100, lambda: ready(window))
        return

    self_timer = QTimer(window)
    self_timer.setSingleShot(True)
    self_timer.timeout.connect(
        lambda: window.grab().save("/tmp/xhs-workbench-offline.png")
    )
    self_timer.start(2200)

    async def exercise():
        from playwright.async_api import async_playwright

        host = window.host
        try:
            async with async_playwright() as p:
                browser, context = await host.connect(p)

                async def wait_page(suffix):
                    for _ in range(100):
                        for c in browser.contexts:
                            for page in c.pages:
                                if suffix in page.url:
                                    return page
                        await asyncio.sleep(0.1)
                    raise AssertionError("trusted UI page missing")

                management = await wait_page("workbench.html")
                legacy = await wait_page("index.html")
                await management.wait_for_function(
                    "!!window.pywebview?.api", timeout=15000
                )
                await legacy.wait_for_function("!!window.pywebview?.api", timeout=15000)
                ui_errors = []
                management.on("pageerror", lambda error: ui_errors.append(str(error)))
                legacy.on("pageerror", lambda error: ui_errors.append(str(error)))
                # Reproduce an onboarding error, then accept in the other view. The
                # shared snapshot must clear ONLY that stale banner, not all errors.
                window.backend.settings["disclaimer_accepted"] = False
                await management.evaluate("document.getElementById('create').click()")
                await management.wait_for_function(
                    "!document.getElementById('error').hidden",
                    timeout=5000,
                    polling=100,
                )
                await legacy.evaluate("window.pywebview.api.accept_disclaimer()")
                await management.wait_for_function(
                    "document.getElementById('error').hidden", timeout=5000, polling=100
                )
                assert window.backend.settings["disclaimer_accepted"]
                result["onboarding_hint_cleared"] = True
                fixture = SimpleNamespace(mode="full", calls=[], responded=[])

                async def route(request):
                    if request.request.url.startswith(
                        ("file:", "qrc:", "about:", "data:")
                    ):
                        await request.continue_()
                    else:
                        await fixtures.BrowserTests.route(fixture, request)

                for c in browser.contexts:
                    await c.route("**/*", route)
                await management.evaluate(
                    "window.pywebview.api.workspace_command('create', {name:'offline-smoke'})"
                )
                browse = await host._find(await host.native("browse_id"))
                await browse.goto(
                    "https://www.xiaohongshu.com/search_result?keyword=fixture"
                )
                await browse.evaluate(
                    """note => {
                    window.__INITIAL_STATE__.search={feeds:[{id:note,noteCard:{},xsecToken:'fixture-token'}]};
                    document.body.innerHTML=`<a href='/explore/${note}'>fixture</a>`;
                    localStorage.setItem('probe', 'persisted');
                    document.cookie='fixture=local; max-age=3600; path=/; SameSite=Lax';
                }""",
                    fixtures.NOTE,
                )
                assert await browse.evaluate(
                    'typeof qt === "undefined" || !qt.webChannelTransport'
                )
                target = await host.native("browse_id")
                for index in [0, 1, 2, 3, 4, 1]:
                    await management.evaluate(
                        "n=>window.pywebview.api.navigate_view(n)", index
                    )
                    assert await host.native("browse_id") == target
                assert (
                    await browse.evaluate('localStorage.getItem("probe")')
                    == "persisted"
                )
                await management.evaluate(
                    "window.pywebview.api.workspace_command('start', {mode:'discover',scrolls:0})"
                )
                await window.backend.workbench.job
                s = await management.evaluate(
                    "window.pywebview.api.workspace_command('state')"
                )
                assert s["counts"]["total"] == 1, s
                assert not fixture.calls
                # Fast synthetic run, don't change persisted production defaults.
                from dataclasses import replace

                store = window.backend.workbench.store
                store.config = replace(
                    store.config, open_interval=0, page_interval=0, operation_timeout=5
                )
                await management.evaluate(
                    "id=>window.pywebview.api.workspace_command('submit',{ids:[id],kind:'comments'})",
                    fixtures.NOTE,
                )
                await management.evaluate(
                    "window.pywebview.api.workspace_command('start',{mode:'comments'})"
                )
                await window.backend.workbench.job
                s = await management.evaluate(
                    "window.pywebview.api.workspace_command('state')"
                )
                assert s["rows"][0]["comments"] == "complete_observed", s
                assert s["progress"] == {"primary": 21, "replies": 65}, s
                await management.evaluate(
                    "id=>window.pywebview.api.workspace_command('submit',{ids:[id],kind:'download'})",
                    fixtures.NOTE,
                )
                await management.evaluate(
                    "window.pywebview.api.workspace_command('start',{mode:'download'})"
                )
                await window.backend.workbench.job
                s = await management.evaluate(
                    "window.pywebview.api.workspace_command('state')"
                )
                assert s["rows"][0]["media"] == "success", s
                assert s["rows"][0]["comments"] == "complete_observed"
                assert len(host.views) == 0, "worker page leaked"
                # Whitelist rejects arbitrary native calls.
                rejected = await management.evaluate(
                    "window.pywebview.api.unknown_native_method().then(()=>false,()=>true)"
                )
                assert rejected
                assert not ui_errors, ui_errors
                result.update(
                    passed=True,
                    discovery=s["counts"]["total"],
                    primary=21,
                    replies=65,
                    media="synthetic_success",
                    same_browser_target=True,
                    ui_errors=ui_errors,
                    site_has_no_bridge=True,
                    arbitrary_rpc_rejected=True,
                )
        except BaseException:
            result["error"] = traceback.format_exc()
        finally:
            print(json.dumps(result, ensure_ascii=False, indent=2), flush=True)
            QMetaObject.invokeMethod(
                window, "close", Qt.ConnectionType.QueuedConnection
            )

    asyncio.run_coroutine_threadsafe(exercise(), window.backend.loop)
    QTimer.singleShot(75000, window.close)


if __name__ == "__main__":
    with tempfile.TemporaryDirectory(prefix="xhs-qt-smoke-") as directory:
        launch(data_root=directory, on_ready=ready, backend_factory=TestBackend)
    if not result["passed"]:
        raise SystemExit(1)
