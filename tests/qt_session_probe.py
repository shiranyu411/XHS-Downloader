"""Explicit two-process Qt persistent-profile test; all requests use fixtures."""

import asyncio
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import traceback

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from qt_workbench_smoke import TestBackend
from source.GUI.qt_launcher import launch


def child(root, phase):
    result = {"passed": False, "phase": phase}

    def ready(window):
        from PySide6.QtCore import QTimer, QMetaObject, Qt

        if not window.backend.ready.is_set():
            QTimer.singleShot(100, lambda: ready(window))
            return

        async def exercise():
            from playwright.async_api import async_playwright

            try:
                async with async_playwright() as p:
                    host = window.host
                    browser, _ = await host.connect(p)

                    async def route(r):
                        if r.request.url.startswith(
                            ("file:", "about:", "qrc:", "data:")
                        ):
                            await r.continue_()
                        elif r.request.url.startswith("https://www.xiaohongshu.com/"):
                            await r.fulfill(
                                status=200,
                                content_type="text/html",
                                body="<title>Offline session fixture</title>",
                            )
                        else:
                            await r.abort()

                    for context in browser.contexts:
                        await context.route("**/*", route)
                    page = await host._find(await host.native("browse_id"))
                    await page.goto("https://www.xiaohongshu.com/explore")
                    if phase == "write":
                        await page.evaluate(
                            "localStorage.setItem('session-fixture','preserved');document.cookie='test_session=preserved; max-age=3600; path=/; SameSite=Lax'"
                        )
                    else:
                        assert (
                            await page.evaluate(
                                "localStorage.getItem('session-fixture')"
                            )
                            == "preserved"
                        )
                        assert "test_session=preserved" in await page.evaluate(
                            "document.cookie"
                        )
                    result["passed"] = True
            except BaseException:
                result["error"] = traceback.format_exc()
            finally:
                print(json.dumps(result), flush=True)
                QMetaObject.invokeMethod(
                    window, "close", Qt.ConnectionType.QueuedConnection
                )

        asyncio.run_coroutine_threadsafe(exercise(), window.backend.loop)
        QTimer.singleShot(25000, window.close)

    launch(data_root=root, on_ready=ready, backend_factory=TestBackend)
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    if len(sys.argv) == 3:
        raise SystemExit(child(sys.argv[1], sys.argv[2]))
    with tempfile.TemporaryDirectory(prefix="xhs-qt-session-") as root:
        for phase in ("write", "read"):
            subprocess.run(
                [sys.executable, __file__, root, phase], check=True, timeout=40
            )
    print(
        "PASS: Qt profile cookies/localStorage survived a real process restart; not a live login test."
    )
