"""Qt owns page lifetime; CDP attaches only to explicitly registered targets."""

import asyncio
from concurrent.futures import Future
import time
from urllib.parse import urlsplit

from PySide6.QtCore import QObject, Signal, Slot, QUrl
from PySide6.QtGui import QDesktopServices
from PySide6.QtWidgets import QApplication
from PySide6.QtWebEngineWidgets import QWebEngineView
from PySide6.QtWebEngineCore import QWebEnginePage


SITES = {"www.xiaohongshu.com", "www.rednote.com"}


def allowed_site(url):
    parts = urlsplit(url)
    return (
        parts.scheme == "https"
        and parts.hostname in SITES
        and not parts.username
        and parts.port in (None, 443)
    )


class SitePage(QWebEnginePage):
    """No WebChannel on remote pages, no arbitrary external protocol launches."""

    def acceptNavigationRequest(self, url, type_, main_frame):
        if not main_frame:
            return True  # site login embeds remain in the browser sandbox
        return url.toString() == "about:blank" or allowed_site(url.toString())

    def createWindow(self, type_):
        # Keep note popups in the application's browser, never create external Chrome.
        return self


class QtBrowserHost(QObject):
    requested = Signal(object)

    def __init__(self, profile, browse, endpoint, parent=None):
        super().__init__(parent)
        self.profile, self.browse, self.endpoint = profile, browse, endpoint
        self.views = {}
        self.browser = None
        self.closed = False
        self.requested.connect(self._perform)

    async def native(self, command, value=None):
        if self.closed:
            raise RuntimeError("浏览器宿主已关闭")
        future = Future()
        self.requested.emit((command, value, future))
        return await asyncio.wait_for(asyncio.wrap_future(future), 15)

    @Slot(object)
    def _perform(self, request):
        command, value, future = request
        if future.cancelled():
            return
        try:
            if command == "new_page":
                view = QWebEngineView(self.browse.parentWidget())
                page = SitePage(self.profile, view)
                view.setPage(page)
                page.setUrl(QUrl("about:blank"))
                view.hide()
                target = page.devToolsId()
                self.views[target] = view
                result = target
            elif command == "close_page":
                view = self.views.pop(value, None)
                if view:
                    view.stop()
                    view.page().deleteLater()
                    view.deleteLater()
                result = True
            elif command == "browse_id":
                result = self.browse.page().devToolsId()
            elif command == "clipboard":
                QApplication.clipboard().setText(value)
                result = True
            elif command == "folder":
                result = QDesktopServices.openUrl(QUrl.fromLocalFile(value))
            else:
                raise ValueError("非法宿主操作")
            if not future.cancelled():
                future.set_result(result)
        except Exception as exc:
            if not future.cancelled():
                future.set_exception(exc)

    async def connect(self, playwright):
        from ..collection.cli import version_info

        info = await asyncio.to_thread(version_info, self.endpoint)
        self.version = info.get("User-Agent", "Qt WebEngine")
        # Resolve loopback without HTTP_PROXY; Qt doesn't implement Browser.setDownloadBehavior.
        self.browser = await playwright.chromium.connect_over_cdp(
            info["webSocketDebuggerUrl"], no_defaults=True, timeout=5000
        )
        if not self.browser.contexts:
            raise RuntimeError("Qt 浏览器尚未创建页面")
        return self.browser, self.browser.contexts[0]

    async def _find(self, target):
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline:
            for context in self.browser.contexts:
                for page in context.pages:
                    session = None
                    try:
                        session = await context.new_cdp_session(page)
                        info = await session.send("Target.getTargetInfo")
                        if info["targetInfo"]["targetId"] == target:
                            return page
                    except Exception:
                        pass
                    finally:
                        if session:
                            await session.detach()
            await asyncio.sleep(0.1)
        raise TimeoutError("Qt 页面未出现在 CDP 中")

    async def discovery_page(self):
        page = await self._find(await self.native("browse_id"))
        path = urlsplit(page.url).path
        if not allowed_site(page.url) or not (
            path in {"/search_result", "/explore", "/"}
            or path.startswith("/user/profile/")
        ):
            raise ValueError("请先在浏览页登录，打开搜索结果、推荐页或作者主页。")
        return page

    async def new_page(self):
        target = await self.native("new_page")
        try:
            return await self._find(target)
        except BaseException:
            await self.native("close_page", target)
            raise

    async def close_page(self, page):
        session = await page.context.new_cdp_session(page)
        try:
            target = (await session.send("Target.getTargetInfo"))["targetInfo"][
                "targetId"
            ]
        finally:
            await session.detach()
        await self.native("close_page", target)
