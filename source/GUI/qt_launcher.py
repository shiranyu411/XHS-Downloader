"""Optional single-window Qt shell. Legacy PyWebView and CLI stay available."""

from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import socket
import sys
import time

from ..module import ROOT


def launch(data_root=None, on_ready=None, backend_factory=None):
    data_root = Path(data_root or ROOT).resolve()
    data_root.mkdir(parents=True, exist_ok=True)
    # Configure WebEngine before importing/constructing QApplication. Never reuse Chrome's profile.
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    endpoint = f"http://127.0.0.1:{port}"
    os.environ["QTWEBENGINE_REMOTE_DEBUGGING"] = f"127.0.0.1:{port}"
    os.environ["QTWEBENGINE_CHROMIUM_FLAGS"] = f"--remote-allow-origins={endpoint}"
    from PySide6.QtCore import (
        QObject,
        Signal,
        Slot,
        QUrl,
        QTimer,
        QCoreApplication,
        QEvent,
    )
    from PySide6.QtWidgets import (
        QApplication,
        QMainWindow,
        QWidget,
        QVBoxLayout,
        QHBoxLayout,
        QStackedWidget,
        QPushButton,
        QLineEdit,
        QLabel,
        QFileDialog,
        QSpinBox,
        QSizePolicy,
    )
    from PySide6.QtWebChannel import QWebChannel
    from PySide6.QtWebEngineCore import (
        QWebEngineProfile,
        QWebEnginePage,
        QWebEngineUrlRequestInterceptor,
    )
    from PySide6.QtWebEngineWidgets import QWebEngineView
    from .qt_host import QtBrowserHost, SitePage, allowed_site
    from .backend import GuiBackend, GuiApi

    app = QApplication(sys.argv[:1])
    app.setApplicationName("XHS-Downloader")
    app.setStyleSheet("""
        QMainWindow {background:#f7f8fa}
        QPushButton {padding:8px 12px;border:1px solid #dce0e7;border-radius:7px;background:white;color:#334155;font-size:13px}
        QPushButton:hover {background:#fff0f3;border-color:#e58da1}
        QPushButton:checked {color:#b52449;background:#fff0f3;border:1px solid #d93555}
        QPushButton:disabled {color:#9299a7;background:#f1f3f6;border-color:#e3e6ed}
        QLineEdit, QSpinBox {padding:8px;border:1px solid #dce0e7;border-radius:6px;background:white;color:#334155}
        QLabel {color:#485266}
        QToolTip {color:#293344;background:#fff;border:1px solid #dce0e7}
    """)
    profile_dir = data_root / ".workbench-browser"
    profile_dir.mkdir(exist_ok=True, mode=0o700)
    profile = QWebEngineProfile("xhs-workbench", app)
    profile.setPersistentStoragePath(str(profile_dir / "storage"))
    profile.setCachePath(str(profile_dir / "cache"))
    profile.setPersistentCookiesPolicy(
        QWebEngineProfile.PersistentCookiesPolicy.ForcePersistentCookies
    )
    ui_profile = QWebEngineProfile(
        app
    )  # off-the-record, never shares site storage/permissions

    class LocalUiOnly(QWebEngineUrlRequestInterceptor):
        def interceptRequest(self, info):
            if info.requestUrl().scheme() not in {"file", "qrc", "data", "about"}:
                info.block(True)

    ui_interceptor = LocalUiOnly(ui_profile)
    ui_profile.setUrlRequestInterceptor(ui_interceptor)
    executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="xhs-qt-rpc")

    class Window(QMainWindow):
        shutdown_done = Signal()
        status_ready = Signal(str)
        summary_ready = Signal(object)
        consent_changed = Signal()

        def __init__(self):
            super().__init__()
            self.closing = self.can_close = False
            self.status_pending = self.session_pending = self.background_busy = False
            self.next_session_check = 0
            self.reported_restriction = None
            self.access_blocked = False
            self.workflow_view = 'pool'
            self.start_buttons, self.pause_buttons = [], []
            self.setWindowTitle("XHS-Downloader · 一体化流程工作台")
            self.resize(1380, 900)
            self.setMinimumSize(1050, 680)
            center = QWidget()
            self.setCentralWidget(center)
            layout = QVBoxLayout(center)
            nav = QHBoxLayout()
            layout.addLayout(nav)
            self.tabs = []
            for index, name in enumerate(
                ["① 浏览发现", "② 链接池", "③ 评论采集", "④ 作品下载", "⑤ 结果管理"]
            ):
                button = QPushButton(name)
                button.setCheckable(True)
                button.clicked.connect(lambda checked=False, n=index: self.select(n))
                self.tabs.append(button)
                nav.addWidget(button)
            nav.addStretch()
            self.session_status = QLabel("浏览器已创建 · 网页未打开")
            settings = QPushButton("设置")
            settings.clicked.connect(lambda: self.legacy_view("settings"))
            nav.addWidget(settings)
            self.stack = QStackedWidget()
            layout.addWidget(self.stack, 1)
            self.footer = QLabel("正在初始化本地工作台…")
            self.footer.setSizePolicy(QSizePolicy.Policy.Ignored, QSizePolicy.Policy.Preferred)
            layout.addWidget(self.footer)
            browser_pane = QWidget()
            box = QVBoxLayout(browser_pane)
            box.setContentsMargins(0, 0, 0, 0)
            toolbar = QHBoxLayout()
            box.addLayout(toolbar)
            self.browse = QWebEngineView()
            self.browse.setPage(SitePage(profile, self.browse))
            self.browse.setUrl(QUrl("about:blank"))
            self.host = QtBrowserHost(profile, self.browse, endpoint, self)
            self.backend = (backend_factory or GuiBackend)(
                browser_host=self.host, workbench_root=data_root
            )
            self.api = GuiApi(self.backend)
            for label, callback in [
                ("←", self.browse.back),
                ("→", self.browse.forward),
                ("↻", self.browse.reload),
            ]:
                button = QPushButton(label)
                button.clicked.connect(callback)
                toolbar.addWidget(button)
            self.address = QLineEdit()
            self.address.setPlaceholderText(
                "输入小红书网址或搜索关键词；启动不会自动访问网站"
            )
            self.address.returnPressed.connect(self.navigate)
            toolbar.addWidget(self.address, 1)
            open_ = QPushButton("打开小红书")
            open_.clicked.connect(lambda: self.open_url("https://www.xiaohongshu.com/"))
            toolbar.addWidget(open_)
            self.browse.urlChanged.connect(self.address_changed)
            box.addWidget(self.session_status)
            box.addWidget(self.browse, 1)
            hint = QLabel(
                "首次在这里扫码。切换标签保留网页与会话；获取结果直接入池，不会自动采评论。"
            )
            hint.setWordWrap(True)
            box.addWidget(hint)
            actions = QHBoxLayout()
            box.addLayout(actions)
            current = QPushButton("获取当前页链接")
            current.clicked.connect(lambda: self.discover(0))
            actions.addWidget(current)
            actions.addWidget(QLabel("最多滚动"))
            self.scrolls = QSpinBox()
            self.scrolls.setRange(1, 500)
            self.scrolls.setValue(30)
            actions.addWidget(self.scrolls)
            auto = QPushButton("自动滚动获取")
            auto.clicked.connect(lambda: self.discover(self.scrolls.value()))
            actions.addWidget(auto)
            stop = QPushButton("停止获取")
            stop.clicked.connect(lambda: self.command("pause"))
            actions.addWidget(stop)
            self.start_buttons.extend([(current, 'discover'), (auto, 'discover')])
            self.pause_buttons.append(stop)
            actions.addStretch()
            pool = QPushButton("前往链接池 →")
            pool.clicked.connect(lambda: self.select(1))
            actions.addWidget(pool)
            self.stack.addWidget(browser_pane)
            self.management = self.trusted("workbench.html")
            self.stack.addWidget(self.management)
            legacy_pane = QWidget()
            legacy_box = QVBoxLayout(legacy_pane)
            legacy_box.setContentsMargins(0, 0, 0, 0)
            legacy_nav = QHBoxLayout()
            legacy_box.addLayout(legacy_nav)
            for label, view in [
                ("直接粘贴下载", "new_task"),
                ("下载队列", "queue"),
                ("文件/历史", "history"),
                ("日志", "logs"),
                ("帮助", "about"),
            ]:
                button = QPushButton(label)
                button.clicked.connect(
                    lambda checked=False, v=view: self.legacy_view(v)
                )
                legacy_nav.addWidget(button)
            queue_actions = QHBoxLayout()
            legacy_box.addLayout(queue_actions)
            queue_actions.addWidget(QLabel("链接池中已提交的媒体任务"))
            queue_actions.addStretch()
            for label, action in [
                ("开始 / 继续下载队列", "start"),
                ("安全暂停", "pause"),
            ]:
                button = QPushButton(label)
                button.clicked.connect(
                    lambda checked=False, a=action: self.command(
                        a, {"mode": "download"}
                    )
                )
                queue_actions.addWidget(button)
                if action == 'start':
                    self.start_buttons.append((button, 'download'))
                else:
                    self.pause_buttons.append(button)
            self.legacy = self.trusted("index.html", "new_task")
            legacy_box.addWidget(self.legacy, 1)
            self.stack.addWidget(legacy_pane)
            self.backend.start(wait=False)
            self.status_ready.connect(self.footer.setText)
            self.summary_ready.connect(self.apply_summary)
            self.consent_changed.connect(self.refresh_consent)
            self.shutdown_done.connect(self.finish_close)
            self.poll = QTimer(self)
            self.poll.setSingleShot(True)
            self.poll.timeout.connect(self.refresh_status)
            self.poll.start(1000)
            self.onboarding_shown = False
            for view in (self.management, self.legacy):
                view.loadFinished.connect(self.sync_ui_visibility)
            self.select(1)

        def trusted(self, filename, fragment=""):
            view = QWebEngineView()
            path = (ROOT / "static" / "GUI" / filename).resolve()

            class TrustedPage(QWebEnginePage):
                def acceptNavigationRequest(self, url, type_, main_frame):
                    return bool(
                        main_frame
                        and url.isLocalFile()
                        and Path(url.toLocalFile()).resolve() == path
                    )

                def createWindow(self, type_):
                    return None

            page = TrustedPage(ui_profile, view)
            view.setPage(page)
            channel = QWebChannel(page)
            rpc = Rpc(self, page)
            channel.registerObject("rpc", rpc)
            page.setWebChannel(channel)
            # Keep wrappers alive for the entire view lifetime.
            view._bridge = (page, channel, rpc)
            url = QUrl.fromLocalFile(str(path))
            url.setQuery("shell=qt")
            url.setFragment(fragment)
            view.setUrl(url)
            return view

        def select(self, index):
            for i, tab in enumerate(self.tabs):
                tab.setChecked(i == index)
            self.stack.setCurrentIndex(0 if index == 0 else 2 if index == 3 else 1)
            if index in (1, 2, 4):
                self.workflow_view = {1: "pool", 2: "collect", 4: "results"}[index]
                self.management.page().runJavaScript(
                    f"window.setWorkspaceView?.({json.dumps(self.workflow_view)})"
                )
            self.sync_ui_visibility()
            if not self.closing:
                self.poll.start(100)

        def sync_ui_visibility(self, *_):
            if self.closing:
                return
            visible = self.isVisible() and not self.isMinimized()
            for view, index in ((self.management, 1), (self.legacy, 2)):
                active = json.dumps(visible and self.stack.currentIndex() == index)
                view.page().runJavaScript(f"""(() => {{
                    const active = {active};
                    if (window.__xhsUiVisible !== active) {{
                        window.__xhsUiVisible = active;
                        window.dispatchEvent(new Event('xhs-ui-visibility'));
                    }}
                }})()""")
            if self.stack.currentIndex() == 1:
                self.management.page().runJavaScript(
                    f"window.setWorkspaceView?.({json.dumps(self.workflow_view)})"
                )

        def changeEvent(self, event):
            super().changeEvent(event)
            if event.type() == QEvent.Type.WindowStateChange and hasattr(self, 'poll'):
                self.sync_ui_visibility()
                if not self.closing:
                    self.poll.start(100)

        def legacy_view(self, name):
            self.select(3)
            self.legacy.page().runJavaScript(
                f"window.XHSNavigate?.({json.dumps(name)})"
            )

        def open_url(self, url):
            if allowed_site(url):
                self.browse.setUrl(QUrl(url))
                self.select(0)
            else:
                self.footer.setText("浏览页仅允许小红书 HTTPS 站点。")

        def navigate(self):
            from urllib.parse import urlencode

            value = self.address.text().strip()
            if value:
                self.open_url(
                    value
                    if "://" in value
                    else "https://www.xiaohongshu.com/search_result?"
                    + urlencode({"keyword": value, "source": "web_explore_feed"})
                )

        def address_changed(self, url):
            # Address display omits access tokens. The page itself retains its exact URL.
            from urllib.parse import urlsplit, parse_qs, urlencode

            parsed = urlsplit(url.toString())
            query = parse_qs(parsed.query)
            shown = parsed._replace(
                query=urlencode({"keyword": query["keyword"][0]})
                if query.get("keyword")
                else "",
                fragment="",
            )
            self.address.setText(shown.geturl() if parsed.scheme != "about" else "")

        def command(self, action, payload=None):
            if self.closing:
                return
            future = executor.submit(self.api.workspace_command, action, payload)

            def done(f):
                try:
                    result = f.result()
                    self.status_ready.emit(result.get("message", "完成"))
                    self.summary_ready.emit(result)
                except Exception as exc:
                    self.status_ready.emit(
                        str(exc) if isinstance(exc, ValueError) else type(exc).__name__
                    )

            future.add_done_callback(done)

        def discover(self, scrolls):
            self.command("start", {"mode": "discover", "scrolls": scrolls})

        def refresh_status(self):
            if self.closing:
                return
            visible = self.isVisible() and not self.isMinimized()
            if not visible and not self.background_busy:
                return  # no idle polling while minimized; queues do not depend on UI timers
            if (not self.session_pending and time.monotonic() >= self.next_session_check
                and (self.stack.currentIndex() == 0 or self.background_busy)):
                self.next_session_check = time.monotonic() + 5
                current_url = self.browse.url().toString()
                if allowed_site(current_url):
                    self.session_pending = True

                    def session_state(value):
                        self.session_pending = False
                        if (
                            not self.closing
                            and self.browse.url().toString() == current_url
                            and isinstance(value, dict)
                        ):
                            self.session_status.setText(
                                "网页已连接 · " + value["status"]
                            )
                            restriction = (current_url, value.get('restriction'))
                            if value.get("restriction") and (
                                restriction != self.reported_restriction or not self.access_blocked
                            ):
                                self.reported_restriction = restriction
                                self.command(
                                    "report_restriction",
                                    {"reason": value["restriction"]},
                                )
                            elif not value.get('restriction'):
                                self.reported_restriction = None

                    self.browse.page().runJavaScript(
                        """(() => {
                        const raw=window.__INITIAL_STATE__?.user?.loggedIn;
                        const login=raw?._rawValue ?? raw?.value ?? raw;
                        const visible=e=>e.getClientRects().length;
                        const blocked=[...document.querySelectorAll('.captcha-container,#captcha,[class*=verify-modal]')].some(visible);
                        const pattern=/账号状态异常|帐号状态异常|账号异常|访问频繁|访问限制/;
                        const refused=pattern.test(document.title) || [...document.querySelectorAll('.error-wrapper,.error-container,.error-content,.risk-container')].some(e=>visible(e)&&pattern.test(e.innerText));
                        return {status:refused?'访问受限，自动操作已暂停':blocked?'需要人工验证':login===true?'已登录':login===false?'未登录':'登录状态待确认', restriction:refused?'account_abnormal':blocked?'verification_required':null};
                    })()""",
                        session_state,
                    )
                else:
                    self.session_status.setText("浏览器已创建 · 网页未打开")
            if self.status_pending:
                return
            if not self.backend.ready.is_set():
                self.poll.start(1000)
                return
            self.status_pending = True
            if not self.onboarding_shown:
                self.onboarding_shown = True
                if (not self.backend.settings.get("disclaimer_accepted")
                    or not self.backend.workbench or not self.backend.workbench.store):
                    self.legacy_view("new_task")
            future = executor.submit(self.api.workspace_command, "summary")

            def done(f):
                try:
                    self.summary_ready.emit(f.result())
                except Exception:
                    self.summary_ready.emit(None)

            future.add_done_callback(done)

        @Slot(object)
        def apply_summary(self, state):
            self.status_pending = False
            if self.closing:
                return
            if state is None:
                self.footer.setText("工作台未就绪；请检查启动日志。")
            else:
                self.background_busy = state['busy']
                self.access_blocked = state['blocked']
                c = state['counts']
                self.footer.setText(
                    f"批次：{state['name']}  | 链接 {c['total']} · 评论完整 {c['complete']} · 媒体完成 {c['downloaded']}  | {state['message']}"
                )
                self.footer.setToolTip(self.footer.text())
                for button, mode in self.start_buttons:
                    button.setEnabled(not state['busy'] and not state['blocked']
                        and (mode == 'discover' or bool(state['queues'][mode])))
                for button in self.pause_buttons:
                    button.setEnabled(state['busy'])
            if (self.isVisible() and not self.isMinimized()) or self.background_busy:
                self.poll.start(1000 if self.background_busy else 5000)

        @Slot()
        def refresh_consent(self):
            if not self.closing:
                self.management.page().runJavaScript("window.refreshWorkspace?.()")
                # One scheduled follow-up covers a state poll already in flight.
                QTimer.singleShot(
                    150,
                    lambda: (
                        None
                        if self.closing
                        else self.management.page().runJavaScript(
                            "window.refreshWorkspace?.()"
                        )
                    ),
                )

        def closeEvent(self, event):
            if self.can_close:
                event.accept()
                return
            event.ignore()
            if self.closing:
                return
            self.closing = True
            self.poll.stop()
            self.footer.setText("正在安全关闭：等待采集响应和下载收尾…")
            future = executor.submit(self.backend.stop)
            future.add_done_callback(lambda _: self.shutdown_done.emit())

        @Slot()
        def finish_close(self):
            self.host.closed = True
            self.can_close = True
            self.close()

    class Rpc(QObject):
        replied = Signal(str, str)
        allowed = {
            "get_state",
            "workspace_command",
            "get_translations",
            "get_disclaimer",
            "accept_disclaimer",
            "get_settings",
            "save_settings",
            "create_tasks",
            "cancel_task",
            "clear_finished",
            "paste_content",
            "copy_task_links",
            "get_history_page",
            "delete_history_records",
            "open_download_folder",
            "check_update",
            "open_url",
            "collection_command",
            "start_monitor",
            "stop_monitor",
        }

        def __init__(self, window, parent):
            super().__init__(parent)
            self.window = window

        @Slot(str, str, str)
        def invoke(self, id_, method, raw):
            def reply(value=None, error=None):
                self.replied.emit(
                    id_,
                    json.dumps(
                        {"ok": error is None, "value": value, "error": error},
                        ensure_ascii=False,
                    ),
                )

            try:
                if self.window.closing:
                    raise ValueError("程序正在关闭")
                if len(raw) > 2_100_000:
                    raise ValueError("请求过大")
                args = json.loads(raw)
                if not isinstance(args, list):
                    raise ValueError("参数必须为数组")
                if method == "browse_directory":
                    reply(
                        QFileDialog.getExistingDirectory(
                            self.window, "选择批次目录", str(ROOT / "collections")
                        )
                    )
                    return
                if method == "decline_disclaimer":
                    reply(True)
                    self.window.close()
                    return
                if method == "navigate_view":
                    index = int(args[0])
                    if index not in range(5):
                        raise ValueError("非法页面")
                    self.window.select(index)
                    reply(True)
                    return
                if method not in self.allowed:
                    raise ValueError("未授权的方法")
                future = executor.submit(getattr(self.window.api, method), *args)

                def done(f):
                    try:
                        value = f.result()
                        if method == "accept_disclaimer" and value.get("accepted"):
                            self.window.consent_changed.emit()
                        reply(value)
                    except Exception as exc:
                        reply(
                            error=str(exc)
                            if isinstance(exc, (ValueError, RuntimeError))
                            else type(exc).__name__
                        )

                future.add_done_callback(done)
            except Exception as exc:
                reply(
                    error=str(exc)
                    if isinstance(exc, ValueError)
                    else type(exc).__name__
                )

    window = Window()
    window.show()
    if on_ready:
        QTimer.singleShot(0, lambda: on_ready(window))
    # No website opens automatically, including when an interrupted batch is restored.
    try:
        app.exec()
    finally:
        executor.shutdown(wait=True, cancel_futures=True)
        # Delete all pages before persistent profiles, ensuring storage flushes.
        for view in [
            window.browse,
            window.management,
            window.legacy,
            *window.host.views.values(),
        ]:
            view.page().deleteLater()
            view.deleteLater()
        QCoreApplication.sendPostedEvents(None, QEvent.Type.DeferredDelete)
        profile.deleteLater()
        ui_profile.deleteLater()
        QCoreApplication.sendPostedEvents(None, QEvent.Type.DeferredDelete)
