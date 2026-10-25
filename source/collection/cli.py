"""Local collection entry point, separate from the upstream media-download queue."""
import argparse
import asyncio
from contextlib import AsyncExitStack
import json
from pathlib import Path
import shutil
import signal
import socket
import subprocess
import sys
from urllib.error import HTTPError, URLError
from urllib.request import ProxyHandler, Request, build_opener

from .config import Config
from .store import Store


def version_info(endpoint):
    # Never send local CDP discovery through HTTP_PROXY/HTTPS_PROXY.
    opener = build_opener(ProxyHandler({}))
    with opener.open(Request(endpoint.rstrip("/") + "/json/version"), timeout=3) as response:
        data = json.load(response)
    if not data.get("webSocketDebuggerUrl"):
        raise ValueError("CDP 响应缺少 WebSocket 地址")
    return data


async def doctor(endpoint):
    from playwright.async_api import async_playwright
    from .runner import ADAPTER, REQUEST_ADAPTER

    Config(cdp=endpoint)
    try:
        info = await asyncio.to_thread(version_info, endpoint)
    except (HTTPError, URLError, ValueError, TimeoutError) as exc:
        detail = f"HTTP {exc.code}" if isinstance(exc, HTTPError) else type(exc).__name__
        print(f"CDP 不可用：{detail}。请启动专用调试 Chrome；普通 Chrome 的监听端口不一定支持 CDP。")
        return 1
    print(f"CDP 可用：{info.get('Browser', 'unknown')}")
    async with async_playwright() as playwright:
        browser = await playwright.chromium.connect_over_cdp(endpoint, timeout=5000)
        found = 0
        for context in browser.contexts:
            for page in context.pages:
                from urllib.parse import urlsplit
                parts = urlsplit(page.url)
                if parts.hostname not in {"www.xiaohongshu.com", "www.rednote.com"}:
                    continue
                found += 1
                if not await page.evaluate("Boolean(window.__xhsReadOnly)"):
                    await page.add_script_tag(path=str(REQUEST_ADAPTER))
                if not await page.evaluate("Boolean(window.__xhsCollect)"):
                    await page.add_script_tag(path=str(ADAPTER))
                probe = await page.evaluate("window.__xhsReadOnly.probe()")
                state = await page.evaluate("window.__xhsCollect.state('')")
                logged_in = await page.evaluate("""() => {
                    const raw = window.__INITIAL_STATE__?.user?.loggedIn;
                    const value = raw?._rawValue ?? raw?.value ?? raw;
                    return typeof value === 'boolean' ? value : null;
                }""")
                print(json.dumps({"page": parts.path, "adapter": probe,
                                  "logged_in": logged_in,
                                  "login_or_verification_visible": state["blocked"]}, ensure_ascii=False))
        if not found:
            print("尚未打开小红书页面。请在专用 Chrome 中登录并打开搜索结果页后重新诊断。")
    return 0


def launch_browser(profile, port):
    if not 1024 <= port <= 65535:
        raise ValueError("调试端口必须在 1024–65535 范围内")
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=0.3):
            pass
    except OSError:
        pass
    else:
        raise ValueError("该端口已被使用；已有专用 Chrome 请直接运行 doctor，否则选择其他端口")
    profile = profile.resolve()
    normal_profiles = [Path.home() / "Library/Application Support/Google/Chrome",
                       Path.home() / ".config/google-chrome"]
    if any(profile == p or p in profile.parents for p in normal_profiles):
        raise ValueError("请指定专用目录，不能使用日常 Chrome 配置目录")
    candidates = ["/Applications/Google Chrome Dev.app/Contents/MacOS/Google Chrome Dev",
                  "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
                  shutil.which("google-chrome"), shutil.which("chromium")]
    executable = next((p for p in candidates if p and Path(p).is_file()), None)
    if not executable:
        raise RuntimeError("未找到 Chrome；请按采集使用说明手动启动")
    profile.mkdir(parents=True, exist_ok=True, mode=0o700)
    subprocess.Popen([executable, f"--user-data-dir={profile}", f"--remote-debugging-port={port}",
                      "--remote-debugging-address=127.0.0.1", "--no-first-run", "--no-default-browser-check",
                      "https://www.xiaohongshu.com/"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                     start_new_session=True)
    print(f"已请求启动专用 Chrome，CDP：http://127.0.0.1:{port}\n配置目录：{profile}\n请在该窗口手动登录、搜索关键词。")
    return 0


async def run_batch(args):
    from .runner import Runner

    overrides = {k: getattr(args, k) for k in Config.__dataclass_fields__ if getattr(args, k, None) is not None}
    existing = args.batch / "run.json"
    if existing.exists():
        saved = json.loads(existing.read_text())["config"]
        defaults = Config()
        if any(saved.get(k, getattr(defaults, k)) != v for k, v in overrides.items()):
            raise ValueError("已有批次沿用 run.json 配置；修改参数请使用新的批次目录")
    if not existing.exists() and not args.discover and not args.urls_file:
        raise ValueError("新批次需要 --discover 或 --urls-file")
    config = Config(**overrides)
    store = Store(args.batch, config)
    try:
        if args.urls_file:
            urls = [line.strip() for line in args.urls_file.read_text().splitlines() if line.strip()]
            store.enqueue(urls, {"mode": "file", "file_name": args.urls_file.name})
        runner = Runner(store)
        async with AsyncExitStack() as stack:
            if store.config.media:
                from source import XHS, Settings
                settings = Settings().run()
                if not settings.get("disclaimer_accepted"):
                    raise ValueError("媒体下载需先在原程序中完成初始化与声明确认")
                xhs = await stack.enter_async_context(XHS(**settings, _print=False))

                async def media(note, url):
                    result = await xhs.process_collected_note(note, url, media=True)
                    return result["media"]

                runner.media = media
            loop = asyncio.get_running_loop()
            signals = []
            for sig in (signal.SIGINT, signal.SIGTERM):
                try:
                    loop.add_signal_handler(sig, runner.pause)
                    signals.append(sig)
                except (NotImplementedError, RuntimeError):
                    pass

            async def show_progress():
                previous = None
                while True:
                    if runner.message != previous:
                        print(runner.message, flush=True)
                        previous = runner.message
                    await asyncio.sleep(0.5)

            monitor = asyncio.create_task(show_progress())
            try:
                await runner.run(discover=args.discover, retry=args.retry)
            finally:
                monitor.cancel()
                await asyncio.gather(monitor, return_exceptions=True)
                for sig in signals:
                    loop.remove_signal_handler(sig)
        print(runner.message)
        print(f"结果：{store.root / 'report.json'}")
        tasks = store.tasks()
        if runner.paused or any(t["status"] != "finished" or t["body"] == "partial" or
               t["comments"] in {"partial", "blocked", "failed"} or
               t["media"] in {"partial", "failed"} for t in tasks):
            return 2
        return 0
    finally:
        store.close()


def parser():
    root = argparse.ArgumentParser(description="小红书原始数据采集（专用 Chrome + CDP）")
    commands = root.add_subparsers(dest="command", required=True)
    browser = commands.add_parser("browser", help="启动独立配置目录的 Chrome，不复制日常浏览器会话")
    browser.add_argument("--profile", type=Path, default=Path(".collection-chrome"))
    browser.add_argument("--port", type=int, default=9223)
    check = commands.add_parser("doctor", help="只检查 CDP 和网页模块，不请求评论")
    check.add_argument("--cdp", default=Config().cdp)
    status = commands.add_parser("status", help="读取已导出的批次结果")
    status.add_argument("--batch", type=Path, required=True)
    run = commands.add_parser("run", help="开始或恢复；Ctrl+C 安全暂停")
    run.add_argument("--batch", type=Path, required=True)
    run.add_argument("--discover", action="store_true", help="先滚动当前唯一搜索/作者/推荐页发现链接")
    run.add_argument("--urls-file", type=Path, help="完整笔记链接，每行一个")
    run.add_argument("--retry", action="store_true", help="重试部分完成任务，保留历史尝试")
    run.add_argument("--cdp")
    run.add_argument("--strategy", choices=("auto", "request", "ui"))
    for name, type_ in (("open_interval", float), ("max_scrolls", int), ("note_timeout", float),
                        ("operation_timeout", float), ("max_operations", int), ("no_progress_limit", int)):
        run.add_argument("--" + name.replace("_", "-"), type=type_)
    run.add_argument("--media", action="store_true", default=None, help="同时调用原下载器下载媒体")
    run.add_argument("--no-comments", dest="comments", action="store_false", default=None)
    return root


def main(argv=None):
    args = parser().parse_args(argv)
    try:
        if args.command == "browser":
            return launch_browser(args.profile, args.port)
        if args.command == "doctor":
            return asyncio.run(doctor(args.cdp))
        if args.command == "status":
            report = json.loads((args.batch / "report.json").read_text())
            # Do not print full token-bearing URLs or raw user data in status output.
            for task in report["tasks"]:
                print(json.dumps({k: v for k, v in task.items() if k != "url"}, ensure_ascii=False))
            return 0
        return asyncio.run(run_batch(args))
    except ValueError as exc:
        print(f"配置错误：{exc}", file=sys.stderr)
    except ModuleNotFoundError:
        print("缺少采集依赖，请执行 uv sync --extra collection --extra mcp", file=sys.stderr)
    except Exception as exc:
        print(f"采集停止：{type(exc).__name__}；请运行 doctor 并检查批次 diagnostics/ 与 report.json。", file=sys.stderr)
    return 1
