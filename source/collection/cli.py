"""Local collection entry point, separate from the upstream media-download queue."""
import argparse
import asyncio
from contextlib import AsyncExitStack, closing
import json
from pathlib import Path
import shutil
import signal
import sqlite3
import socket
import subprocess
import sys
import time
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
        browser = await playwright.chromium.connect_over_cdp(endpoint, no_defaults=True, timeout=5000)
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


async def run_record(args):
    """Passive mode: archive manual browsing. Never injects scripts or sends requests."""
    from .passive import PassiveRecorder

    overrides = {k: getattr(args, k) for k in Config.__dataclass_fields__ if getattr(args, k, None) is not None}
    store = Store(args.batch, Config(**overrides))
    try:
        if store.control("workspace_blocked"):
            raise ValueError("访问限制尚未人工确认恢复；请先停止浏览并处理平台提示。")
        try:
            info = await asyncio.to_thread(version_info, store.config.cdp)
        except (HTTPError, URLError, ValueError, TimeoutError) as exc:
            detail = f"HTTP {exc.code}" if isinstance(exc, HTTPError) else type(exc).__name__
            print(f"CDP 不可用：{detail}。请先启动专用 Chrome 并登录小红书。")
            return 1
        print(f"已连接 {info.get('Browser', 'unknown')}")

        def notify(message):
            print(f"[{time.strftime('%H:%M:%S')}] {message}", flush=True)

        recorder = PassiveRecorder(store, store.config.cdp, on_event=notify)
        loop = asyncio.get_running_loop()
        signals = []
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, recorder.stop)
                signals.append(sig)
            except (NotImplementedError, RuntimeError):
                pass

        async def summary():
            while not recorder.stopped:
                await asyncio.sleep(args.summary_interval)
                report = recorder.summary()
                rate = report["rate"]
                notify(f"清单 {report['discovered']} 篇｜已见评论 {report['comments_total']} 条｜"
                       f"近 10s {rate['last_10s']} 次、60s {rate['last_60s']} 次请求"
                       + ("｜⚠ 速度偏快，建议放慢" if rate["fast"] else ""))

        monitor = asyncio.create_task(summary())
        try:
            await recorder.run(status_interval=args.status_interval)
        finally:
            monitor.cancel()
            await asyncio.gather(monitor, return_exceptions=True)
            for sig in signals:
                loop.remove_signal_handler(sig)
        print(f"已导出 {store.root / 'passive_report.json'}")
        print(json.dumps(recorder.summary(), ensure_ascii=False, indent=1))
        return 0
    finally:
        store.close()


def parser():
    root = argparse.ArgumentParser(description="小红书原始数据采集（专用 Chrome + CDP）")
    commands = root.add_subparsers(dest="command", required=True)
    for name, help_ in [('keyword', '输入一个或多个关键词，完整采集'), ('posts', '输入帖子链接，完整采集')]:
        command = commands.add_parser(name, help=help_)
        command.add_argument('inputs', nargs='+')
        command.add_argument('--batch', type=Path)
        command.add_argument('--cdp')
    resume = commands.add_parser('resume', help='补充未完成内容，复用已校验文件和评论分页')
    resume.add_argument('--batch', type=Path)
    recovery = resume.add_mutually_exclusive_group()
    recovery.add_argument('--confirmed-access', action='store_true', help='已在浏览器人工完成登录/验证；保留平台冷却')
    recovery.add_argument('--retry-api-once', action='store_true',
                          help='人工请求重试上次限流的原 API 分页一次；成功才续采，验证/登录/冷却不解锁')
    for name, help_ in [('pause', '安全暂停正在运行的采集'), ('export', '导出帖子、评论、图片和采集摘要')]:
        command = commands.add_parser(name, help=help_)
        command.add_argument('--batch', type=Path)
    record = commands.add_parser("record", help="被动录制：只存档你手动浏览产生的响应，不注入脚本、不主动请求")
    record.add_argument("--batch", type=Path, required=True)
    record.add_argument("--cdp")
    record.add_argument("--status-interval", type=float, default=2.0, help="写入 passive_report.json 的间隔秒数")
    record.add_argument("--summary-interval", type=float, default=15.0, help="终端汇总间隔秒数")
    browser = commands.add_parser("browser", help="启动独立配置目录的 Chrome，不复制日常浏览器会话")
    browser.add_argument("--profile", type=Path, default=Path(".collection-chrome"))
    browser.add_argument("--port", type=int, default=9223)
    check = commands.add_parser("doctor", help="只检查 CDP 和网页模块，不请求评论")
    check.add_argument("--cdp", default=Config().cdp)
    status = commands.add_parser("status", help="读取已导出的批次结果")
    status.add_argument("--batch", type=Path)
    audit = commands.add_parser("audit", help="只读审计：存档校验、离线回放与验收指标（不联网）")
    audit.add_argument("--batch", type=Path, required=True, action="append")
    repair = commands.add_parser("repair-passive", help="离线重建被动累计报告；--apply 先备份，拒绝占用批次")
    repair.add_argument("--batch", type=Path, required=True)
    repair.add_argument("--output", type=Path)
    repair.add_argument("--apply", action="store_true")
    ack = commands.add_parser("ack-restriction", help="人工确认恢复：仅解除批次访问锁，保留冷却，不开始任务")
    ack.add_argument("--batch", type=Path, required=True)
    ack.add_argument("--confirmed", action="store_true", help="确认已按平台提示等待并人工检查访问恢复")
    run = commands.add_parser("run", help="开始或恢复；Ctrl+C 安全暂停")
    run.add_argument("--batch", type=Path, required=True)
    run.add_argument("--discover", action="store_true", help="先滚动当前唯一搜索/作者/推荐页发现链接")
    run.add_argument("--urls-file", type=Path, help="完整笔记链接，每行一个")
    run.add_argument("--retry", action="store_true", help="重试部分完成任务，保留历史尝试")
    run.add_argument("--cdp")
    run.add_argument("--strategy", choices=("auto", "request", "ui"))
    for name, type_ in (("open_interval", float), ("page_interval", float), ("max_scrolls", int), ("note_timeout", float),
                        ("operation_timeout", float), ("max_operations", int), ("no_progress_limit", int),
                        ("comment_page_quantum", int), ("post_concurrency", int), ("max_requests_per_minute", int), ("session_request_budget", int), ("session_rest", float),
                        ("restriction_cooldown", float)):
        run.add_argument("--" + name.replace("_", "-"), type=type_)
    run.add_argument("--media", action="store_true", default=None, help="同时调用原下载器下载媒体")
    run.add_argument("--no-comments", dest="comments", action="store_false", default=None)
    return root


def selected_batch(args):
    if args.batch:
        return args.batch.expanduser().resolve()
    path = Path.cwd() / '.collection-ui.json'
    saved = json.loads(path.read_text()) if path.exists() else {}
    if not saved.get('selected'):
        raise ValueError('没有上次采集，请用 --batch 指定目录')
    return Path(saved['selected'])


async def run_collection(args):
    from .service import CollectionService
    from .workflow import parse_input
    service = CollectionService(Path.cwd())
    if args.command == 'resume':
        await service.resume_collection(selected_batch(args), args.confirmed_access, include_status=False,
                                        **({'retry_api_once': True} if args.retry_api_once else {}))
    else:
        urls = []
        if args.command == 'posts':
            parsed = parse_input('\n'.join(args.inputs))
            if parsed['short_links'] or parsed['invalid_count']:
                raise ValueError('请粘贴打开帖子后的完整链接（保留访问参数），当前不在后台展开短链接')
            urls = parsed['urls']
        await service.collect(args.inputs if args.command == 'keyword' else [], urls, args.batch, args.cdp, include_status=False)
    print(f'采集目录：{service.selected}', flush=True)
    loop = asyncio.get_running_loop()
    signals = []
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, service.pause)
            signals.append(sig)
        except (NotImplementedError, RuntimeError):
            pass
    previous = None
    try:
        while service.busy:
            # Progress logging must not replay/hash the entire archive on the
            # browser's event loop. Full verification runs at final reporting.
            db = service.store.db
            counts = dict(db.execute("""SELECT count(*) AS discovered,
                coalesce(sum(status='running'),0) AS running,
                coalesce(sum(body='saved'),0) AS bodies_saved,
                coalesce(sum(body='saved' AND comments='complete_observed'
                    AND media='success' AND comment_media='success'),0) AS recorded_complete
                FROM tasks""").fetchone())
            current = (service.runner.message, counts)
            if current != previous:
                print(json.dumps({'message': current[0], **current[1]}, ensure_ascii=False), flush=True)
                previous = current
            await asyncio.sleep(2)
        await service.job
        if service.runner.store.run.get('engine') == 'http_api':
            # CLI termination is a bounded status receipt, not an implicit full
            # archive replay plus a dump of every post/comment into collection.log.
            with closing(sqlite3.connect((service.selected / 'state.sqlite').as_uri() + '?mode=ro', uri=True)) as db:
                db.row_factory = sqlite3.Row
                counts = dict(db.execute("""SELECT count(*) AS discovered,
                    sum(body='saved') AS bodies_saved, sum(comments='complete_observed') AS full_comments,
                    sum(body='saved' AND comments='complete_observed' AND media='success' AND comment_media='success') AS all_parts_complete,
                    sum(NOT(body='unavailable' OR (body='saved' AND comments='complete_observed' AND media='success' AND comment_media='success'))) AS incomplete
                    FROM tasks""").fetchone())
                controls = dict(db.execute("SELECT key,value FROM controls WHERE key IN ('workspace_blocked','api_reply_blocked')"))
            state = {'batch': str(service.selected), 'phase': service.phase, 'message': service.message,
                     'counts': counts, 'deferred_replies': bool(controls.get('api_reply_blocked')),
                     'outcome': 'restricted' if controls.get('workspace_blocked') else 'partial' if counts['incomplete'] else 'complete'}
        else:
            state = service.status()
        print(json.dumps(state, ensure_ascii=False, indent=2))
        return 0 if state['outcome'] == 'complete' else 2
    finally:
        for sig in signals:
            loop.remove_signal_handler(sig)
        await service.close()


def main(argv=None):
    args = parser().parse_args(argv)
    try:
        if args.command in {'keyword', 'posts', 'resume'}:
            return asyncio.run(run_collection(args))
        if args.command == 'pause':
            from .store import atomic_write
            path = selected_batch(args)
            if not (path / 'state.sqlite').is_file():
                raise ValueError('采集目录不存在')
            atomic_write(path / '.pause-requested', b'pause')
            print('已请求安全暂停；在途响应保存后停止')
            return 0
        if args.command == 'export':
            from .results import export_complete
            print(json.dumps(export_complete(selected_batch(args)), ensure_ascii=False))
            return 0
        if args.command == "browser":
            return launch_browser(args.profile, args.port)
        if args.command == "doctor":
            return asyncio.run(doctor(args.cdp))
        if args.command == "status":
            from .results import collection_status
            print(json.dumps(collection_status(selected_batch(args)), ensure_ascii=False, indent=2))
            return 0
        if args.command == "audit":
            from .audit import audit
            for batch in args.batch:
                print(json.dumps({"batch": batch.name, **audit(batch)}, ensure_ascii=False, indent=1))
            return 0
        if args.command == "repair-passive":
            from .repair import repair_report
            print(json.dumps(repair_report(args.batch, args.apply, args.output), ensure_ascii=False))
            return 0
        if args.command == "ack-restriction":
            if not args.confirmed:
                raise ValueError("仅在人工检查恢复后使用 --confirmed；该操作不缩短冷却。")
            if not (args.batch / "state.sqlite").is_file():
                raise ValueError("请选择已有批次")
            store = Store(args.batch)
            try:
                store.control("workspace_blocked", "")
                store.control("workspace_restriction_ack", "yes")
                store.save(None, None, "restriction_acknowledged", b'{}')
            finally:
                store.close()
            print("已记录人工确认；冷却保留，未开始任何任务。Qt 全局访问锁仍需在工作台确认。")
            return 0
        if args.command == "record":
            return asyncio.run(run_record(args))
        return asyncio.run(run_batch(args))
    except ValueError as exc:
        print(f"配置错误：{exc}", file=sys.stderr)
    except ModuleNotFoundError:
        print("缺少采集依赖，请执行 uv sync --extra collection --extra mcp", file=sys.stderr)
    except Exception as exc:
        print(f"采集停止：{type(exc).__name__}；请运行 doctor 并检查批次 diagnostics/ 与 report.json。", file=sys.stderr)
    return 1
