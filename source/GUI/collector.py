"""Small local desktop surface over CollectionService, with no second job engine."""
from contextlib import asynccontextmanager, closing
from pathlib import Path
import secrets
import os
import socket
import subprocess
import sys
import threading
import time

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from ..collection.service import CollectionService
from ..collection.results import open_snapshot, read_post
from ..collection.media import verified_file
from ..collection.workflow import parse_input

STATIC = Path(__file__).resolve().parents[2] / 'static' / 'GUI'


def create_app(root, service=None):
    service = service or CollectionService(root)
    token = secrets.token_urlsafe(32)

    @asynccontextmanager
    async def lifespan(app):
        yield
        await service.close()

    app = FastAPI(lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)
    app.state.collection = service
    app.state.export_folder = None

    @app.middleware('http')
    async def local_access(request, call_next):
        host = request.url.hostname
        if host not in {'127.0.0.1', 'localhost', '::1', 'testserver'}:
            return JSONResponse({'error': '仅允许本机访问'}, status_code=403)
        if request.url.path.startswith(('/api/', '/media/')):
            if request.cookies.get('xhs_local') != token:
                return JSONResponse({'error': '请从本机采集窗口打开'}, status_code=403)
            if request.method != 'GET' and request.headers.get('x-xhs-local') != '1':
                return JSONResponse({'error': '无效的本机操作'}, status_code=403)
            origin = request.headers.get('origin')
            if origin and origin != str(request.base_url).rstrip('/'):
                return JSONResponse({'error': '无效的页面来源'}, status_code=403)
        response = await call_next(request)
        response.headers['X-Content-Type-Options'] = 'nosniff'
        response.headers['Referrer-Policy'] = 'no-referrer'
        response.headers['Cache-Control'] = 'no-store'
        response.headers['Content-Security-Policy'] = "default-src 'self'; img-src 'self' data:; media-src 'self'; script-src 'self'; style-src 'self'; frame-ancestors 'none'; base-uri 'none'"
        return response

    @app.exception_handler(ValueError)
    async def invalid(request, exc):
        return JSONResponse({'error': str(exc)}, status_code=400)

    @app.get('/')
    async def index():
        response = HTMLResponse((STATIC / 'collector.html').read_text())
        response.set_cookie('xhs_local', token, httponly=True, samesite='strict')
        return response

    @app.get('/api/state')
    def state():
        return {**service.status(), 'batches': service.batches(), 'endpoint': service.endpoint}

    @app.post('/api/collect')
    async def collect(request: Request):
        payload = await request.json()
        text = str(payload.get('text', '')).strip()
        if not text:
            raise ValueError('请输入关键词或帖子链接')
        parsed = parse_input(text)
        if parsed['short_links']:
            raise ValueError('请在浏览器打开短链接，再复制地址栏里的完整帖子链接')
        if parsed['invalid_count']:
            raise ValueError('包含无法识别的链接，请检查后再开始')
        keywords = [] if parsed['urls'] else [k.strip() for k in text.splitlines() if k.strip()]
        return await service.collect(keywords, parsed['urls'], cdp=payload.get('cdp'))

    @app.post('/api/pause')
    async def pause():
        service.pause()
        return service.status()

    @app.post('/api/resume')
    async def resume(request: Request):
        payload = await request.json()
        return await service.resume_collection(confirmed_access=payload.get('confirmed_access', False))

    @app.post('/api/load')
    async def load(request: Request):
        payload = await request.json()
        service.load(payload.get('path', ''))
        return service.status()

    @app.post('/api/export')
    def export():
        result = service.export_collection()
        app.state.export_folder = Path(result['folder'])
        return result

    @app.post('/api/open-export')
    async def open_export():
        folder = app.state.export_folder
        if folder is None or not folder.is_dir():
            raise ValueError('请先导出资料')
        if sys.platform == 'win32':
            os.startfile(folder)
        else:
            subprocess.Popen(['open' if sys.platform == 'darwin' else 'xdg-open', str(folder)])
        return {'opened': True}

    @app.post('/api/browser')
    async def browser(request: Request):
        payload = await request.json()
        await service.open_browser(payload.get('cdp') or service.endpoint)
        return {'message': '已打开小红书，请完成登录后返回采集'}

    @app.get('/api/post/{note_id}')
    def detail(note_id: str):
        if not service.selected:
            raise ValueError('请先选择资料')
        return read_post(service.selected, note_id)

    @app.get('/media/{note_id}/{media_id}')
    def media(note_id: str, media_id: str):
        if not service.selected:
            raise HTTPException(404)
        with closing(open_snapshot(service.selected)) as db:
            row = db.execute("SELECT * FROM media_files WHERE note_id=? AND media_id=? AND status='saved' LIMIT 1", (note_id, media_id)).fetchone()
        path = verified_file(service.selected, dict(row)) if row else None
        if not path:
            raise HTTPException(404, '本地文件缺失或校验失败，请补采')
        return FileResponse(path)

    app.mount('/assets', StaticFiles(directory=STATIC), name='assets')
    return app


def launch(data_root=None):
    import uvicorn
    import webview
    from ..module.static import ROOT
    root = Path(data_root or ROOT).resolve()
    sock = socket.socket()
    sock.bind(('127.0.0.1', 0))
    port = sock.getsockname()[1]
    app = create_app(root)
    server = uvicorn.Server(uvicorn.Config(app, host='127.0.0.1', port=port, log_level='warning'))
    thread = threading.Thread(target=lambda: server.run(sockets=[sock]), daemon=True)
    thread.start()
    deadline = time.monotonic() + 10
    while not server.started and thread.is_alive() and time.monotonic() < deadline:
        time.sleep(.05)
    if not server.started:
        server.should_exit = True
        raise RuntimeError('本地界面服务启动失败')
    webview.create_window('小红书采集 · XHS-Downloader', f'http://127.0.0.1:{port}',
                          width=1200, height=820, min_size=(720, 600), text_select=True)
    try:
        webview.start()
    finally:
        server.should_exit = True
        thread.join(timeout=40)
