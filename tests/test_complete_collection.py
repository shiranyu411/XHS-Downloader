"""Essential end-to-end fixtures. These are NOT live-platform acceptance tests."""
import asyncio
import csv
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import tempfile
from threading import Thread
import unittest
from unittest.mock import patch
from urllib.parse import urlsplit

from source.application.download import Download
from source.collection.config import Config
from source.collection.media import register, asset, note_assets
from source.collection.results import collection_status, export_complete, read_post
from source.collection.runner import Runner, Paused
from source.collection.search import add_keywords, discover_keywords, accept_page
from source.collection.store import Store
from tests import test_collection as fixtures
from tests.test_collection import NOTE, URL, UNPACED, comment, envelope

GIF = bytes.fromhex('47494638396101000100800000000000ffffff21f90401000000002c00000000010001000002024401003b')
IMAGE_URL = 'https://sns-img-bd.xhscdn.com/fixture-picture'


class CompleteCollectionTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = fixtures.BrowserTests.asyncSetUp
    asyncTearDown = fixtures.BrowserTests.asyncTearDown

    async def route(self, route):
        self.calls.append(urlsplit(route.request.url).path)
        if urlsplit(route.request.url).hostname == 'www.xiaohongshu.com':
            note = {'noteId': NOTE, 'title': '完整采集', 'desc': '正文🙂', 'type': 'normal',
                    'imageList': [{'urlDefault': IMAGE_URL, 'fileId': 'shared'}]}
            html = '''<script>
              window.__INITIAL_STATE__={note:{noteDetailMap:{'NOTE':{note:NOTE_OBJECT}}}};
              const client={get:url=>fetch(url).then(r=>r.json()),
                post:(url,data)=>fetch(url,{method:'POST',body:JSON.stringify(data)}).then(r=>r.json()),
                makeUri(){}, instance:{},
                interceptors:{request:{},response:{},dispatch:{},result:{}}};
              window.webpackChunkxhs_pc_web={push:c=>c[2]({c:{f:{exports:client}}})};
            </script>'''.replace('NOTE_OBJECT', json.dumps(note)).replace('NOTE', NOTE)
            await route.fulfill(content_type='text/html', body=html)
        elif urlsplit(route.request.url).path == '/api/sns/web/v1/search/notes':
            keyword = json.loads(route.request.post_data)['keyword']
            if keyword == 'fixture-two':
                saved = self.archive_store.tasks()[0]
                self.assertEqual((saved['body'], saved['media'], saved['comments'], saved['comment_media']),
                                 ('saved', 'success', 'complete_observed', 'success'),
                                 'collect the first post before searching the next keyword')
            await route.fulfill(content_type='application/json', headers={'Access-Control-Allow-Origin': '*'},
                                body=json.dumps({'success': True, 'data': {'items': [{'id': NOTE, 'xsec_token': 'fixture'}], 'has_more': False}}))
        else:
            root = comment('root')
            root.update(content='', pictures=[{'url_pre': IMAGE_URL, 'file_id': 'shared'}],
                        sub_comments=[{**comment('reply', 'root'), 'content': '🙂[笑哭R]', 'pictures': [{'url': IMAGE_URL, 'file_id': 'shared'}]}])
            await route.fulfill(content_type='application/json', body=json.dumps(envelope([root])),
                                headers={'Access-Control-Allow-Origin': '*'})

    async def test_complete_post_files_images_relations_export_and_offline_resume(self):
        store = Store(Path(self.temp.name) / 'batch', Config(open_interval=0, page_interval=0,
                                                           max_operations=None, max_scrolls=None, **UNPACED))
        self.archive_store = store
        add_keywords(store, ['fixture-one', 'fixture-two'])
        requests = []
        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                requests.append(self.path)
                self.send_response(200)
                self.send_header('Content-Type', 'image/gif')
                self.send_header('Content-Length', str(len(GIF)))
                self.end_headers()
                self.wfile.write(GIF)
            def log_message(self, *args):
                pass
        server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        thread = Thread(target=server.serve_forever, daemon=True)
        thread.start()
        downloader = Download.for_collection(store.root / 'media')
        original = downloader.download_file
        async def local_download(url, *args):
            return await original(f'http://127.0.0.1:{server.server_port}/image', *args)
        downloader.download_file = local_download
        try:
            runner = Runner(store)
            async def connect(_):
                return self.browser, self.context
            with patch.object(Download, 'for_collection', return_value=downloader), patch.object(runner, 'connect', side_effect=connect):
                await runner.run()
            result = store.tasks()[0]
            self.assertEqual([s['status'] for s in json.loads(store.control('search_progress'))], ['complete', 'complete'])
            self.assertEqual((result['body'], result['media'], result['comments'], result['comment_media']),
                             ('saved', 'success', 'complete_observed', 'success'))
            self.assertEqual(len(requests), 1, 'shared post/comment/reply image downloaded once')
            store.save(None, NOTE, 'network_json', b'{}', {'kind': 'search', 'status': 200})
            post = read_post(store.root, NOTE)
            self.assertEqual(post['outcome'], 'complete')
            self.assertEqual(post['warnings'], [])
            status = collection_status(store.root)
            self.assertEqual(status['counts'], {'discovered': 1, 'complete': 1, 'sampled': 0, 'skipped': 0, 'partial': 0, 'primary': 1, 'replies': 1, 'media': 1})
            self.assertEqual(len(post['comments']), 2)
            reply = next(c for c in post['comments'] if c['comment_id'] == 'reply')
            self.assertEqual((reply['text'], reply['root_comment_id']), ('🙂[笑哭R]', 'root'))
            exported = Path(export_complete(store.root)['folder'])
            with (exported / 'comment_media.csv').open(encoding='utf-8-sig') as stream:
                rows = list(csv.DictReader(stream))
            self.assertEqual(len(rows), 2)
            self.assertEqual((exported / rows[0]['path']).read_bytes(), GIF)
            calls = len(self.calls)
            again = await Runner(store).collect(None, store.tasks()[0])
            self.assertEqual(again['comments'], 'complete_observed')
            self.assertEqual(len(self.calls), calls, 'verified stages resume without a browser request')
            paused = Runner(store)
            paused.pause()
            with self.assertRaises(Paused):
                await paused.collect(None, store.tasks()[0])
            self.assertEqual((store.tasks()[0]['media'], store.tasks()[0]['comment_media']), ('success', 'success'))
            (store.root / post['media_files'][0]['path']).unlink()
            status = collection_status(store.root)
            self.assertEqual(status['outcome'], 'partial')
            self.assertEqual(status['counts']['media'], 0)
            self.assertEqual(status['tasks'][0]['missing'], ['media', 'comment_media'])
            self.assertEqual(store.tasks()[0]['media'], 'success', 'status is a read-only file projection')
            # A comments-only finish cannot erase verified post media.
            attempt = store.begin(NOTE)
            store.finish(attempt, {'body': 'saved', 'comments': 'partial', 'media': 'disabled', 'reason': 'fixture_stop'})
            self.assertEqual(store.tasks()[0]['media'], 'success')
        finally:
            store.close()
            await asyncio.to_thread(server.shutdown)
            server.server_close()

    async def test_search_last_page_dedup_and_resume_after_restriction(self):
        store = Store(Path(self.temp.name) / 'search', Config(open_interval=0, page_interval=0, **UNPACED))
        add_keywords(store, ['家机'])
        blocked = True
        pages = []
        async def search_route(route):
            if urlsplit(route.request.url).hostname == 'www.xiaohongshu.com':
                await route.fulfill(content_type='text/html', body='''<script>
                  const client={get(){},post:(u,p)=>fetch(u,{method:'POST',body:JSON.stringify(p)}).then(r=>r.json()),
                    makeUri(){},instance:{},interceptors:{request:{},response:{},dispatch:{},result:{}}};
                  window.webpackChunkxhs_pc_web={push:c=>c[2]({c:{f:{exports:client}}})};
                </script>''')
            else:
                data = json.loads(route.request.post_data)
                page = data['page']
                pages.append(page)
                items = [{'id': NOTE, 'model_type': 'note', 'xsec_token': 'fixture'}]
                if page == 2:
                    items.append({'id': 'b' * 24, 'model_type': 'note', 'xsec_token': 'fixture-b'})
                payload = {'success': True, 'code': 0, 'data': {'items': items, 'has_more': page == 1}}
                if page == 2 and blocked:
                    payload = {'success': False, 'code': 300011}
                await route.fulfill(content_type='application/json', body=json.dumps(payload), headers={'Access-Control-Allow-Origin': '*'})
        await self.context.unroute('**/*')
        await self.context.route('**/*', search_route)
        try:
            with self.assertRaises(Paused):
                await discover_keywords(Runner(store), self.context)
            self.assertEqual(json.loads(store.control('search_progress'))[0]['next_page'], 2)
            path = store.root
            store.close()
            store = Store(path)
            blocked = False
            await discover_keywords(Runner(store), self.context)
            state = json.loads(store.control('search_progress'))[0]
            self.assertEqual(pages, [1, 2, 2])
            self.assertEqual(len(store.tasks()), 2)
            self.assertEqual((state['status'], state['reason']), ('complete', 'has_more_false'))
            self.assertEqual(len(state['pages'][-1]['ids']), 2)
            state.update(next_page=3, status='running')
            terminal = store.save(None, None, 'network_json', json.dumps(
                {'success': True, 'code': 0, 'data': {'has_more': False}}).encode(),
                {'status': 200, 'post_data': json.dumps({'keyword': '家机', 'page': 3})})
            accept_page(store, state, store.db.execute('SELECT * FROM events WHERE id=?', (terminal,)).fetchone())
            self.assertEqual((state['status'], state['end_event']), ('complete', terminal))
            self.assertEqual(state['pages'][-1]['ids'], [])
            self.assertEqual(len(store.tasks()), 2)
        finally:
            store.close()

    async def test_each_search_page_takes_a_turn_at_the_post_open_gate(self):
        store = Store(Path(self.temp.name) / 'gate', Config(open_interval=0, page_interval=0, **UNPACED))
        add_keywords(store, ['家机'])
        order = []
        async def search_route(route):
            if urlsplit(route.request.url).hostname == 'www.xiaohongshu.com':
                await route.fulfill(content_type='text/html', body='''<script>
                  const client={get(){},post:(u,p)=>fetch(u,{method:'POST',body:JSON.stringify(p)}).then(r=>r.json()),
                    makeUri(){},instance:{},interceptors:{request:{},response:{},dispatch:{},result:{}}};
                  window.webpackChunkxhs_pc_web={push:c=>c[2]({c:{f:{exports:client}}})};
                </script>''')
            else:
                page = json.loads(route.request.post_data)['page']
                order.append(f'page {page}')
                items = [{'id': f'{page:024x}', 'model_type': 'note', 'xsec_token': 'fixture'}]
                await route.fulfill(content_type='application/json', headers={'Access-Control-Allow-Origin': '*'},
                                    body=json.dumps({'success': True, 'code': 0, 'data': {'items': items, 'has_more': page == 1}}))
        await self.context.unroute('**/*')
        await self.context.route('**/*', search_route)
        runner = Runner(store)
        wait = runner.gate.wait
        async def gate(paused):
            order.append('gate')
            await wait(paused)
        runner.gate.wait = gate
        try:
            await discover_keywords(runner, self.context)
            # Page views share one gap: the search page itself, then every results page, like a post open.
            self.assertEqual(order, ['gate', 'gate', 'page 1', 'gate', 'page 2'])
        finally:
            store.close()

    async def test_search_bootstrap_without_end_flag_stops_after_saving_items(self):
        store = Store(Path(self.temp.name) / 'unknown-end', Config(open_interval=0, page_interval=0, **UNPACED))
        add_keywords(store, ['家机'])
        async def route(route):
            if urlsplit(route.request.url).hostname == 'www.xiaohongshu.com':
                await route.fulfill(content_type='text/html; charset=utf-8', body='''<script>
                  const client={get(){},post:(u,p)=>fetch(u,{method:'POST',body:JSON.stringify(p)}).then(r=>r.json()),
                    makeUri(){},instance:{},interceptors:{request:{},response:{},dispatch:{},result:{}}};
                  window.webpackChunkxhs_pc_web={push:c=>c[2]({c:{f:{exports:client}}})};
                  client.post('https://edith.xiaohongshu.com/api/sns/web/v1/search/notes',
                    {keyword:'家机',page:1,page_size:30,sort:'time_descending',note_type:0,search_id:'fixture'});
                </script>''')
            else:
                await route.fulfill(content_type='application/json', headers={'Access-Control-Allow-Origin':'*'},
                                    body=json.dumps({'success': True, 'data': {'items': [{'id': NOTE}]}}))
        await self.context.unroute('**/*')
        await self.context.route('**/*', route)
        try:
            await discover_keywords(Runner(store), self.context)
            state = json.loads(store.control('search_progress'))[0]
            self.assertEqual((state['status'], state['reason']), ('partial', 'missing_has_more'))
            self.assertEqual((state['page_size'], state['sort']), (30, 'time_descending'))
            self.assertEqual(len(store.tasks()), 1)
            self.assertEqual(store.db.execute("SELECT count(*) FROM events WHERE kind='operation_started'").fetchone()[0], 0)
            closed = await self.context.new_page()
            await closed.close()
            with patch.object(self.context, 'new_page', return_value=closed):
                await discover_keywords(Runner(store), self.context)
            state = json.loads(store.control('search_progress'))[0]
            self.assertEqual((state['status'], state['reason']), ('partial', 'browser_connection_error'))
            self.assertEqual(len(store.tasks()), 1, 'browser loss must preserve discovered links')
        finally:
            store.close()


class FileIntegrityTests(unittest.IsolatedAsyncioTestCase):
    def test_video_preserves_preferred_and_fallback_addresses(self):
        video = {'type': 'video', 'video': {'media': {'stream': {'h264': [
            {'height': 720, 'masterUrl': 'https://sns-video-v2.xhscdn.com/master.mp4',
             'backupUrls': ['https://sns-bak-v1.xhscdn.com/backup.mp4', 'https://sns-bak-v6.xhscdn.com/backup.mp4']}
        ]}}}}
        item = note_assets(video)[0]
        self.assertEqual(item['kind'], 'video')
        self.assertEqual(item['urls'], ['https://sns-bak-v1.xhscdn.com/backup.mp4',
                                       'https://sns-video-v2.xhscdn.com/master.mp4',
                                       'https://sns-bak-v6.xhscdn.com/backup.mp4'])

    async def test_deleted_file_is_not_hidden_by_download_history(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            store = Store(root)
            store.enqueue([URL], {})
            register(store, NOTE, '', [asset({'url': IMAGE_URL}, 'image')])
            with store.db:
                store.db.execute("UPDATE media_files SET status='saved',path='missing.jpeg',sha256='old',size=12")
                store.db.execute("UPDATE tasks SET status='finished',media='success'")
            store.close()
            store = Store(root)
            try:
                self.assertEqual(store.tasks()[0]['media'], 'partial')
                self.assertEqual(store.next_task()['note_id'], NOTE)
            finally:
                store.close()

    async def test_server_ignoring_range_replaces_partial_file(self):
        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                self.send_response(200)
                self.send_header('Content-Type', 'image/gif')
                self.send_header('Content-Length', str(len(GIF)))
                self.end_headers()
                self.wfile.write(GIF)
            def log_message(self, *args):
                pass
        server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        Thread(target=server.serve_forever, daemon=True).start()
        try:
            with tempfile.TemporaryDirectory() as temp:
                downloader = Download.for_collection(Path(temp))
                (downloader.temp / 'test.jpeg').write_bytes(b'broken partial')
                try:
                    result = await downloader.download_file(f'http://127.0.0.1:{server.server_port}/image', Path(temp) / 'files', 'test')
                    self.assertEqual(Path(result['path']).read_bytes(), GIF)
                finally:
                    await downloader.manager.request_client.close()
                    await downloader.manager.download_client.close()
        finally:
            await asyncio.to_thread(server.shutdown)
            server.server_close()
