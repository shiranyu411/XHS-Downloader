"""CDN object failures remain missing assets, not account-wide restrictions."""
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock

from source.collection.media import MediaCollector, asset, register
from source.collection.store import Store
from source.module.access import AccessRestricted


class CdnFailureScopeTests(unittest.IsolatedAsyncioTestCase):
    async def test_denied_cdn_object_is_recorded_without_retry_or_batch_stop(self):
        for code in (401, 403):
            with self.subTest(code=code), tempfile.TemporaryDirectory() as root:
                store = Store(Path(root))
                try:
                    note = 'a' * 24
                    register(store, note, 'comment', [asset({
                        'url': 'https://sns-img.xhscdn.com/denied',
                        'backupUrls': ['https://sns-img.xhscdn.com/backup'],
                    }, 'comment_image')])
                    downloader = AsyncMock()
                    downloader.download_file.side_effect = AccessRestricted(f'media_http_{code}')
                    collector = MediaCollector(store, downloader=downloader)
                    self.assertEqual(await collector.download(note, comments=True), 'partial')
                    self.assertEqual(downloader.download_file.await_count, 1)
                    row = store.db.execute('SELECT status, reason FROM media_files').fetchone()
                    self.assertEqual(tuple(row), ('failed', f'media_http_{code}'))
                    self.assertIsNone(store.control('workspace_blocked'))
                    self.assertEqual(store.db.execute("SELECT count(*) FROM events WHERE kind='media_file_result'").fetchone()[0], 1)
                finally:
                    store.close()

    async def test_rate_limits_and_non_cdn_denials_still_propagate(self):
        for host, code in [('sns-img.xhscdn.com', 429), ('sns-img.xhscdn.com', 461),
                           ('www.xiaohongshu.com', 403)]:
            with self.subTest(host=host, code=code), tempfile.TemporaryDirectory() as root:
                store = Store(Path(root))
                try:
                    note = 'b' * 24
                    register(store, note, '', [asset({'url': f'https://{host}/image'}, 'image')])
                    downloader = AsyncMock()
                    downloader.download_file.side_effect = AccessRestricted(f'media_http_{code}')
                    with self.assertRaises(AccessRestricted):
                        await MediaCollector(store, downloader=downloader).download(note, comments=False)
                finally:
                    store.close()
