"""Local regressions: anonymous media flow, consent state, and refusal handling."""

import asyncio
from copy import deepcopy
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch

from source.GUI.backend import GuiBackend
from source.GUI.workbench_service import WorkbenchService
from source.module.settings import Settings
from source.module.access import AccessRestricted, refusal_reason
from source.application.request import Html

NOTE = "0" * 24
URL = "https://www.xiaohongshu.com/explore/" + NOTE + "?xsec_token=fixture"


class DownloadRegressions(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.backend = GuiBackend()
        self.backend.settings_manager = Settings(self.root)
        self.backend.settings = deepcopy(Settings.default)
        self.backend.settings["disclaimer_accepted"] = True
        self.backend.task_queue = asyncio.Queue()
        self.host = SimpleNamespace(
            endpoint="http://127.0.0.1:9234",
            connect=AsyncMock(side_effect=AssertionError("unexpected CDP")),
            new_page=AsyncMock(side_effect=AssertionError("unexpected page")),
        )
        self.service = WorkbenchService(self.root, self.backend, self.host)
        self.backend.workbench = self.service
        self.extract = AsyncMock(side_effect=self.download_result)
        self.backend.xhs = SimpleNamespace(
            extract_links=AsyncMock(return_value=[URL]),
            extract=self.extract,
            manager=SimpleNamespace(download_record=False),
            html=SimpleNamespace(access_restriction=None),
        )

    async def download_result(self, *args, **kwargs):
        kwargs["result_callback"]({"success": 1, "fail": 0, "skip": 0})

    async def asyncTearDown(self):
        await self.service.close()
        self.temp.cleanup()

    async def test_paste_download_needs_no_login_no_manual_batch_no_browser(self):
        self.assertIsNone(self.service.store)
        self.assertEqual(self.backend.settings["cookie"], "")
        created = await self.backend.create_tasks(URL)
        self.assertEqual(len(created), 1)
        await self.service.job
        self.extract.assert_awaited_once()
        self.host.connect.assert_not_awaited()
        self.host.new_page.assert_not_awaited()
        self.assertEqual(self.service.store.root.name, "默认工作区")
        row = self.service.snapshot()["rows"][0]
        self.assertEqual(row["media"], "success")
        self.assertEqual(row["comments"], "not_requested")
        self.assertEqual(
            self.service.store.db.execute("SELECT count(*) FROM attempts").fetchone()[
                0
            ],
            0,
        )
        await self.backend.create_tasks(URL)
        self.extract.assert_awaited_once()  # dedup, no new batch or request

    async def test_default_workspace_is_reused_after_restart_without_autorun(self):
        await self.backend.create_tasks(URL)
        await self.service.job
        path = self.service.store.root
        await self.service.close()
        self.service = WorkbenchService(self.root, self.backend, self.host)
        self.backend.workbench = self.service
        self.assertEqual(self.service.store.root, path)
        self.assertFalse(self.service.busy)
        self.assertEqual(self.service.snapshot()["counts"]["downloaded"], 1)

    async def test_batch_listing_does_not_require_consent_and_state_tracks_it(self):
        self.backend.settings["disclaimer_accepted"] = False
        self.assertEqual(await self.service.command("batches"), [])
        self.assertFalse((await self.service.command("state"))["disclaimer_accepted"])
        self.backend.settings["disclaimer_accepted"] = True
        self.assertTrue((await self.service.command("state"))["disclaimer_accepted"])

    async def test_failed_consent_save_does_not_set_accepted(self):
        self.backend.settings["disclaimer_accepted"] = False
        with patch.object(
            self.backend.settings_manager, "update", side_effect=OSError("disk error")
        ):
            with self.assertRaises(OSError):
                await self.backend.accept_disclaimer()
        self.assertFalse((await self.backend.get_disclaimer())["accepted"])

    async def test_consent_is_persisted_and_ordinary_settings_cannot_revoke_it(self):
        self.backend.settings["disclaimer_accepted"] = False
        with patch.object(self.backend, "_start_runtime", new=AsyncMock()):
            await self.backend.accept_disclaimer()
        self.assertTrue(
            json.loads(self.backend.settings_manager.path.read_text())[
                "disclaimer_accepted"
            ]
        )
        with (
            patch.object(self.backend, "_close_xhs", new=AsyncMock()),
            patch.object(self.backend, "_create_xhs", new=AsyncMock()),
            patch.object(self.backend, "_settings_payload", return_value={}),
        ):
            await self.backend.save_settings({"disclaimer_accepted": False})
        self.assertTrue(self.backend.settings["disclaimer_accepted"])

    async def test_reported_refusal_prevents_even_short_link_resolution(self):
        self.service.restrict("account_abnormal")
        with self.assertRaises(ValueError):
            await self.backend.create_tasks("https://xhslink.com/fixture")
        self.backend.xhs.extract_links.assert_not_awaited()
        self.assertIsNone(self.service.store)
        # Changing archive batches does not change the access refusal.
        await self.service.command("create", {"name": "other"})
        self.assertTrue(self.service.blocked)
        await self.service.close()
        self.service = WorkbenchService(self.root, self.backend, self.host)
        self.backend.workbench = self.service
        self.assertTrue(self.service.blocked)


class RefusalTests(unittest.IsolatedAsyncioTestCase):
    def html(self, status, text):
        response = SimpleNamespace(
            status_code=status, text=text, url=URL, raise_for_status=Mock()
        )
        client = SimpleNamespace(get=AsyncMock(return_value=response))
        manager = SimpleNamespace(
            print=None,
            retry=5,
            request_client=client,
            timeout=1,
            proxy=None,
            impersonate=None,
            avg_delay=0,
            get_headers=lambda url: {},
        )
        return Html(manager), client

    async def test_http_461_is_not_retried_and_blocks_followup(self):
        html, client = self.html(461, '{"success":true,"code":300013,"msg":"访问频繁"}')
        notified = Mock()
        html.on_restriction = notified
        with self.assertRaises(AccessRestricted):
            await html.request_url(URL)
        with self.assertRaises(AccessRestricted):
            await html.request_url(URL)
        self.assertEqual(client.get.await_count, 1)
        notified.assert_called_once_with("http_461")

    async def test_200_account_error_page_is_not_retried(self):
        html, client = self.html(
            200, "<html><title>小红书 - 账号状态异常</title></html>"
        )
        with self.assertRaises(AccessRestricted):
            await html.request_url(URL)
        client.get.assert_awaited_once()

    async def test_ordinary_anonymous_page_still_returns_without_consent_to_login(self):
        html, client = self.html(
            200,
            "<html><title>ordinary note</title><body>请登录可查看更多功能</body></html>",
        )
        with patch("source.application.request.sleep_time", new=AsyncMock()):
            result = await html.request_url(URL)
        self.assertIn("ordinary note", result)
        self.assertIsNone(html.access_restriction)
        client.get.assert_awaited_once()

    async def test_user_content_mentioning_account_error_is_not_a_site_refusal(self):
        self.assertIsNone(
            refusal_reason(
                200, "<title>日记</title><article>昨天我遇到账号状态异常</article>", URL
            )
        )
        self.assertIsNone(
            refusal_reason(
                200, '{"data":{"content":"账号状态异常"},"success":true}', URL
            )
        )
        self.assertEqual(
            refusal_reason(200, '{"msg":"账号状态异常","success":false}', URL),
            "business_access_restricted",
        )


if __name__ == "__main__":
    unittest.main()
