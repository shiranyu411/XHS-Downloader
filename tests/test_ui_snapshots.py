"""Snapshot cache tests: no browser, network, or personal archive access."""

import asyncio
import json
from pathlib import Path
import sqlite3
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from source.collection.workspace import Workspace
from source.GUI.workbench_service import WorkbenchService

NOTE = "0" * 24
URL = f"https://www.xiaohongshu.com/explore/{NOTE}"


class SnapshotTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.backend = SimpleNamespace(settings={"disclaimer_accepted": True}, tasks={}, files={})
        self.service = WorkbenchService(self.root, self.backend, SimpleNamespace(endpoint="http://127.0.0.1:9234"))
        self.store = self.service.store = Workspace(self.root / "batch")
        self.store.enqueue([URL], {"mode": "fixture"})

    def tearDown(self):
        self.service.store.close()
        self.temp.cleanup()

    def test_idle_summary_and_pages_share_one_overview(self):
        with patch.object(self.store, "overview", wraps=self.store.overview) as overview:
            for _ in range(10):
                self.assertEqual(self.service.summary()["counts"]["total"], 1)
                self.assertEqual(len(self.service.snapshot()["rows"]), 1)
                self.assertEqual(self.service.snapshot(query="not-found")["filtered"], 0)
            self.assertEqual(overview.call_count, 1)
        summary = self.service.summary()
        self.assertFalse(set(summary) & {"rows", "sources", "logs", "progress", "all_rows"})

    def test_writes_sources_jobs_progress_and_hide_invalidate(self):
        first = self.service.snapshot()
        self.assertEqual(first["rows"][0]["observations"], 1)
        self.store.enqueue([URL], {"mode": "another"})
        self.assertEqual(self.service.snapshot()["rows"][0]["observations"], 2)
        self.store.submit([NOTE], "download")
        self.assertEqual(self.service.summary()["queues"]["download"], 1)
        self.store.job_state(NOTE, "download", "running")
        self.assertEqual(self.service.summary()["queues"]["download"], 0)
        with self.store.db:
            self.store.db.execute("UPDATE tasks SET media='success'")
            self.store.db.execute(
                "INSERT INTO progress VALUES(?,?)",
                ("fixture", json.dumps({"primary": {"ids": ["p"]}, "replies": {"p": {"ids": ["r1", "r2"]}}})),
            )
        self.assertEqual(self.service.snapshot()["progress"], {"primary": 1, "replies": 2})
        self.assertEqual(self.service.summary()["counts"]["downloaded"], 1)
        self.store.job_state(NOTE, "download", "success")
        self.store.hide([NOTE])
        self.assertEqual(self.service.snapshot()["rows"], [])
        self.store.restore_hidden()
        self.assertEqual(len(self.service.snapshot()["rows"]), 1)
        self.assertEqual(first["rows"][0]["observations"], 1)  # old snapshots not mutated

    def test_external_commit_and_batch_change_invalidate(self):
        self.service.snapshot()
        with sqlite3.connect(self.store.root / "state.sqlite") as db:
            db.execute("UPDATE workspace_items SET title='external title'")
        self.assertEqual(self.service.snapshot(query="EXTERNAL")["filtered"], 1)
        self.service.select(self.root / "second", create=True)
        self.assertEqual(self.service.snapshot()["rows"], [])

    def test_non_database_state_is_never_cached(self):
        self.service.snapshot()
        self.backend.settings["disclaimer_accepted"] = False
        self.service.log("fresh message")
        self.service.access_reason = "http_461"
        state = self.service.snapshot()
        self.assertFalse(state["disclaimer_accepted"])
        self.assertTrue(state["blocked"])
        self.assertIn("http_461", state["message"])
        self.assertEqual(state["logs"][-1]["message"], "fresh message")

    def test_bad_source_metadata_still_counts_as_observation(self):
        with self.store.db:
            self.store.db.execute("INSERT INTO sources(note_id,metadata) VALUES(?,?)", (NOTE, "invalid"))
        row = self.service.snapshot()["rows"][0]
        self.assertEqual(row["observations"], 2)
        self.assertEqual(row["sources"], ["fixture"])

    def test_legacy_collection_is_not_read_in_integrated_mode(self):
        from source.GUI.backend import GuiBackend
        from source.module import Settings

        backend = GuiBackend()
        backend.settings = dict(Settings.default)
        backend.workbench = self.service
        backend.collection.snapshot = Mock(side_effect=AssertionError("unused legacy service must not be polled"))
        state = asyncio.run(backend.snapshot())
        self.assertIsNone(state["collection"])
        backend.collection.snapshot.assert_not_called()


if __name__ == "__main__":
    unittest.main()
