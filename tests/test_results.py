"""Read-only projection, evidence boundaries and safe export. No real archives."""
import csv
from dataclasses import asdict
import json
from pathlib import Path
import tempfile
import unittest

from source.collection.protocol import Request
from source.collection.results import export_archive, read_detail, safe_cell
from source.collection.store import encode
from source.collection.workspace import Workspace

NOTE, OTHER = "a" * 24, "b" * 24
COMMENT = "123456789012345678901234567890"


def response(comments):
    return {"success": True, "code": 0, "data": {"note_id": NOTE, "comments": comments, "has_more": False, "cursor": ""}}


class ArchiveResults(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name) / "batch"
        self.store = Workspace(self.root)
        self.store.enqueue([f"https://www.xiaohongshu.com/explore/{n}?xsec_token=secret-fixture" for n in (NOTE, OTHER)], {})
        self.attempt = self.store.begin(NOTE)
        self.note = {"noteId": NOTE, "title": "<img src=x onerror=alert(1)>", "desc": "=SUM(1,2)\n![remote](https://invalid/x)",
                     "user": {"userId": "12345678901234567890", "nickname": "作者"}}
        self.store.save(self.attempt, NOTE, "note_object", encode(self.note))
        self.root_comment = {"id": COMMENT, "note_id": NOTE, "content": "=HYPERLINK(\"https://invalid\")",
                             "sub_comment_count": 0, "sub_comments": [], "sub_comment_has_more": False, "sub_comment_cursor": ""}
        self.store.save(self.attempt, NOTE, "network_json", encode(response([self.root_comment])),
                        {**asdict(Request("comments", NOTE)), "status": 200})
        self.store.finish(self.attempt, {"body": "saved", "comments": "complete_observed", "media": "disabled", "reason": "observed_end"})
        self.store.control(f"resume_chain:{NOTE}", encode([self.attempt]).decode())

    def tearDown(self):
        self.store.close()
        self.temp.cleanup()

    def cutoff(self):
        return self.store.db.execute("SELECT max(id) FROM events").fetchone()[0]

    def passive(self, comment=None, kind="comments", parent=None):
        self.store.save(None, NOTE, "network_json", encode(response([comment or {**self.root_comment, "content": "changed text"}])),
                        {"mode": "passive", "kind": kind, "note_id": NOTE, "root_comment_id": parent, "status": 200, "session_id": "fixture-session"})

    def test_content_is_readable_and_dated_observations_do_not_become_one_snapshot(self):
        self.passive()
        before = self.store.db.total_changes
        result = read_detail(self.root, NOTE)
        self.assertTrue(result["active_complete_verified"])
        self.assertEqual(result["bodies"][0]["text"], self.note["desc"])
        self.assertEqual(result["comment_total"], 2)
        self.assertEqual([c["source"] for c in result["comments"]], ["active", "passive"])
        self.assertEqual([c["comment_id"] for c in result["comments"]], [COMMENT, COMMENT])
        self.assertEqual(result["comments"][1]["text"], "changed text")
        self.assertEqual(self.store.db.total_changes, before)
        self.assertNotIn("xsec_token", json.dumps(result))

    def test_unknown_and_conflicting_reply_roots_are_not_invented(self):
        self.passive({"id": "orphan", "content": "text"}, "replies")
        self.passive({"id": "wrong-root", "root_comment_id": "other", "target_comment": {"id": "reply-to-reply"}}, "replies", "claimed")
        comments = read_detail(self.root, NOTE, source="passive")["comments"]
        self.assertEqual([c["root_comment_id"] for c in comments], ["", ""])
        self.assertEqual(comments[0]["relationship_issue"], "reply_parent_unconfirmed")
        self.assertEqual(comments[1]["relationship_issue"], "reply_parent_mismatch")
        self.assertEqual(comments[1]["target_comment_id"], "reply-to-reply")
        self.assertEqual(comments[1]["text"], "")
        self.assertIn("text", comments[1]["missing"])

    def test_historical_attempt_is_not_merged_into_current_chain(self):
        next_attempt = self.store.begin(NOTE)
        self.store.save(next_attempt, NOTE, "note_object", encode({**self.note, "desc": "current"}))
        self.store.control(f"resume_chain:{NOTE}", encode([next_attempt]).decode())
        result = read_detail(self.root, NOTE)
        self.assertEqual(result["comment_total"], 0)
        self.assertFalse(result["active_complete_verified"])
        self.assertEqual(read_detail(self.root, NOTE, source="history")["comment_total"], 1)
        self.assertEqual(result["bodies"][0]["text"], "current")

    def test_attempt_chain_keeps_saved_order_instead_of_sorting_random_ids(self):
        chain = sorted([self.attempt, self.store.begin(NOTE)], reverse=True)
        self.store.control(f"resume_chain:{NOTE}", encode(chain).decode())
        self.assertEqual(read_detail(self.root, NOTE)["active_chain"], chain)

    def test_html_only_is_not_claimed_to_be_readable_body(self):
        self.store.save(None, OTHER, "document_html", b"<html><script>danger()</script></html>", {"mode": "passive"})
        result = read_detail(self.root, OTHER)
        self.assertEqual(result["bodies"], [])
        self.assertIn("html_only_no_readable_body", result["warnings"])
        self.assertFalse(result["active_complete_verified"])

    def test_corrupt_blob_is_reported_without_mutating_archive_or_success_flag(self):
        row = self.store.db.execute("SELECT * FROM events WHERE kind='network_json'").fetchone()
        (self.root / row["path"]).write_bytes(b"corrupt")
        before = self.store.db.total_changes
        result = read_detail(self.root, NOTE)
        self.assertFalse(result["active_complete_verified"])
        self.assertEqual(result["comment_total"], 0)
        self.assertGreater(result["warning_count"], 0)
        self.assertEqual(self.store.db.total_changes, before)
        self.assertEqual(self.store.tasks()[0]["comments"], "complete_observed")

    def test_csv_export_preserves_scope_ids_text_and_missing_notes(self):
        self.passive()
        before = self.store.db.total_changes
        result = export_archive(self.root, [NOTE, OTHER], "content", self.cutoff())
        folder = Path(result["folder"])
        with (folder / "comments.csv").open(encoding="utf-8-sig", newline="") as file:
            comments = list(csv.DictReader(file))
        self.assertEqual(len(comments), 2)
        self.assertEqual(comments[0]["comment_id"], "'" + COMMENT)
        self.assertTrue(comments[0]["text"].startswith("'="))
        with (folder / "summary.csv").open(encoding="utf-8-sig", newline="") as file:
            summary = list(csv.DictReader(file))
        self.assertEqual(len(summary), 2)
        self.assertIn("no_readable_body", summary[1]["warnings"])
        self.assertEqual(self.store.db.total_changes, before)
        self.assertEqual(json.loads((folder / "export.json").read_text())["cutoff_event_id"], self.cutoff())
        self.assertFalse(list((self.root / "exports").glob(".export-*")))

    def test_full_links_are_only_in_explicit_advanced_export(self):
        normal = export_archive(self.root, [NOTE], "links", self.cutoff())
        self.assertNotIn("xsec_token", (Path(normal["folder"]) / "links.txt").read_text())
        advanced = export_archive(self.root, [NOTE], "full_links", self.cutoff())
        self.assertIn("xsec_token", (Path(advanced["folder"]) / "links.txt").read_text())

    def test_markdown_escapes_raw_html_and_remote_image_syntax(self):
        result = export_archive(self.root, [NOTE], "md", self.cutoff())
        content = next(Path(result["folder"]).glob("*.md")).read_text()
        self.assertNotIn("<img", content)
        self.assertNotIn("![remote]", content)
        self.assertIn("&lt;img", content)
        self.assertIn("观察时间", content)

    def test_stale_cutoff_and_hidden_scope_refuse_without_publishing_partial_output(self):
        cutoff = self.cutoff()
        self.passive()
        with self.assertRaisesRegex(ValueError, "截止点"):
            export_archive(self.root, [NOTE], "content", cutoff)
        self.store.hide([OTHER])
        with self.assertRaises(ValueError):
            export_archive(self.root, [NOTE, OTHER], "content", self.cutoff())
        self.assertEqual(list((self.root / "exports").iterdir()), [])

    def test_passive_snake_case_body_and_conflicting_card_identity(self):
        data = {'success':True, 'data':{'items':[{'id':NOTE, 'note_card':{'note_id':NOTE, 'title':'被动正文',
                'desc':'观察内容', 'user':{'user_id':'123','nickname':'作者'}, 'interact_info':{'comment_count':'9'}}}]}}
        self.store.save(None, NOTE, 'network_json', encode(data), {'mode':'passive','kind':'detail','note_id':NOTE,'status':200})
        result = read_detail(self.root, NOTE, source='passive')
        self.assertEqual(result['bodies'][0]['text'], '观察内容')
        self.assertEqual(result['bodies'][0]['comment_count'], '9')
        data['data']['items'][0]['note_card']['note_id'] = OTHER
        self.store.save(None, NOTE, 'network_json', encode(data), {'mode':'passive','kind':'detail','note_id':NOTE,'status':200})
        result = read_detail(self.root, NOTE, source='passive')
        self.assertEqual(result['body_total'], 1)
        self.assertGreater(result['warning_count'], 0)

    def test_blob_path_cannot_escape_the_current_archive(self):
        before = self.store.db.total_changes
        with self.store.db:
            self.store.db.execute("UPDATE events SET path='../elsewhere.gz' WHERE kind='network_json'")
        result = read_detail(self.root, NOTE)
        self.assertFalse(result['active_complete_verified'])
        self.assertGreater(result['warning_count'], 0)
        self.assertEqual(self.store.db.total_changes, before+1)

    def test_pagination_and_formula_prefix_whitespace(self):
        for i in range(52):
            self.passive({"id": str(i), "content": "identical"})
        result = read_detail(self.root, NOTE, page=2)
        self.assertEqual(result["comment_total"], 53)
        self.assertEqual(len(result["comments"]), 3)
        self.assertEqual(safe_cell(" \t=2+2"), "' \t=2+2")
        self.assertEqual(safe_cell("+1"), "'+1")
        self.assertEqual(safe_cell("ordinary"), "ordinary")
