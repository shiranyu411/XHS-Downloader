"""Offline tests for batch pacing, restriction cooldown, replay and audit. No network."""
import json
from pathlib import Path
import tempfile
import unittest

from source.collection.config import Config
from source.collection.pacing import Pacer, PacingPause, stored_factor
from source.collection.protocol import Request
from source.collection.replay import chain_key, rebuild, resume_seed, resumable
from source.collection.store import Store, encode

NOTE = "0123456789abcdef01234567"
URL = f"https://www.xiaohongshu.com/explore/{NOTE}?xsec_token=fixture-token"


class FakeTime:
    def __init__(self, start=1_000_000.0):
        self.now = start
        self.slept = []

    def clock(self):
        return self.now

    async def sleep(self, seconds):
        self.slept.append(seconds)
        self.now += seconds


def comment(id_, parent=None):
    c = {"id": id_, "note_id": NOTE, "user_info": {"user_id": "u-" + id_}, "target_comment": None}
    if parent:
        c["root_comment_id"] = parent
    else:
        c.update(sub_comments=[], sub_comment_has_more=False, sub_comment_cursor="")
    return c


def envelope(items, more=False, cursor=""):
    return {"success": True, "code": 0, "data": {"comments": items, "has_more": more, "cursor": cursor}}


class PacerTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()

    def tearDown(self):
        self.temp.cleanup()

    def store(self, **kw):
        return Store(Path(self.temp.name), Config(open_interval=0, **kw))

    async def requests(self, pacer, n):
        times = []
        for _ in range(n):
            await pacer.wait()
            pacer.record()
            times.append(pacer.clock())
        return times

    async def test_interval_floor_comes_from_per_minute_cap(self):
        store = self.store(page_interval=0, max_requests_per_minute=6, session_request_budget=0)
        try:
            t = FakeTime()
            pacer = Pacer(store, t.clock, t.sleep)
            times = await self.requests(pacer, 5)
            gaps = [b - a for a, b in zip(times, times[1:])]
            self.assertTrue(all(g >= 10 - 1e-9 for g in gaps), gaps)
        finally:
            store.close()

    async def test_sliding_window_never_exceeds_cap(self):
        # page_interval small, cap 3/min: the 4th request waits for the window.
        store = self.store(page_interval=1, max_requests_per_minute=3, session_request_budget=0)
        try:
            t = FakeTime()
            pacer = Pacer(store, t.clock, t.sleep)
            pacer.interval = lambda: 1.0  # isolate the window logic from the floor
            times = await self.requests(pacer, 7)
            for i, start in enumerate(times):
                self.assertLessEqual(sum(start <= x < start + 60 for x in times), 3, (i, times))
        finally:
            store.close()

    async def test_session_budget_pauses_and_persists_rest(self):
        store = self.store(page_interval=0, max_requests_per_minute=0, session_request_budget=3, session_rest=600)
        try:
            t = FakeTime()
            pacer = Pacer(store, t.clock, t.sleep)
            await self.requests(pacer, 3)
            with self.assertRaises(PacingPause) as e:
                await pacer.wait()
            self.assertEqual(e.exception.reason, "session_budget_reached")
            # A new process (new Pacer) must still honour the rest period.
            again = Pacer(store, t.clock, t.sleep)
            with self.assertRaises(PacingPause):
                await again.wait()
            t.now += 601
            await again.wait()
            self.assertEqual(again.factor, 1.0)  # a planned rest is not a slowdown
        finally:
            store.close()

    async def test_restriction_doubles_interval_and_escalates_cooldown(self):
        store = self.store(page_interval=15, max_requests_per_minute=0, restriction_cooldown=3600)
        try:
            t = FakeTime()
            pacer = Pacer(store, t.clock, t.sleep)
            self.assertEqual(pacer.interval(), 15)
            until = pacer.restricted(461, 300013)
            self.assertEqual(until, t.now + 3600)
            self.assertEqual(pacer.interval(), 30)
            with self.assertRaises(PacingPause) as e:
                Pacer(store, t.clock, t.sleep).check_cooldown()
            self.assertEqual(e.exception.reason, "access_restricted")
            # Refusals again within 48 h wait 3x, then 12x the configured cooldown.
            t.now = until
            self.assertEqual(pacer.restricted() - t.now, 3 * 3600)
            t.now += 3 * 3600
            for _ in range(3):
                self.assertEqual(pacer.restricted() - t.now, 12 * 3600)
            self.assertEqual(Pacer(store, t.clock, t.sleep).factor, 8.0)  # bounded, persisted
            t.now += 48 * 3600 + 1
            pacer.check_cooldown()  # expired: allowed, but still slow
            # Two clear days later a refusal counts as the first again.
            self.assertEqual(pacer.restricted() - t.now, 3600)
            self.assertEqual(Pacer(store, t.clock, t.sleep).interval(), 15 * 8)
            self.assertEqual(store.control("pacing_restrictions"), "6")
            decisions = store.db.execute("SELECT count(*) FROM events WHERE kind='pacing_decision'").fetchone()[0]
            self.assertEqual(decisions, 6)
        finally:
            store.close()

    async def test_batch_can_choose_a_fixed_restriction_cooldown(self):
        store = self.store(page_interval=15, max_requests_per_minute=0, restriction_cooldown=7200)
        try:
            store.control('pacing_restriction_cooldown_seconds', 3600)
            t = FakeTime()
            pacer = Pacer(store, t.clock, t.sleep)
            self.assertEqual(pacer.restricted() - t.now, 3600)
            t.now += 3600
            self.assertEqual(pacer.restricted() - t.now, 3600)  # no 3x escalation for this batch
        finally:
            store.close()

    async def test_clean_responses_restore_speed_slowly(self):
        store = self.store(page_interval=30, max_requests_per_minute=0)
        try:
            pacer = Pacer(store)
            pacer.restricted()
            factors = []
            for _ in range(1900):
                pacer.succeeded()
                factors.append(pacer.factor)
            self.assertEqual(factors[98], 2.0)
            self.assertAlmostEqual(factors[99], 1.6)
            self.assertEqual(factors[399], 1.0)  # 2x back to 1x takes 400 clean responses
            self.assertEqual(factors[898], 1.0)
            self.assertAlmostEqual(factors[899], 0.9)
            self.assertEqual(factors[-1], 0.75)  # never faster than 0.75x
            self.assertEqual(Pacer(store).factor, 0.75)
            store.control("pacing_factor", 50)
            self.assertEqual(Pacer(store).factor, 8.0)
        finally:
            store.close()

    async def test_researcher_floor_caps_recovered_speed(self):
        store = self.store(page_interval=30, max_requests_per_minute=0)
        try:
            store.control("pacing_factor_floor", 1.6)
            pacer = Pacer(store)
            pacer.restricted()
            for _ in range(1000):
                pacer.succeeded()
            self.assertEqual(pacer.factor, 1.6)  # recovery stops at the chosen pace
            self.assertEqual(Pacer(store).factor, 1.6)
            store.control("pacing_factor", 1.0)
            self.assertEqual(stored_factor(store), 1.6)  # post opens read the same cap
            pacer.restricted()
            self.assertEqual(pacer.factor, 3.2)  # a refusal still slows down past the cap
            store.control("pacing_factor_floor", 0)
            store.control("pacing_factor", 1.0)
            self.assertEqual(stored_factor(store), 1.0)
        finally:
            store.close()

    async def test_last_request_survives_restart(self):
        store = self.store(page_interval=20, max_requests_per_minute=0, session_request_budget=0)
        try:
            t = FakeTime()
            p1 = Pacer(store, t.clock, t.sleep)
            await self.requests(p1, 1)
            t.now += 5
            p2 = Pacer(store, t.clock, t.sleep)
            start = t.now
            await p2.wait()
            self.assertGreaterEqual(t.now - start, 15 - 1e-9)
        finally:
            store.close()

    async def test_checkpoint_interrupts_wait(self):
        store = self.store(page_interval=60, max_requests_per_minute=0, session_request_budget=0)
        try:
            t = FakeTime()
            pacer = Pacer(store, t.clock, t.sleep)
            await self.requests(pacer, 1)

            def stop():
                raise KeyboardInterrupt

            with self.assertRaises(KeyboardInterrupt):
                await pacer.wait(stop)
        finally:
            store.close()


class ReplayTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.store = Store(Path(self.temp.name), Config())
        self.store.enqueue([URL], {})

    def tearDown(self):
        self.store.close()
        self.temp.cleanup()

    def save(self, attempt, req, payload, status=200):
        meta = {"kind": req.kind, "note": req.note, "parent": req.parent, "cursor": req.cursor,
                "path": "/api/sns/web/v2/comment/" + ("sub/page" if req.kind == "replies" else "page"),
                "scope": [list(x) for x in req.scope], "status": status}
        return self.store.save(attempt, NOTE, "network_json", encode(payload), meta)

    def first_attempt(self):
        attempt = self.store.begin(NOTE)
        root = comment("c0")
        root.update(sub_comments=[comment("r0", "c0")], sub_comment_has_more=True, sub_comment_cursor="r1")
        self.save(attempt, Request("comments", NOTE), envelope([root, comment("c1")], True, "p2"))
        self.save(attempt, Request("replies", NOTE, "c0", "r1"), envelope([comment("r1", "c0")], True, "r2"))
        self.save(attempt, Request("replies", NOTE, "c0", "r2"),
                  {"success": True, "code": 300013, "msg": "访问频繁"}, 461)
        self.store.finish(attempt, {"body": "saved", "comments": "blocked", "media": "disabled", "reason": "access_restricted"}, "paused")
        self.store.control(chain_key(NOTE), json.dumps([attempt]))
        return attempt

    def test_restricted_page_is_skipped_and_cursor_resumes(self):
        self.first_attempt()
        ledger, chain, stats = resume_seed(self.store, NOTE)
        self.assertEqual(len(chain), 1)
        self.assertEqual(stats["skipped_restricted"], 1)
        self.assertEqual(ledger.next_request(), Request("replies", NOTE, "c0", "r2"))
        self.assertEqual(set(ledger.comments), {"c0", "c1", "r0", "r1"})

    def test_resumed_chain_completes_across_attempts(self):
        self.first_attempt()
        ledger, chain, _ = resume_seed(self.store, NOTE)
        second = self.store.begin(NOTE)
        self.save(second, Request("replies", NOTE, "c0", "r2"), envelope([comment("r2", "c0")]))
        self.save(second, Request("comments", NOTE, cursor="p2"), envelope([comment("c2")]))
        rebuilt, _ = rebuild(self.store.db, self.store.root, NOTE, chain + [second])
        self.assertTrue(rebuilt.complete())
        self.assertEqual(len(rebuilt.comments), 6)

    def test_changed_first_page_is_not_resumable(self):
        attempt = self.first_attempt()
        # Same cursor, different IDs (new comment arrived): pagination evidence invalid.
        self.save(attempt, Request("comments", NOTE), envelope([comment("c9"), comment("c0")], True, "p2"))
        ledger, chain, _ = resume_seed(self.store, NOTE)
        self.assertEqual(chain, [])
        self.assertEqual(ledger.comments, {})

    def test_corrupt_blob_forces_fresh_start(self):
        self.first_attempt()
        row = self.store.db.execute("SELECT path FROM events WHERE kind='network_json' ORDER BY id LIMIT 1").fetchone()
        (self.store.root / row[0]).write_bytes(b"broken")
        ledger, chain, stats = resume_seed(self.store, NOTE)
        self.assertEqual(stats["unreadable"], 1)
        self.assertEqual(chain, [])

    def test_resumable_requires_evidence(self):
        ledger, _ = rebuild(self.store.db, self.store.root, NOTE, [])
        self.assertFalse(resumable(ledger))


class RunnerCooldownTests(unittest.IsolatedAsyncioTestCase):
    async def test_active_cooldown_refuses_to_connect(self):
        from source.collection.runner import Runner
        with tempfile.TemporaryDirectory() as root:
            store = Store(Path(root), Config())
            try:
                store.enqueue([URL], {})
                Pacer(store).restricted(461, 300013)
                runner = Runner(store)

                async def connect(_):
                    raise AssertionError("must not connect during cooldown")

                runner.connect = connect
                await runner.run()
                self.assertTrue(runner.paused)
                self.assertIn("access_restricted_cooldown", runner.message)
                self.assertEqual(store.db.execute("SELECT count(*) FROM attempts").fetchone()[0], 0)
                self.assertEqual(store.db.execute("SELECT count(*) FROM events WHERE kind='session_config'").fetchone()[0], 1)
            finally:
                store.close()


if __name__ == "__main__":
    unittest.main()
