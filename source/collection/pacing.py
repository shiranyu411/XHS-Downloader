"""Persisted controlled-action pacing. A click on Continue is not a new session."""
import asyncio
from collections import deque
import json
import time
from uuid import uuid4


class PacingPause(Exception):
    def __init__(self, reason, until=None):
        super().__init__(reason)
        self.reason, self.until = reason, until


MAX_FACTOR = 8.0
# Below 1x only after long clean runs; the per-minute cap still bounds it.
MIN_FACTOR = 0.75
# Default cooldown multiples for batches that do not choose a fixed override.
ESCALATION = (1, 3, 12)
RESTRICTION_WINDOW = 48 * 3600


def factor_floor(store):
    """The lowest factor (highest speed) the researcher allows; 0 means no cap."""
    return float(store.control("pacing_factor_floor") or 0)


def stored_factor(store):
    return min(MAX_FACTOR, max(MIN_FACTOR, factor_floor(store), float(store.control("pacing_factor") or 1)))


class Pacer:
    def __init__(self, store, clock=time.time, sleep=asyncio.sleep):
        self.store, self.config, self.clock, self.sleep = store, store.config, clock, sleep
        self.window = deque(json.loads(store.control("pacing_window") or "[]"))
        self.session_count = int(store.control("pacing_session_count") or 0)
        self.session_id = store.control("pacing_session_id") or uuid4().hex
        self.rest_until = float(store.control("pacing_session_rest_until") or 0)
        self.last = float(store.control("pacing_last_request") or 0)
        self.factor = stored_factor(store)
        self.success_streak = int(store.control('pacing_success_streak') or 0)

    def interval(self):
        floor = 60.0 / self.config.max_requests_per_minute if self.config.max_requests_per_minute else 0.0
        return max(self.config.page_interval, floor) * self.factor

    def cooldown_until(self):
        return max(self.rest_until, float(self.store.control("pacing_cooldown_until") or 0))

    def state(self):
        return {"factor": self.factor, "interval": self.interval(), "session_id": self.session_id,
                "session_count": self.session_count, "session_budget": self.config.session_request_budget,
                "cooldown_until": self.cooldown_until(),
                "cooldown_reason": self.store.control("pacing_cooldown_reason") or "",
                "restrictions": int(self.store.control("pacing_restrictions") or 0)}

    def _persist(self, extra=None):
        values = {"pacing_window": json.dumps(list(self.window)), "pacing_session_count": self.session_count,
                  "pacing_session_id": self.session_id, "pacing_session_rest_until": self.rest_until,
                  "pacing_last_request": self.last, **(extra or {})}
        with self.store.db:
            self.store.db.executemany("INSERT OR REPLACE INTO controls VALUES(?,?)",
                                     [(k, str(v)) for k, v in values.items()])

    def check_cooldown(self):
        until = self.cooldown_until()
        if until > self.clock():
            raise PacingPause(self.store.control("pacing_cooldown_reason") or "cooldown_active", until)

    async def wait(self, checkpoint=lambda: None):
        checkpoint()
        self.check_cooldown()
        # A session ends only after its recorded rest, including rest=0.
        if self.rest_until and self.clock() >= self.rest_until:
            self.session_count, self.rest_until, self.session_id = 0, 0, uuid4().hex
            self._persist()
        budget = self.config.session_request_budget
        if budget and self.session_count >= budget:
            self.start_cooldown("session_budget_reached", self.config.session_rest)
        while True:
            checkpoint()
            now = self.clock()
            while self.window and self.window[0] <= now - 60:
                self.window.popleft()
            ready = self.last + self.interval()
            cap = self.config.max_requests_per_minute
            if cap and len(self.window) >= cap:
                ready = max(ready, self.window[0] + 60)
            if ready <= now:
                return
            await self.sleep(min(0.25, ready - now))

    def record(self):
        now = self.clock()
        while self.window and self.window[0] <= now - 60:
            self.window.popleft()
        self.window.append(now)
        self.session_count += 1
        self.last = now
        extra = {}
        budget = self.config.session_request_budget
        if budget and self.session_count >= budget:
            self.rest_until = now + self.config.session_rest
            extra = {"pacing_cooldown_until": max(self.cooldown_until(), self.rest_until),
                     "pacing_cooldown_reason": "session_budget_reached"}
        self._persist(extra)

    def start_cooldown(self, reason, seconds):
        until = max(self.cooldown_until(), self.clock() + seconds)
        if reason == "session_budget_reached":
            self.rest_until = until
        self._persist({"pacing_cooldown_until": until, "pacing_cooldown_reason": reason})
        self.log(reason)
        raise PacingPause(reason, until)

    def recent_restrictions(self):
        now = self.clock()
        return [t for t in json.loads(self.store.control("pacing_restriction_log") or "[]") if t > now - RESTRICTION_WINDOW]

    def restricted(self, status=None, code=None):
        recent = self.recent_restrictions() + [self.clock()]
        self.success_streak = 0
        # Resume at half speed or slower, whatever speed had been earned.
        self.factor = min(MAX_FACTOR, max(2.0, max(self.factor, float(self.store.control("pacing_factor") or 1)) * 2))
        fixed = float(self.store.control("pacing_restriction_cooldown_seconds") or 0)
        cooldown = fixed if fixed > 0 else self.config.restriction_cooldown * ESCALATION[min(len(recent), len(ESCALATION)) - 1]
        until = max(self.cooldown_until(), self.clock() + cooldown)
        self._persist({"pacing_factor": self.factor, 'pacing_success_streak': 0,
                       "pacing_restrictions": int(self.store.control("pacing_restrictions") or 0) + 1,
                       "pacing_restriction_log": json.dumps(recent),
                       "pacing_cooldown_until": until, "pacing_cooldown_reason": "access_restricted"})
        self.log("access_restricted", {"status": status, "code": code})
        return until

    def succeeded(self):
        """Recover throughput slowly after verified successful responses."""
        chosen = factor_floor(self.store)
        if self.factor <= max(MIN_FACTOR, chosen):
            return
        self.success_streak += 1
        # Halving every 10 responses jumped from a restriction back to full
        # speed within minutes. 2x -> 1x now takes ~400 clean operations; each
        # step below 1x (the pace that ran 10 h without a refusal) takes 500.
        step, ratio, floor = (100, .8, 1.0) if self.factor > 1 else (500, .9, MIN_FACTOR)
        if self.success_streak >= step:
            self.factor = max(floor, chosen, self.factor * ratio)
            self.success_streak = 0
            self.store.control('pacing_factor', self.factor)
            self.log('successful_requests_restore_speed')
        self.store.control('pacing_success_streak', self.success_streak)

    def clear_cooldown(self):
        """Explicit administrative action, not used by the acknowledgement button."""
        self.store.control("pacing_cooldown_until", 0)
        self.store.control("pacing_cooldown_reason", "")

    def log(self, decision, extra=None):
        from .store import encode
        self.store.save(None, None, "pacing_decision", encode({"decision": decision, **self.state(), **(extra or {})}),
                        {"decision": decision})
