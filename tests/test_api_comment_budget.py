"""Offline shared comment budget and network-slot tests; no browser/platform calls."""
import asyncio
import time
from types import SimpleNamespace
import unittest
from urllib.parse import urlsplit

from source.collection.http_runner import ApiTransport, COMMENT, DETAIL, REPLIES, HOST, Stopped


class Client:
    def __init__(self, *, restricted=False, delay=0):
        self.calls, self.restricted, self.delay = [], restricted, delay
        self.active = self.peak = 0

    async def request(self, method, url, **kwargs):
        self.calls.append((urlsplit(url).path, time.monotonic()))
        self.active += 1
        self.peak = max(self.peak, self.active)
        try:
            await asyncio.sleep(self.delay)
            return SimpleNamespace(content=b'{"success":true,"code":0,"data":{}}',
                                   status_code=461 if self.restricted else 200, headers={}, url=url)
        finally:
            self.active -= 1


def transport(client, interval=.01, comment_interval=.08, concurrency=1):
    result = ApiTransport.__new__(ApiTransport)
    result.client = client
    result.interval, result.comment_interval = interval, comment_interval
    result.last = result.last_comment = 0.0
    result.gate, result.slots = asyncio.Lock(), asyncio.Semaphore(concurrency)
    result.sent = result.successful = 0
    result.blocked = False
    result.checkpoint = lambda: None
    result.prepare_request = lambda method, path, data: (HOST + path, None, {})
    return result


class CommentBudgetTests(unittest.IsolatedAsyncioTestCase):
    async def test_primary_and_replies_share_budget_without_blocking_ready_detail(self):
        client = Client()
        api = transport(client)
        await api.request('GET', COMMENT, {})
        reply = asyncio.create_task(api.request('GET', REPLIES, {}))
        await asyncio.sleep(0)
        await api.request('POST', DETAIL, {})
        await reply
        self.assertEqual([p for p, _ in client.calls], [COMMENT, DETAIL, REPLIES])
        self.assertGreaterEqual(client.calls[-1][1] - client.calls[0][1], .075)
        self.assertEqual(client.peak, 1)

    async def test_refusal_stops_a_request_waiting_for_a_network_slot(self):
        client = Client(restricted=True, delay=.02)
        api = transport(client, interval=0, comment_interval=0)
        results = await asyncio.gather(api.request('POST', DETAIL, {}), api.request('POST', DETAIL, {}), return_exceptions=True)
        self.assertEqual(len(client.calls), 1)
        self.assertEqual(api.sent, 1)
        self.assertIsInstance(results[1], Stopped)

    async def test_prepare_failure_releases_slot(self):
        client = Client()
        api = transport(client, interval=0)
        def broken(*args):
            raise ValueError('fixture signing failure')
        api.prepare_request = broken
        with self.assertRaises(ValueError):
            await api.request('POST', DETAIL, {})
        api.prepare_request = lambda method, path, data: (HOST + path, None, {})
        await asyncio.wait_for(api.request('POST', DETAIL, {}), .3)
        self.assertEqual(len(client.calls), 1)

    async def test_network_concurrency_is_still_bounded(self):
        client = Client(delay=.01)
        api = transport(client, interval=0, concurrency=2)
        await asyncio.gather(*(api.request('POST', DETAIL, {}) for _ in range(6)))
        self.assertEqual(client.peak, 2)
        self.assertEqual(api.successful, 6)


if __name__ == '__main__':
    unittest.main()
