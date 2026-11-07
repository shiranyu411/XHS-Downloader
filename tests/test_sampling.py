"""Primary-only sampling, topic screening and queue priority; local fixtures, no platform traffic."""
import json
from pathlib import Path
import tempfile
import unittest

from source.collection.config import Config
from source.collection.protocol import Ledger, Request
from source.collection.request import navigation_url
from source.collection.results import collection_status, read_post
from source.collection.runner import Runner
from source.collection.screening import PROFILE_NAME, classify_title, load_profile, priority, relevant_note, valid_comment
from source.collection.search import accept_page
from source.collection.store import Store, encode
from tests import test_collection as fixtures
from tests.test_collection import NOTE, URL, UNPACED, comment, envelope

EXAMPLE = Path(__file__).resolve().parents[1] / 'docs' / 'topic-profile.example.json'
PROFILE = load_profile(EXAMPLE)


def use_example_profile(root):
    (Path(root) / PROFILE_NAME).write_bytes(EXAMPLE.read_bytes())


class SamplingLedgerTests(unittest.TestCase):
    def test_primary_only_counts_substantive_comments_and_skips_reply_threads(self):
        ledger = Ledger(NOTE)
        root = comment('c0')
        root.update(sub_comments=[comment('r0', 'c0'), dict(comment('r1', 'c0'), content='蹲')],
                    sub_comment_has_more=True, sub_comment_cursor='r2')
        ledger.observe(Request('comments', NOTE), envelope(
            [root, dict(comment('c1'), content='哈哈哈'), dict(comment('c2'), content='', pictures=[{'url': 'x'}])], True, 'p2'))
        self.assertEqual(ledger.valid, {'c0', 'r0', 'c2'})
        self.assertEqual(ledger.record()['valid_comments'], 3)
        self.assertEqual(ledger.next_request(), Request('replies', NOTE, 'c0', 'r2'))
        self.assertEqual(ledger.next_request(primary_only=True), Request('comments', NOTE, cursor='p2'))
        self.assertFalse(ledger.sampled())
        self.assertTrue(ledger.sampled(cap=3))
        self.assertFalse(ledger.sampled(cap=4))
        ledger.observe(Request('comments', NOTE, cursor='p2'), envelope([comment('c3')]))
        self.assertTrue(ledger.sampled())  # every primary page seen; reply threads not expanded
        self.assertFalse(ledger.complete())

    def test_refusal_is_never_a_sample(self):
        ledger = Ledger(NOTE)
        ledger.observe(Request('comments', NOTE), envelope([comment(f'c{i}') for i in range(5)], True, 'p2'))
        ledger.observe(Request('comments', NOTE, cursor='p2'), {'success': True, 'code': 300013, 'msg': '访问频繁'}, 461)
        self.assertFalse(ledger.sampled(cap=1))


class ScreeningTests(unittest.TestCase):
    def test_without_a_profile_nothing_is_screened(self):
        self.assertEqual(classify_title('小米14 Ultra 开箱', ['小米']), ('保留', '未配置主题筛选'))
        self.assertEqual(priority('保留', ['小米']), 0)
        self.assertTrue(relevant_note({'title': '手机开箱'}, ['小米'])[0])

    def test_titles_drop_only_clear_collisions(self):
        cases = [('小米粥的做法', ['小米'], '保留'), ('小米南瓜粥', ['小米'], '保留'),
                 ('米油 破壁机开箱', ['小米'], '待定'), ('小米14 Ultra 开箱', ['小米'], '剔除'),
                 ('小米手机熬夜也不卡', ['小米'], '待定'), ('今天的早餐', ['小米粥'], '待定'), ('', ['小米'], '待定')]
        for title, keywords, verdict in cases:
            self.assertEqual(classify_title(title, keywords, PROFILE)[0], verdict, title)
        ranks = [priority(v, k, PROFILE) for v, k in [('保留', ['小米粥']), ('待定', ['小米粥']), ('保留', ['小米']),
                                                      ('待定', ['小米']), ('剔除', ['小米粥']), ('保留', [])]]
        self.assertEqual(ranks, [0, 1, 2, 3, -1, 0])

    def test_body_gate_only_screens_non_core_keywords(self):
        phone = {'title': '小米开箱', 'desc': '手机续航很顶 拍照也不错', 'tagList': [{'name': '数码'}]}
        tagged = {'title': '我的小米', 'desc': '', 'tagList': [{'name': '小米粥'}]}
        hint = {'title': '小米今天', 'desc': '养胃', 'tagList': []}
        bare = {'title': '分享', 'desc': '', 'tagList': []}
        self.assertTrue(relevant_note(phone, ['小米粥'], PROFILE)[0], 'core keywords are never body-gated')
        self.assertTrue(relevant_note(phone, ['小米', '小米粥'], PROFILE)[0])
        self.assertFalse(relevant_note(phone, ['小米'], PROFILE)[0])
        self.assertTrue(relevant_note(tagged, ['小米'], PROFILE)[0])
        self.assertTrue(relevant_note(hint, ['小米'], PROFILE)[0])
        self.assertFalse(relevant_note(bare, ['小米'], PROFILE)[0])

    def test_valid_comment_needs_substance_or_a_picture(self):
        self.assertTrue(valid_comment({'content': '我也天天喝这个粥'}))
        self.assertTrue(valid_comment({'content': '', 'pictures': [{'url': 'x'}]}))
        for junk in ['蹲', '蹲蹲蹲蹲', '同问同问', '哈哈哈哈哈', '[笑哭R][笑哭R]', '@某人 ', 'mark', '先赞后看', '我也想要']:
            self.assertFalse(valid_comment({'content': junk}), junk)


class QueueTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.store = Store(Path(self.temp.name), Config())

    def tearDown(self):
        self.store.close()
        self.temp.cleanup()

    def test_priority_orders_queue_and_never_opens_dropped_titles(self):
        ids = [f'{i:024x}' for i in range(1, 5)]
        self.store.enqueue([f'https://www.xiaohongshu.com/explore/{id_}' for id_ in ids], {})
        self.store.prioritize([(ids[0], 3, '待定', ''), (ids[1], -1, '剔除', ''), (ids[2], 0, '保留', '')])
        order = []
        while (task := self.store.next_task()) is not None:
            order.append(task['note_id'])
            self.store.finish(self.store.begin(task['note_id']), {'body': 'saved', 'comments': 'sampled', 'reason': 'fixture'}, 'finished')
        self.assertEqual(order, [ids[2], ids[3], ids[0]])  # unranked posts rank as core 待定

    def collect_order(self, posts, outcome=lambda n: 'sampled', earlier=()):
        """posts: (keyword, id, priority[, title reason]); earlier: (id, comments, status) of posts
        opened before. Returns the ids in the order the queue hands them out."""
        for keyword, id_, *_ in posts:
            self.store.enqueue([f'https://www.xiaohongshu.com/explore/{id_}'], {'mode': 'keyword_search', 'request': {'keyword': keyword}})
        self.store.prioritize([(id_, rank, '', ''.join(reason)) for _, id_, rank, *reason in posts])
        for id_, comments, status in earlier:
            self.store.finish(self.store.begin(id_), {'body': 'saved', 'comments': comments, 'reason': 'fixture'}, status)
        order = []
        while (task := self.store.next_task()) is not None:
            order.append(task['note_id'])
            self.store.finish(self.store.begin(task['note_id']), {'body': 'saved', 'comments': outcome(len(order)), 'reason': 'fixture'}, 'finished')
        return order

    def collect_keywords(self, posts, **options):
        keyword_of = {id_: keyword for keyword, id_, *_ in posts}
        return [keyword_of[id_] for id_ in self.collect_order(posts, **options)]

    def test_keyword_sample_takes_keywords_in_turn_before_priority_order(self):
        self.store.config = Config(sample_per_keyword=2)
        posts = [('A', f'{i:024x}', 0) for i in (1, 2, 3)] + [('B', f'{i:024x}', 2) for i in (4, 5, 6)]
        self.assertEqual(self.collect_keywords(posts), ['A', 'B', 'A', 'B', 'A', 'B'])  # priority alone: A A A B B B

    def test_off_topic_opens_take_a_turn_and_unfinished_searches_wait(self):
        self.store.config = Config(sample_per_keyword=1)
        self.store.control('search_progress', json.dumps([{'keyword': 'C', 'status': 'running'}]))
        posts = [('A', f'{i:024x}', 0) for i in (1, 2)] + [('B', f'{3:024x}', 0)] + [('C', f'{i:024x}', 0) for i in (4, 5)]
        order = self.collect_keywords(posts, outcome=lambda n: 'skipped' if n == 1 else 'sampled')
        self.assertEqual(order, ['A', 'B', 'A', 'C', 'C'])  # A's off-topic open still counts as its turn

    def test_posts_short_of_comments_leave_their_keyword_a_turn_and_resume_first(self):
        self.store.config = Config(sample_per_keyword=10)
        a1, a2, a3, b1, b2, b3 = (f'{i:024x}' for i in range(1, 7))
        posts = [('A', a1, 0), ('A', a2, 0), ('A', a3, 0), ('B', b1, 0), ('B', b2, 0), ('B', b3, 0)]
        order = self.collect_order(posts, earlier=[(a1, 'partial', 'paused'), (a2, 'blocked', 'paused'), (b1, 'sampled', 'finished')])
        # A has nothing settled and B one post, so A goes twice first, resuming its started posts.
        self.assertEqual((set(order[:2]), order[3], set(order[2::2])), ({a1, a2}, a3, {b2, b3}))

    def test_unclear_titles_wait_and_tool_titles_go_last(self):
        self.store.config = Config(sample_per_keyword=5)
        slang, tool, ai = (f'{i:024x}' for i in (1, 2, 3))
        posts = [('小米', slang, 3, '标题看不出，打开正文后判断'), ('小米', tool, 2, '主题+工具类:米油+破壁机，打开正文后判断'),
                 ('小米', ai, 2, '主题相关:米油')]
        self.assertEqual(self.collect_order(posts), [ai, tool, slang])  # priority alone: tool ai slang

    def test_search_pages_rank_titles_and_keep_each_posts_best_rank(self):
        a, b, c = 'a' * 24, 'b' * 24, 'c' * 24

        def page(keyword, items):
            state = {'keyword': keyword, 'next_page': 1, 'pages': []}
            payload = {'success': True, 'code': 0, 'data': {'has_more': False, 'items': [
                {'id': id_, 'model_type': 'note', 'xsec_token': 't', 'note_card': {'display_title': title}}
                for id_, title in items]}}
            event = self.store.save(None, None, 'network_json', encode(payload),
                                    {'status': 200, 'post_data': json.dumps({'keyword': keyword, 'page': 1})})
            accept_page(self.store, state, self.store.db.execute('SELECT * FROM events WHERE id=?', (event,)).fetchone())
            return {r[0]: r[1] for r in self.store.db.execute('SELECT note_id, priority FROM task_priority')}

        use_example_profile(self.store.root)
        self.assertEqual(page('小米', [(a, '小米粥的做法'), (b, '小米14开箱'), (c, '今天分享一下')]), {a: 2, b: -1, c: 3})
        self.assertEqual(page('小米粥', [(a, '小米粥的做法')]), {a: 0, b: -1, c: 3})
        self.assertEqual(page('小米', [(a, '小米粥的做法')]), {a: 0, b: -1, c: 3})

    def test_navigation_url_keeps_token_and_query_exactly(self):
        query = '?xsec_token=AB%2B%3D&xsec_source=pc_search'
        self.assertEqual(navigation_url('https://www.xiaohongshu.com/search_result/' + NOTE + query),
                         'https://www.xiaohongshu.com/explore/' + NOTE + query)


class SamplingBrowserTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = fixtures.BrowserTests.asyncSetUp
    asyncTearDown = fixtures.BrowserTests.asyncTearDown
    route = fixtures.BrowserTests.route

    def batch(self, keyword='小米粥', **options):
        store = Store(Path(self.temp.name) / 'batch', Config(**{
            'open_interval': 0, 'page_interval': 0, 'media': False, 'strategy': 'request',
            'comment_scope': 'primary', 'topic_gate': True, **UNPACED, **options}))
        use_example_profile(store.root)
        store.enqueue([URL], {'mode': 'keyword_search', 'request': {'keyword': keyword}})
        return store

    def cursors(self):
        return [q['cursor'] for _, q in self.calls]

    async def test_primary_scope_reads_every_primary_page_without_expanding_replies(self):
        store = self.batch()
        try:
            result = await Runner(store).collect(self.context, store.next_task())
            self.assertEqual((result['comments'], result['reason']), ('sampled', 'primary_end'))
            self.assertEqual(self.cursors(), ['', 'primary2'])
            self.assertEqual((store.tasks()[0]['status'], store.tasks()[0]['comments']), ('finished', 'sampled'))
            self.assertIsNone(store.next_task())
            self.assertEqual(read_post(store.root, NOTE)['outcome'], 'sampled', 'labelled as a sample, not as complete or partial')
            counts = collection_status(store.root)['counts']
            self.assertEqual((counts['complete'], counts['sampled'], counts['partial']), (0, 1, 0))
        finally:
            store.close()

    async def test_cap_stops_after_enough_substantive_comments(self):
        store = self.batch(valid_comment_cap=20)
        try:
            result = await Runner(store).collect(self.context, store.next_task())
            self.assertEqual((result['comments'], result['reason']), ('sampled', 'valid_comment_cap'))
            self.assertEqual(self.cursors(), [''])
            progress = json.loads(store.db.execute('SELECT data FROM progress').fetchone()[0])
            self.assertEqual(progress['valid_comments'], 22)  # 20 primaries + 2 inline replies
        finally:
            store.close()

    async def test_posts_already_meeting_the_rule_need_no_page_or_request(self):
        store = self.batch(comment_scope='all', max_operations=1)
        try:
            result = await Runner(store).collect(self.context, store.next_task())
            self.assertEqual(result['comments'], 'partial')
            calls = len(self.calls)
            store.config = Config(open_interval=0, page_interval=0, media=False, strategy='request',
                                  comment_scope='primary', valid_comment_cap=20, **UNPACED)
            store.retry_incomplete()
            result = await Runner(store).collect(self.context, store.next_task())
            self.assertEqual((result['body'], result['comments']), ('saved', 'sampled'))
            self.assertEqual(len(self.calls), calls)
            attempt = store.db.execute('SELECT id FROM attempts ORDER BY rowid DESC LIMIT 1').fetchone()[0]
            opened = store.db.execute("SELECT count(*) FROM events WHERE attempt=? AND kind='page_state_snapshot'",
                                      (attempt,)).fetchone()[0]
            self.assertEqual(opened, 0, 'decided from saved responses, without opening the post')
        finally:
            store.close()

    async def test_topic_gate_skips_comments_of_off_topic_posts(self):
        store = self.batch(keyword='小米')
        try:
            result = await Runner(store).collect(self.context, store.next_task())
            self.assertEqual((result['body'], result['comments'], result['media'], result['reason']),
                             ('saved', 'skipped', 'skipped', 'off_topic'))
            self.assertEqual(self.calls, [])
            self.assertIsNone(store.next_task())
            self.assertEqual(store.db.execute("SELECT count(*) FROM events WHERE kind='topic_gate'").fetchone()[0], 1)
            self.assertEqual(read_post(store.root, NOTE)['outcome'], 'skipped')
        finally:
            store.close()


if __name__ == '__main__':
    unittest.main()
