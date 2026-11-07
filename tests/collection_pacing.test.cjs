// Offline checks of the browser adapter: discovery timers and page state. No website or browser connection.
const {test} = require('node:test');
const assert = require('node:assert/strict');
const {readFileSync} = require('node:fs');
const vm = require('node:vm');
const source = readFileSync(`${__dirname}/../static/collection-page.js`, 'utf8');

function harness(title = '') {
    const timers = new Map();
    let sequence = 0, scrolls = 0;
    const location = {href: 'https://www.xiaohongshu.com/search_result', pathname: '/search_result'};
    const window = {innerHeight: 500, scrollY: 0, scrollBy: () => {scrolls++;}};
    const document = {title, querySelector: () => null, querySelectorAll: () => [], documentElement: {scrollHeight: 5000}};
    vm.runInNewContext(source, {
        window, document, location,
        setTimeout: (fn, ms) => {const id = ++sequence; timers.set(id, {fn, ms}); return id;},
        clearTimeout: id => timers.delete(id),
    });
    return {
        api: window.__xhsCollect, timers, location,
        get scrolls() {return scrolls;},
        fire() {const [id, {fn}] = timers.entries().next().value; timers.delete(id); fn();},
    };
}

test('discovery waits 15s before each scroll, with only one pending timer', () => {
    const h = harness();
    h.api.startDiscovery(2);
    assert.equal(h.scrolls, 0);
    for (let i = 1; i <= 2; i++) {
        assert.equal(h.timers.size, 1);
        assert.equal([...h.timers.values()][0].ms, 15000);
        h.fire();
        assert.equal(h.scrolls, i);
    }
    h.fire();
    assert.equal(h.api.discoveryProgress().done, true);
    assert.equal(h.timers.size, 0);
});

test('pause cancels a long wait immediately and cannot scroll later', () => {
    const h = harness();
    h.api.startDiscovery(50, 60000);
    const delayed = [...h.timers.values()][0].fn;
    h.api.cancelDiscovery();
    assert.equal(h.timers.size, 0);
    assert.equal(h.api.discoveryProgress().reason, 'paused');
    delayed();
    assert.equal(h.scrolls, 0);
    assert.equal(h.timers.size, 0);
});

test('Python-controlled discovery never schedules autonomous scroll timers', () => {
    const h = harness();
    h.api.startDiscovery(5, 15000, true);
    assert.equal(h.timers.size, 0);
    assert.equal(h.scrolls, 0);
    h.api.stepDiscovery();
    assert.equal(h.scrolls, 1);
    assert.equal(h.timers.size, 0);
    h.api.cancelDiscovery();
    h.api.stepDiscovery();
    assert.equal(h.scrolls, 1);
});

test("XHS's 404 page marks the note unavailable, not refused", () => {
    const id = '65a0000000000000000a0001';
    const gone = harness('小红书 - 你访问的页面不见了').api.state(id);
    assert.equal(gone.unavailable, true);
    assert.equal(gone.blocked, false);
    assert.equal(harness('小红书 - 你的生活兴趣社区').api.state(id).unavailable, false);
    assert.equal(harness('安全限制').api.state(id).blocked, true);
});

test('snapshot-only discovery finishes immediately; changed page stops pending scroll', () => {
    const h = harness();
    h.api.startDiscovery(0);
    assert.equal(h.api.discoveryProgress().done, true);
    assert.equal(h.timers.size, 0);
    h.api.startDiscovery(2);
    h.location.href += '?changed=1';
    h.fire();
    assert.equal(h.scrolls, 0);
    assert.equal(h.api.discoveryProgress().reason, 'page_changed');
});
