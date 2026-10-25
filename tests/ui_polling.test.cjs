// Run: node --test tests/ui_polling.test.cjs (no browser/network/dependencies).
const {test} = require('node:test');
const assert = require('node:assert/strict');
const {readFileSync} = require('node:fs');
const vm = require('node:vm');

const source = readFileSync(`${__dirname}/../static/GUI/ui-polling.js`, 'utf8');
function harness(refresh, isBusy = () => false) {
    const timers = new Map(), events = new Map();
    let sequence = 0;
    const window = {addEventListener: (event, fn) => events.set(event, fn)};
    const document = {hidden: false, addEventListener: window.addEventListener};
    vm.runInNewContext(source, {
        window, document,
        setTimeout: (fn, ms) => {const id = ++sequence; timers.set(id, {fn, ms}); return id;},
        clearTimeout: id => timers.delete(id),
    });
    const poller = window.createUiPoller({refresh, isBusy});
    return {
        poller, timers, window, document,
        hide() {window.__xhsUiVisible = false; events.get('xhs-ui-visibility')();},
        show() {window.__xhsUiVisible = true; events.get('xhs-ui-visibility')();},
        unload() {events.get('pagehide')();},
        fire() {const [id, {fn}] = timers.entries().next().value; timers.delete(id); return fn();},
    };
}
const settle = () => new Promise(resolve => setImmediate(resolve));

test('idle 5s, active 1s; timer reschedules only after completion', async () => {
    let busy = false, calls = 0;
    const h = harness(async () => {calls++;}, () => busy);
    h.poller.start();
    assert.equal([...h.timers.values()][0].ms, 5000);
    await h.fire();
    assert.equal(calls, 1);
    busy = true;
    await h.fire();
    assert.equal([...h.timers.values()][0].ms, 1000);
    assert.equal(h.timers.size, 1);
});

test('hidden pages have no timers; forced consent sync still works', async () => {
    let calls = 0;
    const h = harness(async () => {calls++;});
    h.poller.start();
    h.hide();
    assert.equal(h.timers.size, 0);
    await h.poller.request(false);
    assert.equal(calls, 0);
    await h.poller.request();
    assert.equal(calls, 1);
    assert.equal(h.timers.size, 0);
    h.show();
    await settle();
    assert.equal(calls, 2);
    assert.equal(h.timers.size, 1);
});

test('in-flight refreshes coalesce but do not lose the final requested state', async () => {
    let calls = 0, active = 0, peak = 0, release;
    const h = harness(async () => {
        calls++; active++; peak = Math.max(peak, active);
        if (calls === 1) await new Promise(resolve => {release = resolve;});
        active--;
    });
    h.poller.start();
    const first = h.poller.request();
    await settle();
    const second = h.poller.request();
    const third = h.poller.request();
    assert.equal(h.timers.size, 0);
    release();
    await Promise.all([first, second, third]);
    assert.equal(calls, 2);
    assert.equal(peak, 1);
    assert.equal(h.timers.size, 1);
});

test('hiding during a request prevents automatic follow-up timers', async () => {
    let release;
    const h = harness(() => new Promise(resolve => {release = resolve;}));
    h.poller.start();
    const flight = h.poller.request();
    await settle();
    h.hide(); release();
    await flight;
    assert.equal(h.timers.size, 0);
});

test('plain PyWebView uses document visibility and unload cancels polling', async () => {
    let calls = 0;
    const h = harness(async () => {calls++;});
    h.document.hidden = true;
    h.poller.start();
    assert.equal(h.timers.size, 0);
    h.document.hidden = false;
    await h.poller.request();
    assert.equal(calls, 1);
    assert.equal(h.timers.size, 1);
    h.unload();
    assert.equal(h.timers.size, 0);
});
