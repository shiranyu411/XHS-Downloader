/* Compatibility for an already-running collector using the old count=20 fallback.
 * No request is issued here. Existing operations, status callbacks, browser client,
 * ownership checks and the collector's pacing remain unchanged.
 */
(() => {
    'use strict';
    const adapter = window.__xhsReadOnly;
    if (!adapter || typeof adapter.start !== 'function') return {installed: false, reason: 'adapter_not_ready'};
    if (adapter.replySizeCompat?.version === 'native-num10-v1') return {installed: true, already_installed: true};
    const original = adapter.start;
    function wrapped(id, rawUrl) {
        const url = new URL(rawUrl);
        if (url.origin === 'https://edith.xiaohongshu.com' && !url.username && !url.password && !url.hash &&
            url.pathname === '/api/sns/web/v2/comment/sub/page' &&
            !url.searchParams.has('num') && url.searchParams.get('count') === '20') {
            // Keep legacy expected parameters so the original CDP operation still
            // correlates exactly. `num` is the page-size parameter used by the site.
            url.searchParams.set('num', '10');
        }
        return original.call(this, id, url.href);
    }
    adapter.start = wrapped;
    adapter.replySizeCompat = {version: 'native-num10-v1', rollback() {
        if (adapter.start !== wrapped) return false;
        adapter.start = original;
        delete adapter.replySizeCompat;
        return true;
    }};
    return {installed: true, already_installed: false, version: 'native-num10-v1'};
})();
