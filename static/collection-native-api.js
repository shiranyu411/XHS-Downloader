/* Read-only, resident-page API bridge. No navigation, cookie export, or retries.
 * Client discovery adapted from the existing collection-request.js, based on
 * Yht20927/xiaohongshu-cli (MIT); see THIRD_PARTY_NOTICES.md.
 */
(() => {
    'use strict';
    if (window.__xhsNativeApi?.version === '1' &&
        typeof window.__xhsNativeApi.start === 'function' && typeof window.__xhsNativeApi.probe === 'function') return;
    let client;
    function probe() {
        if (!['www.xiaohongshu.com', 'www.rednote.com'].includes(location.hostname))
            return {available: false, reason: 'wrong_origin'};
        if (client) return {available: true, version: '1'};
        let require_;
        const chunks = window.webpackChunkxhs_pc_web;
        if (!chunks?.push) return {available: false, reason: 'webpack_unavailable'};
        chunks.push([[`native_api_${Date.now()}`], {}, r => { require_ = r; }]);
        if (!require_?.c) return {available: false, reason: 'webpack_cache_unavailable'};
        const candidates = [];
        for (const mod of Object.values(require_.c)) {
            try {
                const e = mod?.exports;
                for (const c of [e, e?.default, e?.Z, e?.A, e?.N]) {
                    if (!c || typeof c.get !== 'function' || typeof c.post !== 'function' ||
                        !c.interceptors?.request || !c.interceptors?.response) continue;
                    if (!candidates.some(other => other.get === c.get && other.interceptors === c.interceptors))
                        candidates.push(c);
                }
            } catch (_) { /* Unrelated exports can have getters. */ }
        }
        const wrappers = candidates.filter(c => c.makeUri && c.instance && c.interceptors.dispatch && c.interceptors.result);
        const preferred = candidates.filter(c => {
            try { return new URL(c.defaults?.baseURL).origin === 'https://edith.xiaohongshu.com'; }
            catch (_) { return false; }
        });
        const configured = candidates.filter(c => c.interceptors.request.handlers?.some(Boolean));
        const choices = wrappers.length ? wrappers : preferred.length ? preferred : configured;
        if (choices.length !== 1) return {available: false, reason: 'request_client_not_unique', candidates: choices.length};
        client = choices[0];
        return {available: true, version: '1'};
    }
    function start({method, url: rawUrl, payload}) {
        const capability = probe();
        if (!capability.available) return {started: false, reason: capability.reason};
        let url;
        try { url = new URL(rawUrl); } catch (_) { return {started: false, reason: 'invalid_url'}; }
        const detail = url.pathname === '/api/sns/web/v1/feed';
        const comments = ['/api/sns/web/v2/comment/page', '/api/sns/web/v2/comment/sub/page'].includes(url.pathname);
        const note = detail ? payload?.source_note_id : url.searchParams.get('note_id');
        const token = detail ? payload?.xsec_token : url.searchParams.get('xsec_token');
        if (url.origin !== 'https://edith.xiaohongshu.com' || url.username || url.password || url.hash ||
            !(detail && method === 'POST' || comments && method === 'GET') ||
            !/^[a-f0-9]{24}$/i.test(note || '') || !token ||
            (url.pathname.endsWith('/sub/page') && !/^[a-f0-9]{24}$/i.test(url.searchParams.get('root_comment_id') || '')))
            return {started: false, reason: 'invalid_readonly_api_request'};
        try {
            // The page's configured client supplies its own current signature and
            // browser headers. Python captures the real network response, not this Promise.
            Promise.resolve(detail ? client.post(url.href, payload) : client.get(url.href)).catch(() => {});
            return {started: true};
        } catch (_) { return {started: false, reason: 'native_client_dispatch_failed'}; }
    }
    window.__xhsNativeApi = {version: '1', probe, start};
})();
