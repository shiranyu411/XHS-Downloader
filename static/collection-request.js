/* Read-only adapter 0.2.0.
 * Webpack/request-client discovery adapted from Yht20927/xiaohongshu-cli
 * 5c0159768147b4151d4912c083863a23bbbebb7a (MIT, copyright 2026 Yht20927).
 * See third_party/xiaohongshu-cli-LICENSE and THIRD_PARTY_NOTICES.md.
 * No response interceptor changes, response wrapping, polling server or write APIs.
 */
(() => {
    'use strict';
    let require_, client, moduleId, operation;
    function probe() {
        if (client) return {available: true, module_id: moduleId, version: '0.2.0'};
        const chunks = window.webpackChunkxhs_pc_web;
        if (!require_ && chunks && typeof chunks.push === 'function') {
            try { chunks.push([[`xhs_collect_${Date.now()}`], {}, r => { require_ = r; }]); }
            catch (_) { /* Report incompatibility, do not guess module IDs. */ }
        }
        if (!require_?.c) return {available: false, reason: 'webpack_cache_unavailable', version: '0.2.0'};
        const candidates = new Map();
        for (const [id, mod] of Object.entries(require_.c)) {
            try {
                const exp = mod?.exports;
                for (const c of [exp, exp?.default, exp?.Z, exp?.A, exp?.N]) {
                    if (!c || !['object', 'function'].includes(typeof c)) continue;
                    if (typeof c.get === 'function' && c.interceptors?.request && c.interceptors?.response) {
                        // Namespace/default exports may alias the same client.
                        if (![...candidates.keys()].some(other => other.get === c.get && other.interceptors === c.interceptors)) {
                            candidates.set(c, id);
                        }
                    }
                }
            } catch (_) { /* Some unrelated exports have getters. */ }
        }
        const preferred = [...candidates].filter(([c]) => {
            try { return new URL(c.defaults?.baseURL).origin === 'https://edith.xiaohongshu.com'; }
            catch (_) { return false; }
        });
        // Current page exports its configured HTTP wrapper alongside bare axios.
        // Prefer that wrapper; never choose an arbitrary unconfigured axios instance.
        const wrappers = [...candidates].filter(([c]) => typeof c.makeUri === 'function' &&
            c.instance && c.interceptors.dispatch && c.interceptors.result);
        const configured = [...candidates].filter(([c]) => c.interceptors.request.handlers?.some(h => h));
        const choices = wrappers.length ? wrappers : preferred.length ? preferred : configured;
        if (choices.length !== 1) return {available: false,
            reason: choices.length ? 'ambiguous_request_client' : 'request_client_unavailable',
            candidates: choices.length, version: '0.2.0'};
        [client, moduleId] = choices[0];
        return {available: true, module_id: moduleId, version: '0.2.0'};
    }
    function start(id, rawUrl) {
        if (operation?.state === 'pending') return {started: false, reason: 'operation_pending'};
        const capability = probe();
        if (!capability.available) return {started: false, reason: capability.reason};
        const url = new URL(rawUrl);
        const note = location.pathname.match(/^\/(?:explore|discovery\/item)\/([a-f0-9]{24})\/?$/i)?.[1];
        if (!['www.xiaohongshu.com', 'www.rednote.com'].includes(location.hostname) ||
            url.origin !== 'https://edith.xiaohongshu.com' || url.username || url.password || url.hash ||
            !/^\/api\/sns\/web\/v[12]\/comment\/(?:sub\/)?page$/.test(url.pathname) ||
            !note || url.searchParams.get('note_id') !== note ||
            (url.pathname.includes('/sub/') && !url.searchParams.get('root_comment_id'))) {
            return {started: false, reason: 'invalid_readonly_request'};
        }
        const current = operation = {id, state: 'pending'};
        // Return immediately; Python waits for the CDP response's durable commit.
        // Even a resolved Promise says nothing about server success or completeness.
        try {
            Promise.resolve(client.get(url.href)).then(
                () => { current.state = 'settled'; },
                () => { current.state = 'rejected'; }
            );
        } catch (_) { current.state = 'rejected'; }
        return {started: true};
    }
    window.__xhsReadOnly = {probe, start,
        status: id => operation?.id === id ? {id, state: operation.state} : null};
})();
