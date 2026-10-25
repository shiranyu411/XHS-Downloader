/* Browser adapter 0.1.0. Discovery state/scroll algorithm derived from
 * XHS-Downloader 2ab2620b (GPL-3.0); navigation pacing is deliberately absent. */
(() => {
    const unwrap = v => v?._rawValue ?? v?.value ?? v;
    const visible = el => !!el && !!(el.offsetWidth || el.offsetHeight || el.getClientRects().length);
    function links() {
        const s = window.__INITIAL_STATE__;
        let feeds = [];
        if (location.pathname === '/search_result') feeds = unwrap(s?.search?.feeds) || [];
        else if (location.pathname.startsWith('/user/profile/')) {
            // Only the currently selected author's published notes. No recursive expansion.
            feeds = (unwrap(s?.user?.notes) || [])[0] || [];
        } else feeds = unwrap(s?.feed?.feeds) || [];
        const found = new Map();
        for (const a of document.querySelectorAll('a[href]')) {
            try {
                const u = new URL(a.href);
                if (u.origin === location.origin && /^\/(explore|discovery\/item)\/[a-f0-9]{24}\/?$/i.test(u.pathname)) {
                    const id = u.pathname.split('/').filter(Boolean).pop().toLowerCase();
                    const previous = found.get(id);
                    // Duplicate cover/title anchors must not downgrade a usable URL.
                    if (!previous || u.searchParams.get('xsec_token') || !new URL(previous).searchParams.get('xsec_token')) {
                        found.set(id, u.href);
                    }
                }
            } catch (_) { /* non-URL anchor */ }
        }
        for (const item of feeds) {
            if (!item?.noteCard || !/^[a-f0-9]{24}$/i.test(item.id) || !item.xsecToken) continue;
            const id = item.id.toLowerCase();
            const existing = found.get(id);
            if (existing && new URL(existing).searchParams.get('xsec_token')) continue;
            const url = new URL(existing || `/explore/${id}`, location.origin);
            url.searchParams.set('xsec_token', item.xsecToken);
            url.searchParams.set('xsec_source', location.pathname === '/search_result' ? 'pc_search' :
                location.pathname.startsWith('/user/profile/') ? 'pc_user' : 'pc_feed');
            found.set(id, url.href);
        }
        return [...found.values()];
    }
    let discovery;
    function startDiscovery(maxScrollCount) {
        if (discovery && !discovery.done) throw Error('discovery already running');
        const origin = location.href;
        discovery = {done: false, count: 0, reason: '', pending: [], seen: new Set(), origin};
        const d = discovery;
        const collect = () => {
            for (const url of links()) if (!d.seen.has(url)) { d.seen.add(url); d.pending.push(url); }
        };
        const finish = reason => { collect(); d.reason = reason; d.done = true; };
        const random = (min, max) => Math.floor(Math.random() * (max - min + 1)) + min;
        const atBottom = () => document.documentElement.scrollHeight - window.innerHeight - window.scrollY <= 10;
        // Upstream setInterval, 20% pause, 250–500 ms interval and 100–300 px step.
        const scroll = () => {
            const timer = setInterval(() => {
                collect();
                if (d.cancel || location.href !== origin) {
                    clearInterval(timer); finish(d.cancel ? 'paused' : 'page_changed');
                } else if (Math.random() < 0.2) {
                    clearInterval(timer); setTimeout(scroll, random(250, 500));
                } else if (d.count < maxScrollCount && !atBottom()) {
                    window.scrollBy(0, random(100, 300)); d.count++;
                } else {
                    clearInterval(timer); finish(d.count >= maxScrollCount ? 'scroll_limit' : 'observed_bottom');
                }
            }, random(250, 500));
        };
        collect(); scroll();
    }
    function discoveryProgress() {
        if (!discovery) return null;
        const {done, count, reason, origin} = discovery;
        return {done, count, reason, origin, links: discovery.pending.splice(0),
            filters: [...document.querySelectorAll('.active, [aria-selected="true"]')]
                .filter(visible).map(x => x.innerText).filter(Boolean).slice(0, 30)};
    }
    function state(noteId) {
        const s = window.__INITIAL_STATE__;
        const map = unwrap(s?.note?.noteDetailMap) || {};
        const entry = unwrap(map[noteId]);
        const note = unwrap(entry?.note);
        // No 'last object' fallback: it can silently archive a different note.
        const valid = note && (note.noteId || note.note_id) === noteId;
        const restriction = /账号状态异常|帐号状态异常|账号异常|访问频繁|访问限制|安全验证/;
        const refusal = restriction.test(document.title) || [...document.querySelectorAll('.error-wrapper, .error-container, .error-content, .risk-container')]
            .some(el => visible(el) && restriction.test(el.innerText));
        return {note: valid ? JSON.parse(JSON.stringify(note)) : null,
            href: location.href, commentsReady: !!document.querySelector('.comments-container, .comment-list, .comments-el'),
            blocked: refusal || [...document.querySelectorAll('.login-container, .login-modal, .captcha-container, #captcha, [class*="verify-modal"]')].some(visible),
            unavailable: [...document.querySelectorAll('.error-wrapper, .note-error, .error-container')]
                .some(el => visible(el) && /不存在|已删除|无法查看|不可见/.test(el.innerText))};
    }
    function replyButtons() {
        const result = [];
        for (const el of document.querySelectorAll('.show-more, .show-more-reply, .more-replies, .reply-container button, [data-reply-more]')) {
            if (!visible(el) || !/展开|更多|more|repl/i.test(el.innerText)) continue;
            const root = el.closest('.parent-comment') || el.closest('[id^="comment-"]');
            const idEl = root?.matches('[id^="comment-"]') ? root : root?.querySelector('[id^="comment-"], [data-comment-id]');
            const id = root?.getAttribute('data-comment-id') || idEl?.getAttribute('data-comment-id') || idEl?.id?.replace(/^comment-/, '');
            if (id) result.push({id, el});
        }
        return result;
    }
    function action(incomplete, clickedParent) {
        const buttons = replyButtons();
        const target = clickedParent ? buttons.find(b => b.id === clickedParent) : buttons.find(b => incomplete.includes(b.id));
        if (target) {
            target.el.scrollIntoView({block: 'center'});
            target.el.click();
            return {kind: 'reply', parent: target.id};
        }
        // Scroll the actual overflowing ancestor, not always window/body.
        let el = document.querySelector('.comments-container, .comment-list, .comments-el');
        while (el && el !== document.body) {
            if (el.scrollHeight > el.clientHeight + 5 && /auto|scroll/.test(getComputedStyle(el).overflowY)) break;
            el = el.parentElement;
        }
        if (!el || el === document.body) el = document.scrollingElement;
        const before = el.scrollTop;
        el.scrollBy(0, Math.max(200, el.clientHeight * 0.8));
        return {kind: 'scroll', moved: el.scrollTop !== before};
    }
    window.__xhsCollect = {links, startDiscovery, discoveryProgress, state, action,
        cancelDiscovery: () => { if (discovery) discovery.cancel = true; }};
})();
