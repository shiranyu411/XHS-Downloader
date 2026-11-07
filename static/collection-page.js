/* Browser adapter. Link discovery derived from XHS-Downloader 2ab2620b
 * (GPL-3.0); scroll actions use the collector's explicit pacing interval. */
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
                if (u.origin === location.origin && /^\/(explore|discovery\/item|search_result)\/[a-f0-9]{24}\/?$/i.test(u.pathname)) {
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
            const route = location.pathname === '/search_result' ? 'search_result' : 'explore';
            const url = new URL(existing || `/${route}/${id}`, location.origin);
            url.searchParams.set('xsec_token', item.xsecToken);
            url.searchParams.set('xsec_source', location.pathname === '/search_result' ? 'pc_search' :
                location.pathname.startsWith('/user/profile/') ? 'pc_user' : 'pc_feed');
            found.set(id, url.href);
        }
        return [...found.values()];
    }
    let discovery;
    function startDiscovery(maxScrollCount, intervalMs = 15000, controlled = false) {
        if (discovery && !discovery.done) throw Error('discovery already running');
        if (!Number.isFinite(intervalMs) || intervalMs < 0 || intervalMs > 86400000) throw Error('invalid scroll interval');
        const origin = location.href;
        discovery = {done: false, count: 0, reason: '', pending: [], seen: new Set(), origin};
        const d = discovery;
        const collect = () => {
            for (const url of links()) if (!d.seen.has(url)) { d.seen.add(url); d.pending.push(url); }
        };
        const finish = reason => { clearTimeout(d.timer); collect(); d.reason = reason; d.done = true; };
        d.stop = () => finish('paused');
        const atBottom = () => document.documentElement.scrollHeight - window.innerHeight - window.scrollY <= 10;
        const scroll = () => {
            if (d.done) return;
            collect();
            if (location.href !== origin) { finish('page_changed'); return; }
            const limitReached = maxScrollCount !== null && d.count >= maxScrollCount;
            if (limitReached || atBottom()) {
                finish(limitReached ? 'scroll_limit' : 'observed_bottom'); return;
            }
            window.scrollBy(0, 300); d.count++;
            // A single timer prevents delayed callbacks from creating a burst.
            if (!controlled) d.timer = setTimeout(scroll, intervalMs);
        };
        d.step = scroll;
        collect();
        if (maxScrollCount === 0 || atBottom()) finish(maxScrollCount === 0 ? 'scroll_limit' : 'observed_bottom');
        else if (!controlled) d.timer = setTimeout(scroll, intervalMs);
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
        const restriction = /账号状态异常|帐号状态异常|账号异常|访问频繁|访问限制|安全限制|安全验证/;
        const refused = [document.title, ...[...document.querySelectorAll('.error-wrapper, .error-container, .error-content, .risk-container')]
            .filter(visible).map(el => el.innerText)].find(text => restriction.test(text));
        const gate = [...document.querySelectorAll('.login-container, .login-modal, .captcha-container, #captcha, [class*="verify-modal"]')].find(visible);
        return {note: valid ? JSON.parse(JSON.stringify(note)) : null,
            href: location.href, commentsReady: !!document.querySelector('.comments-container, .comment-list, .comments-el'),
            blocked: !!refused || !!gate,
            // A captcha or login gate needs a person, even on a refusal page.
            blockKind: gate ? (gate.matches('.captcha-container, #captcha, [class*="verify-modal"]') ? 'verification' : 'login')
                : refused ? (/安全验证/.test(refused) ? 'verification' : 'access') : null,
            // XHS's 404 page ("你访问的页面不见了") for a deleted or hidden note soon leaves the
            // note's address, which would otherwise read as a page to retry.
            unavailable: /页面不见了/.test(document.title) || [...document.querySelectorAll('.error-wrapper, .note-error, .error-container')]
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
        stepDiscovery: () => discovery?.step(),
        cancelDiscovery: () => { if (discovery && !discovery.done) discovery.stop(); }};
})();
