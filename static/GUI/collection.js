(() => {
    'use strict';
    const el = id => document.getElementById(id);
    let api, toast, state, pending = false, initialized = false, batchesLoaded = false, tableKey = '', logKey = '';
    const phases = {idle: '未开始', ready: '已打开', running: '采集中', pausing: '正在收尾', paused: '已暂停', finished: '处理结束', error: '已停止'};
    const labels = {saved: '已保存', complete_observed: '完整', partial: '不完整', blocked: '受限', not_requested: '未请求',
        disabled: '未开启', success: '成功', failed: '失败', unavailable: '不可访问', pending: '待处理', running: '采集中',
        interrupted: '待续采', paused: '已暂停', finished: '已处理'};
    const reasons = {observed_end: '可见分页已遍历结束', body_saved: '正文已保存', operation_limit: '达到单篇操作预算',
        note_or_page_timeout: '达到时间预算', operation_response_timeout: '等待分页响应超时', pagination_evidence_invalid: '分页证据不足或矛盾',
        adapter_unavailable: '网页模块不兼容', adapter_start_rejected: '网页模块拒绝发起请求', initial_capture_pending: '首批响应未完成',
        capture_pending: '响应尚未保存', no_progress: '多次操作没有新进展', access_restricted: '访问受限，请在浏览器处理',
        login_or_verification_required: '需要登录或验证', note_unavailable: '笔记不可访问', user_pause: '手动暂停',
        process_restart: '上次运行中断', archive_corrupt: '存档校验失败', controlled_page_changed_or_redirected: '页面发生跳转',
        TimeoutError: '页面操作超时', Error: '浏览器操作失败，请检查连接'};
    const text = (id, value) => { const node = el(id); if (node.textContent !== String(value)) node.textContent = value; };
    function pill(id, message, tone = '') { text(id, message); el(id).className = `collect-pill ${tone}`; }
    function complete(t) { return t.body === 'saved' && ['complete_observed', 'not_requested'].includes(t.comments) && !['partial', 'failed'].includes(t.media); }
    function attention(t) { return ['partial', 'blocked', 'failed'].includes(t.comments) && t.status !== 'pending' || t.body === 'unavailable' || ['partial', 'failed'].includes(t.media); }

    function controls() {
        const busy = Boolean(state?.busy), hasBatch = Boolean(state?.batch), connected = state?.connection?.status === 'connected';
        const pages = state?.connection?.pages || [];
        const loginNeeded = pages.some(p => p.logged_in === false || p.blocked);
        el('collectNewPanel').disabled = busy || pending;
        for (const id of ['collectBrowser', 'collectSearch', 'collectEndpoint', 'collectKeyword', 'collectLoad', 'collectBrowseBatch', 'collectBatchSelect']) el(id).disabled = busy || pending;
        for (const id of ['collectCheck', 'collectRefreshBatches']) el(id).disabled = pending;
        el('collectCreate').disabled = busy || pending || !connected || loginNeeded;
        el('collectResume').disabled = !hasBatch || busy || pending;
        el('collectRetry').disabled = !hasBatch || busy || pending;
        el('collectDiscover').disabled = !hasBatch || busy || pending;
        el('collectPause').disabled = !busy || state?.phase === 'pausing';
        el('collectFolder').disabled = !hasBatch || pending;
        el('collectExport').disabled = !hasBatch || pending;
        text('collectStartHint', busy ? '当前批次运行中。安全暂停后可创建新的批次。' : pending ? '正在处理操作，请稍候…' :
            !connected ? '请先打开专用浏览器，并点击“刷新连接”。' : loginNeeded ? '请在 Chrome 完成登录 / 验证后刷新连接。' : '就绪。点击后自动建立存档并开始采集。');
    }

    async function command(action, payload = {}) {
        if (pending && action !== 'pause') return null;
        const ownsPending = !pending;
        if (ownsPending) pending = true;
        controls();
        try {
            const result = await api.collection_command(action, payload);
            if (!result.ok) throw new Error(result.error || '操作未完成');
            if (result.state) render(result.state);
            return result;
        } catch (error) {
            toast(error.message || String(error), 'warning');
            return null;
        } finally {
            if (ownsPending) pending = false;
            controls();
        }
    }

    async function refreshBatches() {
        const result = await command('batches');
        if (!result) return;
        const select = el('collectBatchSelect'), previous = state?.batch || select.value;
        select.replaceChildren();
        if (!result.batches.length) select.add(new Option('还没有已保存的批次', ''));
        for (const batch of result.batches) select.add(new Option(batch.name, batch.path));
        if ([...select.options].some(o => o.value === previous)) select.value = previous;
    }

    function renderTable() {
        if (!state) return;
        const filter = el('collectFilter').value;
        const rows = state.tasks.filter(t => filter === 'all' || filter === 'complete' && complete(t) ||
            filter === 'attention' && attention(t) || filter === 'pending' && t.status !== 'finished');
        const key = JSON.stringify([rows, filter, state.truncated]);
        if (key === tableKey) return;
        tableKey = key;
        const body = el('collectTaskRows');
        body.replaceChildren();
        for (const task of rows) {
            const tr = document.createElement('tr');
            for (const field of ['note_id', 'body', 'comments', 'media']) {
                const td = document.createElement('td');
                if (field === 'note_id') td.textContent = task[field];
                else {
                    const badge = document.createElement('span');
                    const value = task[field];
                    badge.className = 'collect-pill ' + (['saved', 'complete_observed', 'success'].includes(value) ? 'good' : ['partial', 'blocked', 'failed', 'unavailable'].includes(value) ? 'warn' : '');
                    badge.textContent = labels[value] || value;
                    td.append(badge);
                }
                tr.append(td);
            }
            const td = document.createElement('td');
            td.textContent = labels[task.status] || task.status;
            if (task.reason) {
                const small = document.createElement('small');
                small.textContent = reasons[task.reason] || task.reason;
                small.title = task.reason;
                td.append(small);
            }
            tr.append(td); body.append(tr);
        }
        if (!rows.length) {
            const tr = document.createElement('tr'), td = document.createElement('td');
            td.colSpan = 5; td.className = 'collect-empty';
            td.textContent = state.batch ? '当前筛选下没有笔记。采集开始后，结果会自动更新。' : '这里还没有笔记。连接浏览器，创建一个批次开始。';
            tr.append(td); body.append(tr);
        }
        text('collectTableHint', state.truncated ? '当前展示运行中的笔记及最近 200 篇；导出结果表可查看完整清单。筛选仅作用于当前展示范围。' : `当前展示 ${rows.length} 篇。完整原始响应与历史尝试保存在批次目录。`);
    }

    function render(next) {
        if (!next) return;
        state = next;
        if (!initialized) {
            initialized = true;
            el('collectOutput').value = state.output;
            el('collectEndpoint').value = state.config?.cdp || state.endpoint;
            el('collectName').value = '采集-' + new Date().toLocaleString('sv-SE').replace(/[-:]/g, '').replace(' ', '-');
        }
        const connection = state.connection || {}, pages = connection.pages || [], connected = connection.status === 'connected';
        const needsLogin = pages.some(p => p.logged_in === false || p.blocked), loggedIn = pages.some(p => p.logged_in === true);
        pill('collectCdpStatus', connected ? '浏览器已连接' : connection.status === 'error' ? '浏览器未连接' : '连接待检查', connected ? 'good' : '');
        pill('collectLoginStatus', !connected ? '登录待检查' : needsLogin ? '需要登录 / 验证' : loggedIn ? '已登录' : '登录未确认', connected && loggedIn && !needsLogin ? 'good' : connected ? 'warn' : '');
        pill('collectAdapterStatus', pages.some(p => p.adapter?.available) ? '网页请求模块可用' : connected ? '模块未就绪 / 可用兼容模式' : '请求模块待检查', pages.some(p => p.adapter?.available) ? 'good' : '');
        text('collectConnectionMessage', connected ? needsLogin ? '请在 Chrome 完成登录或验证，完成后点击“刷新连接”。' : pages.length ? '可在下方搜索关键词，或直接使用浏览器当前结果页。' : '已连接 Chrome；请打开小红书页面。' : connection.message || '打开 Chrome，手动登录一次即可开始。');
        text('collectCheckedAt', connection.checked_at ? `检查于 ${connection.checked_at}` : '默认本机 9223');
        text('collectBatchTitle', state.name || '等待第一份采集');
        text('collectBatchPath', state.batch || '创建或打开一个批次，进度与结果会显示在这里。');
        el('collectBatchPath').title = state.batch || '';
        pill('collectPhase', phases[state.phase] || state.phase, state.phase === 'running' ? 'active' : state.phase === 'error' ? 'bad' : ['paused', 'pausing'].includes(state.phase) ? 'warn' : '');
        for (const [id, key] of [['collectTotal', 'total'], ['collectComplete', 'complete'], ['collectPartial', 'partial'], ['collectPending', 'pending']]) text(id, state.counts[key] || 0);
        text('collectProgressText', state.counts.total ? `已处理 ${state.counts.processed} / ${state.counts.total} 篇` : '尚未发现笔记');
        text('collectUnavailable', state.counts.unavailable ? `${state.counts.unavailable} 篇不可访问` : '');
        el('collectProgress').max = Math.max(state.counts.total, 1);
        el('collectProgress').value = state.counts.processed;
        text('collectMessage', state.message);
        text('collectPageProgress', state.busy ? `当前篇：一级 ${state.progress.primary} 条 · 回复 ${state.progress.replies} 条｜批次已存响应 ${state.progress.responses} 页` : '原始响应先保存，确认成功后再进入下一页。');
        el('collectLiveDot').classList.toggle('running', state.busy);
        text('collectSavedConfig', state.batch ? `沿用批次参数：间隔 ${state.config.open_interval ?? '—'} 秒 · ${state.config.strategy || 'auto'} · 媒体${state.config.media ? '开启' : '关闭'}。修改参数请新建批次。` : '');
        const nextLogKey = JSON.stringify(state.logs);
        if (nextLogKey !== logKey) {
            logKey = nextLogKey;
            const list = el('collectLogList'), atEnd = list.scrollTop + list.clientHeight >= list.scrollHeight - 20;
            list.replaceChildren();
            for (const item of state.logs) {
                const li = document.createElement('li'), time = document.createElement('time');
                time.textContent = item.time; li.append(time, document.createTextNode(item.message)); list.append(li);
            }
            if (atEnd) list.scrollTop = list.scrollHeight;
        }
        renderTable(); controls();
        if (api && !batchesLoaded) { batchesLoaded = true; void refreshBatches(); }
    }

    function tab(existing) {
        el('collectCreateForm').hidden = existing;
        el('collectExistingPanel').hidden = !existing;
        el('collectNewTab').setAttribute('aria-selected', String(!existing));
        el('collectExistingTab').setAttribute('aria-selected', String(existing));
        if (existing) void refreshBatches();
    }

    function connect(nativeApi, showToast) {
        if (api) return;
        api = nativeApi; toast = showToast;
        el('collectNewTab').onclick = () => tab(false);
        el('collectExistingTab').onclick = () => tab(true);
        el('collectFilter').onchange = renderTable;
        el('collectSource').onchange = () => {
            const links = el('collectSource').value === 'links';
            el('collectLinksArea').hidden = !links;
            el('collectSourceHint').hidden = links;
            el('collectLinks').required = links;
        };
        el('collectCheck').onclick = () => command('check', {endpoint: el('collectEndpoint').value.trim()});
        el('collectBrowser').onclick = async () => {
            const result = await command('browser', {endpoint: el('collectEndpoint').value.trim()});
            if (result) { toast('浏览器已打开。登录后回到这里刷新连接。'); await command('check', {endpoint: el('collectEndpoint').value.trim()}); }
        };
        el('collectSearch').onclick = () => command('search', {keyword: el('collectKeyword').value, endpoint: el('collectEndpoint').value.trim()});
        el('collectKeyword').onkeydown = event => { if (event.key === 'Enter') { event.preventDefault(); el('collectSearch').click(); } };
        el('collectCreateForm').onsubmit = async event => {
            event.preventDefault();
            if (!el('collectCreateForm').reportValidity()) return;
            const number = id => Number(el(id).value);
            const result = await command('create', {name: el('collectName').value, source: el('collectSource').value,
                links: el('collectLinks').value, output: el('collectOutput').value,
                config: {cdp: el('collectEndpoint').value.trim(), strategy: el('collectStrategy').value,
                    open_interval: number('collectInterval'), max_scrolls: number('collectScrolls'),
                    note_timeout: number('collectTimeout'), operation_timeout: number('collectOpTimeout'),
                    max_operations: number('collectOperations'), media: el('collectMedia').checked}});
            if (result) { toast('批次已创建，开始采集。'); await refreshBatches(); }
        };
        el('collectResume').onclick = () => command('start', {mode: 'resume'});
        el('collectRetry').onclick = () => command('start', {mode: 'retry'});
        el('collectDiscover').onclick = () => command('start', {mode: 'discover'});
        el('collectPause').onclick = () => command('pause');
        el('collectFolder').onclick = () => command('open_folder');
        el('collectExport').onclick = async () => { if (await command('export')) toast('已导出 summary.csv，可在数据目录中打开。'); };
        el('collectRefreshBatches').onclick = refreshBatches;
        const load = async path => {
            if (!path) return toast('请选择一个已有批次。', 'warning');
            const result = await command('load', {path});
            if (result) { el('collectEndpoint').value = result.state.config.cdp; toast('批次已打开。'); }
        };
        el('collectLoad').onclick = () => load(el('collectBatchSelect').value);
        async function browse(callback, initial) {
            try { const path = await api.browse_directory(initial || ''); if (path) await callback(path); }
            catch (error) { toast(error.message || String(error), 'warning'); }
        }
        el('collectBrowseBatch').onclick = () => browse(load, state?.output);
        el('collectBrowseOutput').onclick = () => browse(path => { el('collectOutput').value = path; }, el('collectOutput').value);
        el('collectPaste').onclick = async () => { try { el('collectLinks').value = await api.paste_content(); } catch (_) { toast('读取剪贴板失败，请手动粘贴。', 'warning'); } };
    }
    window.XHSCollection = {connect, render};
})();
