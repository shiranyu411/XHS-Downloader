(() => {
    'use strict';
    const $ = id => document.getElementById(id), selected = new Set();
    let api, state, page = 1, busy = false, view = 'pool', rowsKey = '', sourcesKey = '';
    const labels = {saved:'已保存',partial:'部分完成',blocked:'受限暂停',complete_observed:'完整',not_requested:'未采集',disabled:'未下载',success:'完成',failed:'失败',pending:'排队',running:'运行中',paused:'暂停',interrupted:'中断',cancelled:'已取消',unavailable:'不可访问',finished:'已处理'};
    const label = x => labels[x] || x || '—';
    const text = (id, value) => {if ($(id).textContent !== String(value)) $(id).textContent = value;};
    const filters = () => ({query:$('query').value,filter:$('filter').value,source:$('source').value,page});
    const poller = window.createUiPoller({refresh:fetchState,isBusy:()=>state?.busy});
    const refresh = () => api ? poller.request() : Promise.resolve();

    window.setWorkspaceView = name => {
        if (!['pool','collect','results'].includes(name)) return;
        view = name;
        text('heading', {pool:'链接池',collect:'评论采集',results:'结果管理'}[name]);
        document.querySelectorAll('[data-section]').forEach(el => {el.hidden = el.dataset.section !== name;});
        $('tablePanel').hidden = name === 'results';
        if (poller.visible()) void refresh();
    };
    function controls() {
        const locked = busy || !state || state.busy;
        document.querySelectorAll('[data-submit], [data-retry], #hide').forEach(el => {el.disabled = locked || !selected.size;});
        document.querySelectorAll('#import, #create, #browse, [data-action="restore_hidden"]').forEach(el => {el.disabled = locked;});
        $('load').disabled = locked || !$('batches').value;
        document.querySelectorAll('[data-start]').forEach(el => {el.disabled = locked || state.blocked || !state.queues[el.dataset.start];});
        document.querySelectorAll('[data-pause]').forEach(el => {el.disabled = busy || !state?.busy;});
        document.querySelectorAll('[data-action="copy"], [data-action="export_links"]').forEach(el => {el.disabled = busy || !selected.size;});
        document.querySelectorAll('[data-action="folder"], [data-action="export"]').forEach(el => {el.disabled = busy || !state?.batch;});
        $('ack').disabled = locked;
        $('clear').disabled = !selected.size;
        $('selectPage').disabled = !state?.rows.length;
    }
    function selection() {
        text('selected', `已选 ${selected.size} 篇`);
        const rows = state?.rows || [], count = rows.filter(r => selected.has(r.note_id)).length;
        $('selectPage').checked = !!rows.length && count === rows.length;
        $('selectPage').indeterminate = count > 0 && count < rows.length;
        document.querySelectorAll('#rows input[data-note]').forEach(el => {el.checked = selected.has(el.dataset.note);});
        controls();
    }
    function renderRows(rows) {
        const key = JSON.stringify([state.batch, rows]);
        if (rowsKey === key) return; // Keep focus, selection and scroll on unchanged polls.
        rowsKey = key;
        const fragment = document.createDocumentFragment();
        for (const r of rows) {
            const tr = document.createElement('tr'), cell = document.createElement('td'), check = document.createElement('input');
            check.type = 'checkbox'; check.dataset.note = r.note_id;
            check.checked = selected.has(r.note_id); check.setAttribute('aria-label',`选择 ${r.note_id}`);
            check.onchange = () => {if (check.checked) selected.add(r.note_id); else selected.delete(r.note_id); selection();};
            cell.append(check); tr.append(cell);
            const bodyLabel = r.status === 'pending' && r.comments === 'not_requested' && r.body === 'partial' ? '未采集' : label(r.body);
            const values = [r.title || r.note_id, r.sources.join(' / ') || `${r.observations} 次`, `${bodyLabel} / ${label(r.comments)}`, label(r.media), `${label(r.collection_job)} / ${label(r.download_job)} · ${r.reason || '—'}`];
            values.forEach((value,i) => {
                const td=document.createElement('td');
                if (i===0) {
                    const title=document.createElement('span');title.className='note-title';title.textContent=value;title.title=value;td.append(title);
                    if (r.title) {const sub=document.createElement('small');sub.textContent=r.note_id;td.append(sub);}
                } else td.textContent=value;
                tr.append(td);
            });
            fragment.append(tr);
        }
        if (!rows.length) {
            const tr=document.createElement('tr'), td=document.createElement('td');td.colSpan=6;
            td.textContent='暂无符合条件的作品。可以直接下载、浏览发现或手工加入链接；默认工作区会自动保存。';
            tr.append(td);fragment.append(tr);
        }
        $('rows').replaceChildren(fragment);
    }
    function render(s) {
        if (state?.batch !== s.batch) selected.clear();
        state = s; page = s.page;
        text('batch', `当前批次：${s.name}`);
        text('phase', s.blocked ? '受限暂停' : s.busy ? '后台运行中' : '待命');
        if (s.disclaimer_accepted && $('error').dataset.kind === 'onboarding') {
            $('error').hidden = true; text('error', ''); delete $('error').dataset.kind;
        }
        text('notice', s.disclaimer_accepted ? s.message : '首次使用请在作品下载页确认使用声明（不是小红书登录）。确认后所有页面共享状态。');
        for (const key of ['total','complete','partial','downloaded']) text(key, s.counts[key]);
        text('path', s.batch || '未打开批次');
        text('configSummary', s.batch ? `当前批次：打开间隔 ${s.config.open_interval}s · 分页间隔 ${s.config.page_interval}s · 单篇超时 ${s.config.note_timeout}s；改变上方参数仅影响下一次新建批次。` : '间隔不保证避免访问限制；超出预算按部分完成保存。');
        const nextSources = JSON.stringify(s.sources);
        if (sourcesKey !== nextSources) {
            sourcesKey = nextSources;
            const chosen = $('source').value;
            $('source').replaceChildren(new Option('全部来源',''), ...s.sources.map(name => new Option(name,name)));
            $('source').value = s.sources.includes(chosen) ? chosen : '';
        }
        $('restricted').hidden = !s.blocked;
        text('queues', `待运行：评论 ${s.queues.comments} · 下载 ${s.queues.download}`);
        text('progress', `最近尝试：一级 ${s.progress.primary} · 二级回复 ${s.progress.replies}（不是当前批次累计总量）`);
        text('logs', s.logs.map(l => `[${l.time}] ${l.message}`).join('\n'));
        renderRows(s.rows);
        text('range', `第 ${page} 页 · 符合筛选 ${s.filtered} 篇（每页 50）`);
        $('previous').disabled = page<=1; $('next').disabled = page*50>=s.filtered;
        selection();
    }
    async function fetchState() {
        if (!api || busy) return;
        const payload = filters();
        try {
            const s = await api.workspace_command('state', payload);
            // A search/page change while RPC is in flight must not be lost or rolled back.
            if (!busy && JSON.stringify(payload) === JSON.stringify(filters())) render(s);
            else if (!busy) void refresh();
        } catch(e) {error(e);}
    }
    function error(e) {
        $('error').hidden=false;
        text('error', e.message || String(e));
        $('error').dataset.kind = $('error').textContent.includes('首次使用确认') ? 'onboarding' : 'operation';
    }
    async function run(action,payload={}) {
        if (!api || busy) return;
        busy=true;$('error').hidden=true;controls();
        try {
            await api.workspace_command(action,payload);
            if (action==='hide') selected.clear();
            if (['create','load'].includes(action)) {
                page=1;$('query').value='';$('filter').value='all';$('source').value='';await batches();
            }
            if (action==='import') $('links').value='';
        } catch(e) {error(e);}
        finally {busy=false;controls();await refresh();}
    }
    async function batches() {
        const list = await api.workspace_command('batches'), chosen = $('batches').value;
        $('batches').replaceChildren(new Option('选择已有批次…',''), ...list.map(b => new Option(b.name,b.path)));
        $('batches').value = list.some(b => b.path === chosen) ? chosen : '';
        controls();
    }
    $('create').onclick=()=>run('create',{name:$('name').value,config:{open_interval:Number($('openInterval').value),page_interval:Number($('pageInterval').value),note_timeout:Number($('noteTimeout').value),max_operations:Number($('maxOperations').value)}});
    $('load').onclick=()=>run('load',{path:$('batches').value});
    $('batches').onchange=controls;
    $('browse').onclick=async()=>{try {const path=await api.browse_directory('');if(path) await run('load',{path});}catch(e){error(e);}};
    $('import').onclick=()=>run('import',{links:$('links').value});
    $('search').onclick=()=>{page=1;void refresh();};
    $('query').onkeydown=e=>{if(e.key==='Enter'){page=1;void refresh();}};
    $('filter').onchange=$('source').onchange=()=>{page=1;void refresh();};
    $('previous').onclick=()=>{page--;void refresh();};$('next').onclick=()=>{page++;void refresh();};
    $('clear').onclick=()=>{selected.clear();selection();};
    $('selectPage').onchange=e=>{for(const r of state?.rows || []) {if(e.target.checked)selected.add(r.note_id);else selected.delete(r.note_id);}selection();};
    $('hide').onclick=()=>{if(confirm(`隐藏 ${selected.size} 篇作品？原始存档不会删除。`))run('hide',{ids:[...selected]});};
    $('ack').onclick=()=>{if(confirm('确认已按平台提示等待，并人工检查登录与访问恢复？这不会自动开始任务。'))run('ack_restriction');};
    document.querySelectorAll('[data-submit],[data-retry]').forEach(el=>{el.onclick=()=>run('submit',{ids:[...selected],kind:el.dataset.submit||el.dataset.retry,retry:!!el.dataset.retry});});
    document.querySelectorAll('[data-start]').forEach(el=>{el.onclick=()=>run('start',{mode:el.dataset.start});});
    document.querySelectorAll('[data-pause]').forEach(el=>{el.onclick=()=>run('pause');});
    document.querySelectorAll('[data-action]').forEach(el=>{el.onclick=()=>run(el.dataset.action,{ids:[...selected]});});
    document.querySelectorAll('[data-go]').forEach(el=>{el.onclick=()=>api?.navigate_view(Number(el.dataset.go)).catch(error);});
    window.refreshWorkspace = refresh;
    window.addEventListener('pywebviewready',async()=>{
        api=window.pywebview.api;await refresh();
        try{await batches();}catch(e){error(e);}
        poller.start();
    }, {once:true});
    window.setWorkspaceView(view);
    controls();
})();
