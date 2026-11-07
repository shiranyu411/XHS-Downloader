(() => {
  'use strict';
  const $ = id => document.getElementById(id);
  const labels = {body:'正文',media:'帖子媒体',comments:'评论与回复',comment_media:'评论图片'};
  const reasons = {browser_connection_error:'浏览器页面关闭或连接中断，可继续采集',browser_timeout:'浏览器响应超时，可继续采集',TargetClosedError:'浏览器页面关闭或连接中断，可继续采集',TimeoutError:'浏览器响应超时，可继续采集',has_more_false:'已到搜索末页',user_pause:'已暂停，可继续',search_page_changed:'搜索页面发生变化',access_restricted:'平台限制访问，请处理后继续',login_or_verification_required:'请先完成登录或验证',observed_end:'已到评论末页',operation_limit:'本次操作已停止，可继续',operation_response_timeout:'响应超时，可继续',note_or_page_timeout:'页面超时，可继续',adapter_unavailable:'接口适配不可用',pagination_evidence_invalid:'分页证据异常，需检查',missing_has_more:'搜索缺少结束标志',repeated_search_page:'搜索返回重复页',note_unavailable:'帖子暂不可访问'};
  let state = {}, selected = null, pending = false, rendered = '', detailVersion = '', previousBatch = null, exportFolder = '';
  async function api(path, data) {
    const response = await fetch('/api/' + path, data === undefined ? {} : {method:'POST',headers:{'Content-Type':'application/json','X-XHS-Local':'1'},body:JSON.stringify(data)});
    let result;
    try { result = await response.json(); } catch (_) { throw Error('本地服务暂不可用，请稍后重试'); }
    if (!response.ok) throw Error(result.error || result.detail || '操作未完成');
    return result;
  }
  function notice(message, success=false) { $('notice').textContent=message; $('notice').className=success?'success':''; $('notice').hidden=!message; }
  function text(tag, value, className) { const node=document.createElement(tag); node.textContent=value || ''; if(className)node.className=className; return node; }
  function date(value) { const n=Number(value); if(!n)return ''; return new Date(n<1e12?n*1000:n).toLocaleString('zh-CN',{hour12:false}); }
  function displayReason(reason) { return reasons[reason] || (reason ? '仍有未完成内容：' + reason : ''); }
  function media(items, note) {
    const grid=text('div','','media-grid');
    for(const item of items || []) {
      if(!item.verified) { grid.append(text('div','图片或视频尚未保存，可继续补采','file-failed')); continue; }
      const url=`/media/${encodeURIComponent(note)}/${encodeURIComponent(item.media_id)}`;
      if(item.kind.includes('video')) { const el=document.createElement('video'); el.src=url; el.controls=true; el.preload='metadata'; grid.append(el); }
      else { const link=document.createElement('a'); link.href=url; link.target='_blank'; link.rel='noopener'; const img=document.createElement('img'); img.src=url; img.alt=item.comment_id?'评论图片':'帖子图片'; img.loading='lazy'; link.append(img); grid.append(link); }
    }
    return grid;
  }
  function comment(item, note) {
    const section=text('section','',`comment ${item.kind==='reply'?'reply':''}`);
    section.id='comment-'+item.comment_id;
    const heading=text('div','','comment-heading'); heading.append(text('strong',item.user_name || '用户'));
    heading.append(text('span',date(item.created_at)),text('span',item.like_count?`${item.like_count} 赞`:''));
    section.append(heading);
    if(item.target_comment_id) { const target=text('a',`回复 ${item.target_user_name || '其他评论'}`,'comment-heading');target.href='#comment-'+item.target_comment_id;section.append(target); }
    if(item.text) section.append(text('div',item.text,'comment-text'));
    section.append(media(item.media,note));
    if(item.relationship_issue) section.append(text('p','回复关系未能确认','file-failed'));
    return section;
  }
  async function showPost(id, force=false) {
    if(!force && selected===id)return;
    const batch=state.batch;
    selected=id; $('detail').classList.add('loading'); renderList();
    try {
      const post=await api('post/'+encodeURIComponent(id));
      if(selected!==id || state.batch!==batch)return;
      const body=post.body || {}, article=text('div','','post-body');
      article.append(text('span',post.outcome==='complete'?'完整资料':post.outcome==='restricted'?'访问受限':'已保存的资料','eyebrow'));
      article.append(text('h2',post.title),text('div',[body.author_name,date(body.published_at)].filter(Boolean).join(' · '),'post-meta'));
      article.append(text('div',body.text || (post.body?'这篇帖子没有文字正文':'正文尚未保存'),'body-text'));
      if(body.tags)article.append(text('div',body.tags,'tags'));
      article.append(media(post.media_files.filter(m=>!m.comment_id),id));
      const statuses=text('div','','post-states');
      for(const [key,label] of Object.entries(labels))statuses.append(text('span',label+' · '+(['saved','success','complete_observed'].includes(post.status[key])?'已保存':'待补采')));
      article.append(statuses);
      if(post.outcome!=='complete')article.append(text('p',displayReason(post.status.reason),'file-failed'));
      const heading=text('div','','comments-heading'); heading.append(text('h3','评论与回复'),text('span',`${post.comments.length} 条已保存`,'eyebrow')); article.append(heading);
      const roots=post.comments.filter(c=>c.kind==='primary'), replies=post.comments.filter(c=>c.kind==='reply');
      const displayed=new Set();
      for(const root of roots) { article.append(comment(root,id)); for(const reply of replies.filter(r=>r.root_comment_id===root.comment_id)){article.append(comment(reply,id));displayed.add(reply.comment_id);} }
      for(const reply of replies.filter(r=>!displayed.has(r.comment_id)))article.append(comment(reply,id));
      if(!post.comments.length)article.append(text('p',post.status.comments==='complete_observed'?'平台当前未返回可访问评论':'评论尚未保存'));
      const evidence=document.createElement('details'); evidence.className='evidence'; evidence.append(text('summary','原始记录与保存状态'));
      evidence.append(text('p',`帖子 ID：${id}`),text('p',`正文证据事件：${body.event_id || '尚无'}；原始 JSON 保留在采集目录 raw/。`));
      if(post.warnings.length)evidence.append(text('p',post.warnings.join('；')));
      article.append(evidence); $('detail').replaceChildren(article);
    } catch(e) { notice(e.message); }
    finally { $('detail').classList.remove('loading'); }
  }
  function renderList() {
    const query=$('filter').value.trim().toLowerCase();
    const tasks=(state.tasks || []).filter(t=>(t.note_id+' '+(t.title||'')+' '+t.missing.map(k=>labels[k]).join(' ')).toLowerCase().includes(query));
    const fingerprint=JSON.stringify([tasks,selected]); if(rendered===fingerprint)return; rendered=fingerprint;
    const nodes=tasks.map(task=>{ const button=text('button','','post'); button.type='button';button.setAttribute('aria-current',String(task.note_id===selected));
      button.append(text('strong',task.title || task.note_id),text('small',task.missing.length?'待补：'+task.missing.map(k=>labels[k]).join('、'):'正文、媒体和评论均已保存'));
      button.append(text('span',task.status==='running'?'正在采集':task.missing.length?'部分保存':'完整资料','badge'+(task.missing.length?' partial':'')));
      button.addEventListener('click',()=>showPost(task.note_id)); return button; });
    $('posts').replaceChildren(...(nodes.length?nodes:[text('div',query?'没有匹配的帖子':'采集的帖子会出现在这里','empty-list')]));
  }
  function render() {
    if(previousBatch!==state.batch){selected=null;rendered='';detailVersion='';previousBatch=state.batch;$('detail').replaceChildren(text('div','选择一篇帖子查看完整资料','empty-detail'));}
    document.body.classList.toggle('has-collection',!!state.batch);
    const c=state.counts || {};
    for(const [id,key] of Object.entries({'discovered':'discovered','completed':'complete','saved-media':'media','primary-count':'primary','reply-count':'replies'}))$(id).textContent=c[key] || 0;
    $('progress-panel').hidden=!state.batch; $('pause').hidden=!state.busy; $('resume').hidden=!state.batch || state.busy || state.outcome==='complete';
    $('pause').disabled=state.phase==='pausing'; $('pause').textContent=state.phase==='pausing'?'正在保存…':'暂停';
    $('resume').textContent=state.access_restricted?'已处理登录或验证，继续':'继续采集';
    $('start').disabled=state.busy || pending; $('start').textContent=state.busy?'正在采集…':'开始采集 →';
    $('export').disabled=!state.batch || !c.discovered || pending; $('batch').disabled=state.busy || pending;
    $('phase-dot').className='dot'+(state.busy?' running':'');
    const outcome=state.outcome==='complete'?'全部可访问内容已保存':state.access_restricted?'采集已暂停，请在浏览器处理登录或验证':state.phase==='error'?'采集未能启动，请检查浏览器连接':state.phase==='paused'?'采集已暂停，进度已保存':!state.busy?'已保存当前资料，可继续补采':state.message || '准备采集';
    $('run-message').textContent=outcome; $('current-note').textContent=state.current?'当前帖子：'+state.current:(!state.busy && state.phase==='error'?state.message:'');
    $('coverage').replaceChildren(...(state.search || []).map(s=>text('span',`${s.keyword} · ${s.pages} 页 / ${s.discovered} 篇 · ${s.status==='complete'?'已到末页':displayReason(s.reason)||'发现中'}`)));
    $('archive-path').textContent=state.batch?'保存位置：'+state.batch:'';
    const options=(state.batches || []).map(b=>{const o=text('option',b.name);o.value=b.path;return o;});
    if(options.length)$('batch').replaceChildren(...options);$('batch').value=state.batch || '';
    renderList();
    if(!selected && state.tasks?.some(t=>t.body==='saved'))showPost(state.tasks.find(t=>t.body==='saved').note_id);
    const task=(state.tasks || []).find(t=>t.note_id===selected);const version=task?JSON.stringify([task.updated,c.media,c.primary,c.replies]):'';
    if(selected && version!==detailVersion){detailVersion=version;showPost(selected,true);}
  }
  async function refresh(){try{state=await api('state');render();}catch(e){notice(e.message);}}
  async function action(name,payload={}) { if(pending)return;pending=true;render();try { const result=await api(name,payload);if(result.folder){exportFolder=result.folder;$('export-result').hidden=false;notice('已导出到：'+result.folder,true);}else if(result.message && name==='browser')notice(result.message,true);else notice('');await refresh();return result; }catch(e){notice(e.message);}finally{pending=false;render();} }
  $('collect-form').addEventListener('submit',e=>{e.preventDefault();action('collect',{text:$('target').value,cdp:$('cdp').value});});
  $('pause').onclick=()=>action('pause');$('resume').onclick=()=>action('resume',{confirmed_access:!!state.access_restricted});$('export').onclick=()=>action('export');
  $('open-export').onclick=()=>action('open-export');$('copy-export').onclick=async()=>{try{await navigator.clipboard.writeText(exportFolder);notice('导出路径已复制',true);}catch(_){notice('复制未成功，请复制上方显示的导出路径');}};
  $('login').onclick=()=>action('browser',{cdp:$('cdp').value});$('settings-open').onclick=()=>$('settings').showModal();
  $('batch').onchange=()=>action('load',{path:$('batch').value});$('filter').oninput=renderList;
  $('open-archive').onclick=async()=>{if(await action('load',{path:$('archive-input').value}))$('settings').close();};
  refresh().then(()=>{if(state.endpoint)$('cdp').value=state.endpoint;});
  setInterval(()=>{if(!pending && !document.hidden)refresh();},1800);
})();
