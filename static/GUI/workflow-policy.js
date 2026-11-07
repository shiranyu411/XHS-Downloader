/* Pure presentation helpers. Server scope tokens and permissions are authoritative. */
(function (root) {
    'use strict';
    const names = {discover:'收集作品链接', comments:'正文与评论采集', download:'媒体下载', passive:'浏览记录'};
    const categories = {new:'新作品',ready:'已登记未处理',pending:'已在待处理队列',resume:'旧暂停 / 中断',retry:'需明确补采 / 重试',complete:'已有处理结果',running:'正在处理',hidden:'已隐藏'};
    const reasons = {
        existing_download_record:'已有下载去重记录；文件位置未核验', categories_disabled:'相关下载类别关闭，没有保存媒体',
        no_new_files:'没有新增文件；可能已有文件，未核验完整文件集', skip_unknown:'已跳过，旧记录未说明原因',
        download_completed:'下载器已完成；文件是否被移动需另行确认', download_completed_unverified:'已处理，文件未核验',
        note_or_page_timeout:'达到单篇时间预算，已有部分结果保留', operation_limit:'达到单篇操作预算，已有部分结果保留',
        user_pause:'用户暂停，已有结果保留', process_restart:'上次进程中断，未自动继续', archive_corrupt:'存档校验异常，需要检查',
        session_budget_reached:'动作预算已用完，休息结束后仍需明确继续', access_restricted:'平台访问受限，需要人工处理',
        login_or_verification_required:'需要在浏览页人工登录或验证', no_progress:'连续未发现有效分页进展，已停止',
        discovery_page_changed:'发现时页面已改变，已保存先前登记的作品', observed_end:'本次可见分页已完成'
    };
    const policy = {
        names, categories,
        reason(value) { return reasons[value] || value || ''; },
        canStart(state, mode) {
            return !!state && state.disclaimer_accepted !== false && !state.busy && !state.closing && !!state.permissions?.[mode]?.can_start &&
                (['discover','passive'].includes(mode) || state.queues?.[mode] > 0);
        },
        phase(state, now = Date.now() / 1000) {
            if (!state) return '状态尚未确认';
            if (state.phase_text) return state.phase_text;
            if (state.closing) return '正在安全关闭';
            if (state.disclaimer_accepted === false) return '首次使用确认';
            if (state.stopping && state.busy) return state.blocked ? '平台要求暂停，正在保存已收到内容' : '正在暂停并保存最后响应';
            if (state.blocked) return '受限暂停，需要人工处理';
            if (state.busy) return `${names[state.mode] || '后台任务'}中`;
            const until = Math.max(state.pacing?.cooldown_until || 0, state.permissions?.comments?.until || 0);
            if (until > now) return '冷却 / 预算休息中，不会自动开始';
            if ((state.queues?.comments || 0) + (state.queues?.download || 0)) return '有未完成作业，未在后台执行';
            return '可以添加或选择作品';
        },
        observation(row) {
            const o = row.observed;
            return o ? `浏览观察 ${o.comments_total} 条 · ${o.body_saved ? '有原始正文响应' : '无正文响应'} · 未验证完整性` : '';
        },
        media(row) {
            if (row.media_reason) return policy.reason(row.media_reason);
            if (row.media === 'success') return '有完成记录，文件未核验';
            if (row.media === 'skipped') return '已跳过，没有确认保存文件';
            return row.media === 'disabled' ? '未安排媒体下载' : `媒体：${row.media || '未知'}`;
        },
        selection(ids, rows) {
            const visible = rows.filter(r => ids.has(r.note_id)).length;
            return `已选 ${ids.size} 篇：本页 ${visible} 篇，其他页面 / 筛选外 ${ids.size-visible} 篇`;
        },
        scopePreview(value) {
            const detail = Object.entries(value.counts).filter(([,n])=>n).map(([key,n])=>`${categories[key]} ${n} 篇`).join('；');
            const intent = {start:'开始',resume:'继续',retry:'补采 / 重试'}[value.intent];
            let text = `本次范围 ${value.total} 篇：${detail || '没有作品'}。\n将${intent}${names[value.mode]} ${value.count} 篇。`;
            if (value.reason) text += `\n${value.reason}`;
            const config=value.config;
            if(config) text+=`\n打开间隔 ${config.open_interval} 秒；分页间隔 ${config.page_interval} 秒；单篇时间预算 ${config.note_timeout} 秒。`;
            if(value.media_settings) text+=`\n${policy.mediaSettings(value.media_settings)}`;
            return text+'\n只执行本次可处理集合，不带跑其他队列，不连带启动媒体 / 评论。';
        },
        mediaSettings(settings) {
            if (!settings) return '下载器设置尚未确认';
            return `当前启用：${[['image_download','图片'],['video_download','视频'],['video_cover_download','视频封面'],['live_download','Live Photo']].filter(([k])=>settings[k]).map(([,n])=>n).join(' / ') || '无媒体类别'}；数据请求平均等待 ${settings.avg_delay ?? '未知'} 秒（不是固定倒计时）。`;
        },
        preview(value) {
            return `即将${names[value.mode]} ${value.count} 篇，其中旧暂停/中断 ${value.resumed} 篇。\n` +
                value.note_ids.slice(0, 8).join('\n') + (value.count > 8 ? '\n…' : '') +
                '\n只处理这次预览范围，不自动追加。确认开始？';
        }
    };
    if (typeof module !== 'undefined') module.exports = policy;
    root.WorkspacePolicy = policy;
})(typeof window !== 'undefined' ? window : globalThis);
