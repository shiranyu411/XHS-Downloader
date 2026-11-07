"""Read-only progress report for one keyword batch, in Chinese; sends no request to the platform."""
import argparse
from contextlib import closing
from datetime import datetime
import json
import os
from pathlib import Path
import shutil
import sqlite3
import time
import unicodedata

SETTLED = ('complete_observed', 'sampled', 'skipped')
STATES = {'collecting': '采集中', 'cooldown': '被限制后冷却中', 'backoff': '退出后等待重启', 'paused_by_user': '已暂停',
          'needs_person': '需要你处理', 'finished': '全部完成', 'in_use_elsewhere': '批次被其他程序占用',
          'stopping_stalled': '卡住了，正在重启'}


def pad(text, width, right=False):
    """Pad to a terminal width where CJK characters take two columns."""
    text = str(text)
    space = ' ' * max(0, width - sum(2 if unicodedata.east_asian_width(ch) in 'WF' else 1 for ch in text))
    return space + text if right else text + space


def keyword(metadata):
    meta = json.loads(metadata or '{}')
    return meta.get('keyword') or (meta.get('request') or {}).get('keyword')


def read(batch):
    """The supervisor's last report, per-keyword counts, search progress, controls and task rows."""
    try:
        report = json.loads((batch / 'supervisor.json').read_text())
    except (FileNotFoundError, ValueError):
        report = {}
    with closing(sqlite3.connect((batch / 'state.sqlite').as_uri() + '?mode=ro', uri=True)) as db:
        by_note = {}
        for id_, metadata in db.execute('SELECT note_id, metadata FROM sources'):
            if k := keyword(metadata):
                by_note.setdefault(id_, set()).add(k)
        rows = db.execute("SELECT t.note_id, t.status, t.body, t.comments, coalesce(p.priority, 1) FROM tasks t "
                          "LEFT JOIN task_priority p USING(note_id)").fetchall()
        controls = dict(db.execute('SELECT key, value FROM controls'))
    search = {s['keyword']: s for s in json.loads(controls.get('search_progress') or '[]')}
    counts = {k: dict.fromkeys(('settled', 'useful', 'partial', 'todo'), 0) for k in search}
    for id_, status, body, comments, rank in rows:
        for k in by_note.get(id_, ()):
            c = counts.setdefault(k, dict.fromkeys(('settled', 'useful', 'partial', 'todo'), 0))
            settled = body == 'unavailable' or comments in SETTLED
            c['settled'] += settled
            c['useful'] += comments in ('complete_observed', 'sampled')
            c['partial'] += body == 'saved' and not settled
            c['todo'] += not settled and rank in (0, 1, 2)
    return report, counts, search, controls, rows


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--batch', type=Path, required=True)
    batch = parser.parse_args().batch.expanduser().resolve()
    report, counts, search, controls, rows = read(batch)
    now = time.time()
    recent = [t for t in json.loads(controls.get('pacing_restriction_log') or '[]') if t > now - 48 * 3600]
    paused = (batch / '.pause-requested').exists()
    state = report.get('state', '未知')
    print(f"小红书采集进度  {datetime.now():%m-%d %H:%M}（只读，不联网）\n")
    print(f"状态：{STATES.get(state, state)}{'（已放暂停文件）' if paused and state != 'paused_by_user' else ''}")
    print(f"说明：{report.get('message', '')}" + (f"；下一步 {report['next_at']}" if report.get('next_at') else ''))
    checked = report.get('checked_at', '')
    stale = checked and (now - datetime.fromisoformat(checked).timestamp()) > 120
    print(f"看守程序最近检查：{checked[5:16].replace('T', ' ')}" + ('  ⚠ 超过 2 分钟没更新，看守程序可能没在运行' if stale else ''))
    floor = float(controls.get('pacing_factor_floor') or 0)
    opens = json.loads((batch / 'run.json').read_text())['config'].get('open_interval') or 0
    limit = (f"你设的上限：不低于 {floor:.2f}，即每小时最多约 {3600 / (opens * floor):.0f} 次页面访问"
             if floor and opens else '最低 0.75')
    print(f"48 小时内被平台限制：{len(recent)} 次；当前速度系数 {float(controls.get('pacing_factor') or 1):.2f}（越小越快，{limit}）\n")
    columns = (('关键词', 10), ('已完成', 8), ('其中有评论', 12), ('评论未采完', 12), ('待采', 8))
    print(''.join(pad(name, width, i > 0) for i, (name, width) in enumerate(columns)) + '  搜索')
    for k in sorted(counts, key=lambda k: -counts[k]['settled']):
        c, s = counts[k], search.get(k, {})
        searched = {'complete': '完成', 'pending': '未开始'}.get(s.get('status'), f"进行中（第 {len(s.get('pages') or [])} 页）")
        cells = (k, c['settled'], c['useful'], c['partial'], c['todo'])
        print(''.join(pad(v, width, i > 0) for i, (v, (_, width)) in enumerate(zip(cells, columns))) + '  ' + searched)
    bodies = sum(body == 'saved' for _, _, body, _, _ in rows)
    useful = sum(comments in ('complete_observed', 'sampled') for *_, comments, _ in rows)
    print(f"\n合计：发现 {len(rows)} 篇，已存正文 {bodies} 篇，评论达标 {useful} 篇（同一帖子可能属于多个关键词）")
    print(f"磁盘剩余：{shutil.disk_usage(batch).free / 1024**3:.0f} GB")
    print(f"可读成果：{batch / '可读成果' / 'index.html'}")
    print("\n「已完成」= 评论已按规则采够、或正文判定与主题无关、或帖子已删除；「待采」= 还排队的相关帖子。")
    return 0


if __name__ == '__main__':
    os.environ.setdefault('PYTHONDONTWRITEBYTECODE', '1')
    raise SystemExit(main())
