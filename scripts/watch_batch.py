"""Print one Chinese line per notable event in a keyword batch, for a terminal or log monitor.

Read-only and offline. It prints a line as soon as the supervisor's state changes, the platform
refuses a request, a keyword search starts or finishes, or the supervisor, the collector, a
background helper or the disk needs a look, and again when such a condition clears; and every
--report-every minutes a progress line with the change since the previous one (kept in the batch's
.watch-state.json). A Monitor ends after 30 minutes, so the last poll, the open alarms and the time
of the next progress line are kept in .watch-loop.json: a re-armed watcher reports what happened in
between, repeats no alarm and keeps the schedule.
"""
import argparse
from contextlib import closing
from datetime import datetime
import fcntl
import json
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parent))

from batch_status import SETTLED, STATES, read  # noqa: E402

POLL = 20
STALE = 180    # the launchd supervisor rewrites supervisor.json every 30 s
QUIET = 3600   # collecting this long without a newly finished post is worth a look
LOW_DISK = 25  # GiB; below about 20 the user's rule moves finished files to /Volumes/Game
CLEARED = {'stale': '✅ 看守程序恢复更新了', 'quiet': '✅ 又有新完成的帖子了', 'disk': '✅ 磁盘空间恢复了',
           'unreadable': '✅ 又能读到批次状态了'}
HELPERS = {'readable-results.pid': '可读成果的自动更新', 'exporter.pid': '增量导出'}


def say(text):
    print(stamp(time.time(), text), flush=True)


def notify(text):
    """Write the event to the service log and also show it without requiring an agent session."""
    print(text, flush=True)
    script = f'display notification {json.dumps(text, ensure_ascii=False)} with title "小红书采集 Monitor"'
    try:
        subprocess.run(['osascript', '-e', script], capture_output=True, timeout=5)
    except (OSError, subprocess.TimeoutExpired):
        pass


def stamp(at, text):
    return f"[{datetime.fromtimestamp(at):%H:%M}] {text}"


def name(state):
    return STATES.get(state, state or '未知')


def load(path):
    try:
        return json.loads(path.read_text())
    except (FileNotFoundError, ValueError):
        return {}


def progress(batch, force=False):
    """State, finished posts in total and per keyword, unfinished searches and pacing; None when
    neither the state nor the finished count moved since the last line (a cooldown repeats nothing),
    unless forced."""
    report, counts, search, controls, rows = read(batch)
    path = batch / '.watch-state.json'
    before = load(path)
    settled = sum(body == 'unavailable' or comments in SETTLED for _, _, body, comments, _ in rows)
    useful = sum(comments in ('complete_observed', 'sampled') for *_, comments, _ in rows)
    keywords = {k: c['settled'] for k, c in counts.items()}
    state = report.get('state')
    if not force and before and (before.get('state'), before.get('settled')) == (state, settled):
        return None
    path.write_text(json.dumps({'at': time.time(), 'state': state, 'settled': settled, 'keywords': keywords}, ensure_ascii=False))
    old = before.get('keywords', {})
    per = ' '.join(f"{k} {v}" + (f"(+{v - old[k]})" if k in old and v > old[k] else '')
                   for k, v in sorted(keywords.items(), key=lambda item: -item[1]))
    since = f"（{datetime.fromtimestamp(before['at']):%H:%M} 以来 {settled - before['settled']:+d}）" if before else ''
    searching = [f"{k} 未开始" if s.get('status') == 'pending' else f"{k} 第 {len(s.get('pages') or [])} 页"
                 for k, s in search.items() if s.get('status') != 'complete']
    recent = sum(t > time.time() - 48 * 3600 for t in json.loads(controls.get('pacing_restriction_log') or '[]'))
    return (f"📊 {name(state)}｜已完成 {settled} 篇{since}，其中评论达标 {useful} 篇｜{per}"
            + (f"｜搜索中：{'，'.join(searching)}" if searching else '')
            + f"｜速度系数 {float(controls.get('pacing_factor') or 1):.2f}，48 小时内被限制 {recent} 次")


def poll(batch):
    """What the event checks need, without per-keyword counts."""
    report = json.loads((batch / 'supervisor.json').read_text())
    with closing(sqlite3.connect((batch / 'state.sqlite').as_uri() + '?mode=ro', uri=True)) as db:
        controls = dict(db.execute("SELECT key, value FROM controls WHERE key IN ('search_progress', 'pacing_restriction_log')"))
        last = db.execute("SELECT max(updated) FROM tasks WHERE body='unavailable' OR comments IN (?, ?, ?)", SETTLED).fetchone()[0]
    launches = (report.get('supervisor') or {}).get('launches') or [{}]
    return {'state': report.get('state'), 'message': report.get('message', ''), 'next_at': report.get('next_at') or '',
            'checked': datetime.fromisoformat(report['checked_at']).timestamp() if report.get('checked_at') else 0,
            'launched': launches[-1].get('at') or 0, 'last': datetime.fromisoformat(last).timestamp() if last else 0,
            'search': {s['keyword']: (s.get('status'), len(s.get('pages') or []))
                       for s in json.loads(controls.get('search_progress') or '[]')},
            'refusals': json.loads(controls.get('pacing_restriction_log') or '[]')}


def events(old, new, now):
    if new['state'] != old['state']:
        icon = {'collecting': '✅', 'needs_person': '🚨', 'paused_by_user': '⏸', 'finished': '🏁'}.get(new['state'], '⚠')
        yield (f"{icon} 状态：{name(old['state'])} → {name(new['state'])}｜{new['message']}"
               + (f"｜下一步 {new['next_at']}" if new['next_at'] else ''))
    for t in sorted(set(new['refusals']) - set(old['refusals'])):
        recent = sum(x > now - 48 * 3600 for x in new['refusals'])
        yield f"⚠ 平台在 {datetime.fromtimestamp(t):%H:%M} 拒绝了请求（48 小时内第 {recent} 次），批次按规则停下冷却"
    for k, (status, pages) in new['search'].items():
        was = old['search'].get(k, (None, 0))[0]
        if status != was:
            yield f"🔎 {k}：" + (f"搜索翻完，共 {pages} 页" if status == 'complete' else '开始搜索' if was == 'pending'
                                 else f"搜索状态 {was} → {status}")


def alarms(batch, new, now):
    """Conditions that hold now; each is reported when it starts and when it clears."""
    found = {}
    if now - new['checked'] > STALE:
        found['stale'] = f"⚠ 看守程序 {(now - new['checked']) / 60:.0f} 分钟没更新状态，launchd 里的看守可能停了"
    quiet = now - max(new['last'], new['launched'])
    if new['state'] == 'collecting' and quiet > QUIET:
        found['quiet'] = f"⚠ 状态是采集中，但已经 {quiet / 60:.0f} 分钟没有新完成的帖子"
    free = shutil.disk_usage(batch).free / 1024**3
    if free < LOW_DISK:
        found['disk'] = f"⚠ 磁盘只剩 {free:.0f} GB；低于约 20 GB 要把已完成的文件挪到 /Volumes/Game"
    return found


def alive(pidfile):
    try:
        os.kill(int(pidfile.read_text()), 0)
    except (FileNotFoundError, ValueError, ProcessLookupError):
        return False
    except PermissionError:
        pass
    return True


def helper_alarms(batch):
    return {'helper:' + pidfile: f"⚠ {label}程序没在运行" for pidfile, label in HELPERS.items() if not alive(batch / pidfile)}


def cleared(key):
    return CLEARED.get(key) or f"✅ {HELPERS[key.removeprefix('helper:')]}程序又在运行了"


def follow(batch, every, now=time.time, sleep=time.sleep, out=lambda line: print(line, flush=True)):
    """Event lines as they happen and a progress line every `every` seconds, until the batch finishes."""
    path = batch / '.watch-loop.json'
    kept = load(path)
    old, active, failures = kept.get('poll'), set(kept.get('alarms', [])), 0
    # Without its own file the schedule continues from the last progress line, if there is one.
    due = kept.get('due') or load(batch / '.watch-state.json').get('at', now() - every) + every
    while True:
        t, lines = now(), []
        try:
            new = poll(batch)
        except (OSError, ValueError, KeyError, sqlite3.Error) as error:
            failures += 1
            if failures >= 9 and 'unreadable' not in active:
                active.add('unreadable')
                lines.append(f"⚠ 3 分钟读不到批次状态：{error}")
        else:
            failures = 0
            lines += events(old, new, t) if old else []
            found = {**alarms(batch, new, t), **helper_alarms(batch)}
            lines += [found[key] for key in found.keys() - active] + [cleared(key) for key in active - found.keys()]
            active, old = set(found), new
        if t >= due:
            try:
                lines.append(progress(batch, force=True))
            except (OSError, ValueError, sqlite3.Error) as error:
                lines.append(f"⚠ 读不到进度：{error}")
            # Stay on the schedule unless the watcher was away for a whole period.
            due = due + every if due + every > t else t + every
        for line in lines:
            out(stamp(t, line))
        # Saved after printing: a kill in between repeats a line rather than losing one.
        temp = path.with_name(path.name + '.tmp')
        temp.write_text(json.dumps({'due': due, 'poll': old, 'alarms': sorted(active)}, ensure_ascii=False))
        os.replace(temp, path)
        if old and old['state'] == 'finished':
            return 0
        sleep(POLL)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--batch', type=Path, required=True)
    parser.add_argument('--once', action='store_true', help='print the progress line, if anything moved, and exit')
    parser.add_argument('--report-every', type=float, default=90, metavar='MINUTES',
                        help='minutes between progress lines (default 90)')
    parser.add_argument('--notify', action='store_true', help='also send each event as a macOS notification')
    args = parser.parse_args()
    batch = args.batch.expanduser().resolve()
    if args.once:
        try:
            if line := progress(batch):
                say(line)
        except (OSError, ValueError, sqlite3.Error) as error:
            say(f"⚠ 读不到进度：{error}")
        for pidfile, label in HELPERS.items():
            if not alive(batch / pidfile):
                say(f"⚠ {label}程序没在运行")
        return 0

    # A resumed agent can otherwise leave two continuous watchers racing on the
    # same state files and emitting duplicate reports. `--once` stays lock-free.
    with (batch / '.watch.lock').open('a+') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print('已有一个持续 Monitor 在运行，本进程退出', flush=True)
            return 0
        lock.seek(0)
        lock.truncate()
        lock.write(str(os.getpid()))
        lock.flush()
        output = notify if args.notify else lambda line: print(line, flush=True)
        return follow(batch, args.report_every * 60, out=output)


if __name__ == '__main__':
    raise SystemExit(main())
