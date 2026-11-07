"""Keep one keyword batch collecting at its configured slow pace: no probing, no route switching.

Run by launchd (KeepAlive). Every 30 s:
- a refusal on the direct API channel stops the batch and waits for a person;
  it is never retried automatically and the browser is never restarted for it;
- never launches while <batch>/.pause-requested exists (the project's pause switch);
- a refusal (workspace_blocked=access_restricted) waits out the batch's persisted
  cooldown (by default 6 h, then 18 h, then 72 h within 48 h) and resumes at the slower pace;
  at the 3rd refusal within 48 h it notifies and waits for a person;
- a captcha or login gate notifies and waits for a person (ack-restriction);
- other exits relaunch after 0/15/30/60 min; 4 launches in a row without progress
  notify and wait. An API collector with no successful response/media save for
  30 min is stopped; error-log activity is not progress.
Status: <batch>/supervisor.json. `--reset` (or touching <batch>/.supervisor-reset)
clears the no-progress streak.
"""
import argparse
from contextlib import closing
from datetime import datetime
import json
import os
from pathlib import Path
import shutil
import signal
import sqlite3
import subprocess
import sys
import time
from urllib.request import urlopen

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from source.collection.store import Store, archive_busy, atomic_write, encode  # noqa: E402


WINDOW = 48 * 3600
BACKOFF = (0, 15 * 60, 30 * 60, 60 * 60)
STALL = 30 * 60
TICK = 30


def clock(epoch):
    return datetime.fromtimestamp(epoch).strftime('%m-%d %H:%M')


def tail(path):
    try:
        with path.open('rb') as f:
            f.seek(max(0, f.seek(0, 2) - 4096))
            lines = [line for line in f.read().decode(errors='replace').splitlines() if line.strip()]
        return lines[-1][-300:] if lines else ''
    except OSError:
        return ''


class Supervisor:
    def __init__(self, batch):
        self.batch = batch
        settings = json.loads((batch / 'run.json').read_text())
        self.api_mode = settings.get('engine') == 'http_api'
        self.report_interval = float(settings.get('progress_report_interval_seconds') or 0)
        self.path = batch / 'supervisor.json'
        self.reset = batch / '.supervisor-reset'
        try:
            self.state = json.loads(self.path.read_text()).get('supervisor', {})
        except (FileNotFoundError, ValueError, AttributeError):
            self.state = {}
        self.state.setdefault('launches', [])
        self.children = []
        self.previous = None

    def query(self, sql, args=()):
        with closing(sqlite3.connect((self.batch / 'state.sqlite').as_uri() + '?mode=ro', uri=True)) as db:
            return db.execute(sql, args).fetchall()

    def controls(self):
        return dict(self.query("SELECT key, value FROM controls WHERE key IN ('workspace_blocked', 'pacing_cooldown_until', "
                               "'pacing_session_rest_until', 'pacing_restriction_log', 'pacing_refusal_manual_threshold', "
                               "'pacing_factor', 'search_progress', 'api_stop_evidence', 'api_reply_blocked', 'api_primary_stage_complete', "
                               "'api_chrome_restart')"))

    def worker(self):
        try:
            pid = int((self.batch / 'collector.pid').read_text())
        except (FileNotFoundError, ValueError):
            return None
        out = subprocess.run(['ps', '-p', str(pid), '-o', 'stat=', '-o', 'command='],
                             capture_output=True, text=True).stdout.strip()
        stat, _, command = out.partition(' ')
        return pid if stat and not stat.startswith('Z') and 'COLLECT' in command and str(self.batch) in command else None

    def finished(self):
        if self.api_mode:
            return self.query("""SELECT count(*) FROM tasks WHERE body='saved'
                AND comments='complete_observed' AND media='success' AND comment_media='success'""")[0][0]
        return self.query("SELECT count(*) FROM tasks WHERE status='finished'")[0][0]

    def remaining(self, controls):
        tasks = self.query("""SELECT count(*) FROM tasks t LEFT JOIN task_priority p USING(note_id)
            WHERE coalesce(p.priority, 1) >= 0 AND t.body != 'unavailable'
            AND (t.status IN ('pending', 'interrupted', 'paused') OR t.comments IN ('partial', 'blocked', 'failed'))""")[0][0]
        if self.api_mode:
            tasks = self.query("""SELECT count(*) FROM tasks WHERE body!='unavailable'
                AND NOT (body='saved' AND comments='complete_observed' AND media='success' AND comment_media='success')""")[0][0]
        searches = sum(s.get('status') != 'complete' for s in json.loads(controls.get('search_progress') or '[]'))
        return tasks, searches

    def failures(self):
        """Launches in a row that ended without a finished post and without a refusal."""
        count = 0
        for launch in reversed(self.state['launches']):
            if launch['at'] < self.state.get('reset_at', 0) or 'ended' not in launch or launch.get('progress') or launch.get('blocked'):
                break
            count += 1
        return count

    def log(self, **fields):
        print(json.dumps({'time': datetime.now().isoformat(timespec='seconds'), **fields}, ensure_ascii=False), flush=True)

    def attention(self, key, message):
        if self.state.get('notified') != key:
            self.state['notified'] = key
            script = f'display notification {json.dumps(message, ensure_ascii=False)} with title "小红书采集" sound name "Glass"'
            subprocess.run(['osascript', '-e', script], capture_output=True)
            self.log(action='notify', reason=key, message=message)
        return 'needs_person' if key != 'finished' else 'finished', message, None

    def chrome_ready(self):
        cdp = json.loads((self.batch / 'run.json').read_text())['config']['cdp']
        try:
            with urlopen(cdp.rstrip('/') + '/json/version', timeout=3) as response:
                return response.status == 200
        except OSError:
            return False

    def acknowledge(self, recent, until):
        """The cooldown has passed: lift the batch lock the collector refuses to start under."""
        store = Store(self.batch)
        try:
            if store.control('workspace_blocked') == 'access_restricted':
                store.control('workspace_blocked', '')
                store.save(None, None, 'supervisor_resume', encode({
                    'reason': 'cooldown_elapsed', 'refusals_within_48h': recent, 'cooldown_until': until,
                    'manual_verification_claimed': False}))
        finally:
            store.close()

    def launch(self, reason):
        command = [str(REPO / '.venv/bin/python'), '-u', str(REPO / 'main.py'), 'COLLECT', 'resume', '--batch', str(self.batch)]
        if reason == 'api_rate_limit_elapsed':
            command.append('--retry-api-once')
        with (self.batch / 'collection.log').open('a') as log:
            worker = subprocess.Popen(command, cwd=REPO, stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT,
                                      start_new_session=True, env={**os.environ, 'PYTHONPATH': str(REPO)})
        atomic_write(self.batch / 'collector.pid', str(worker.pid).encode())
        # Keep the Mac from idle-sleeping (display may sleep) only while this collector runs.
        awake = subprocess.Popen(['/usr/bin/caffeinate', '-i', '-w', str(worker.pid)], stdin=subprocess.DEVNULL,
                                 stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)
        self.children += [worker, awake]
        self.state['launches'] = self.state['launches'][-50:] + [
            {'at': time.time(), 'pid': worker.pid, 'reason': reason, 'finished_before': self.finished()}]
        self.state.pop('notified', None)
        self.log(action='launch', pid=worker.pid, reason=reason)

    def watch(self, pid, now):
        if self.api_mode:
            # Error records, replayed note objects and empty progress snapshots
            # are not collection progress. Include durable media saves as well.
            seen = self.query("""SELECT max(at) FROM (
                SELECT seen AS at FROM events WHERE kind='network_json' AND valid=1
                    AND json_extract(metadata, '$.business_success')=1
                UNION ALL SELECT updated AS at FROM media_files WHERE status='saved'
            )""")
        else:
            seen = self.query('SELECT seen FROM events ORDER BY id DESC LIMIT 1')
        age = now - datetime.fromisoformat(seen[0][0]).timestamp() if seen and seen[0][0] else float('inf')
        launch = self.state['launches'][-1] if self.state['launches'] and self.state['launches'][-1]['pid'] == pid else {}
        idle = min(age, now - launch.get('at', 0))
        if idle < STALL:
            elapsed = f'{int(age)} 秒前' if age != float('inf') else '尚无'
            return 'collecting', f'采集中，最近一次有效数据写入：{elapsed}', None
        stop = self.state.setdefault('stopping', {})
        if str(pid) not in stop:
            stop[str(pid)] = now
            os.kill(pid, signal.SIGTERM)  # the CLI pauses safely on SIGTERM
            self.log(action='stop_stalled_collector', pid=pid, idle_seconds=int(idle))
        elif now - stop[str(pid)] > 180:
            os.kill(pid, signal.SIGKILL)
            self.log(action='kill_stalled_collector', pid=pid)
        return 'stopping_stalled', f'{int(idle // 60)} 分钟没有新写入，正在停止后重启', None

    def decide(self, now, controls):
        pid = self.worker()
        if pid:
            return self.watch(pid, now)
        self.state.pop('stopping', None)
        last = self.state['launches'][-1] if self.state['launches'] else None
        if last and 'ended' not in last:
            last.update(ended=now, progress=self.finished() - last['finished_before'],
                        blocked=controls.get('workspace_blocked') or '', exit_line=tail(self.batch / 'collection.log'))
            self.log(action='collector_exited', **{k: last[k] for k in ('pid', 'progress', 'blocked', 'exit_line')})
        if archive_busy(self.batch):
            return 'in_use_elsewhere', '批次正被其他窗口或进程使用，等待其结束', None
        if (self.batch / '.pause-requested').exists():
            return 'paused_by_user', '已暂停（批次目录里有 .pause-requested）；删除该文件后自动继续', None
        blocked = controls.get('workspace_blocked') or ''
        proof = json.loads(controls.get('api_stop_evidence') or '{}')
        recover_cdp = (blocked == 'api_protocol_or_transport_failures'
                       and proof.get('stage') == 'native_cdp_body'
                       and 'No resource with given identifier found' in proof.get('error', ''))
        until = max(float(controls.get('pacing_cooldown_until') or 0), float(controls.get('pacing_session_rest_until') or 0))
        recent = [t for t in json.loads(controls.get('pacing_restriction_log') or '[]') if t > now - WINDOW]
        manual_threshold = int(controls.get('pacing_refusal_manual_threshold') or 3)
        if blocked == 'access_restricted' and manual_threshold > 0 and len(recent) >= manual_threshold:
            return self.attention('refusal_limit', f'48 小时内第 {len(recent)} 次被限制，已停止自动续采'
                                  + (f'，冷却至 {clock(until)}' if until > now else ''))
        if blocked == 'api_access_restricted':
            # Public build: a platform refusal on the direct API channel is never
            # retried automatically and the browser is never restarted for it.
            proof = json.loads(controls.get('api_stop_evidence') or '{}')
            return self.attention('api_access_review', f"平台拒绝了 API 请求（HTTP {proof.get('http_status', '?')} / "
                                  f"业务码 {proof.get('business_code', '?')}），已停止；请人工确认后再决定是否继续")
        elif blocked.startswith('api_') and not recover_cdp:
            return self.attention(blocked, f'API 采集已停止：{blocked}；检查采集日志，不通过登录确认清除此错误')
        if blocked and blocked not in {'access_restricted', 'api_access_restricted'} and not recover_cdp:
            return self.attention(blocked, '需要你在专用 Chrome 里处理登录或验证，然后运行 ack-restriction --confirmed')
        if controls.get('api_reply_blocked') and controls.get('api_primary_stage_complete') == '1':
            return self.attention('reply_scope_pending', '正文、一级评论和已知媒体阶段已结束；二级回复仍待恢复，未宣称全量完成')
        if now < until:
            return 'cooldown', f'被限制后冷却中，{clock(until)} 后自动以更慢速度继续', until
        tasks, searches = self.remaining(controls)
        if not tasks and not searches:
            return self.attention('finished', '本批次的小红书采集已全部完成')
        if shutil.disk_usage(self.batch).free < 2 * 1024**3:
            return self.attention('disk', '磁盘剩余不足 2 GB，已暂停自动续采')
        if not self.chrome_ready():
            return self.attention('chrome', '专用 Chrome 没有打开（端口 9223），打开后自动继续')
        failures = self.failures()
        if failures >= 4:
            return self.attention('no_progress', '连续 4 次启动都没有完成新帖子，已停止自动续采，请看 collection.log')
        at = (last['ended'] if last else 0) + (60 if recover_cdp else BACKOFF[min(failures, len(BACKOFF) - 1)])
        if now < at:
            return 'backoff', f'上次退出后等待，{clock(at)} 重启', at
        if recover_cdp:
            # Only this identified local body-retention failure is recoverable.
            # The new collector rechecks identity/login and resumes saved work;
            # neither API refusals nor other protocol failures are acknowledged.
            store = Store(self.batch)
            try:
                if (store.control('workspace_blocked') != blocked
                        or store.control('api_stop_evidence') != controls.get('api_stop_evidence')):
                    return 'backoff', '采集状态已变化，下次检查后继续', None
                store.save(None, None, 'local_adapter_repair', encode({
                    'change': 'restart_native_capture_with_durable_response_bodies',
                    'previous_error': proof, 'manual_login_confirmation_claimed': False}))
                store.control('workspace_blocked', '')
            finally:
                store.close()
            self.launch('local_cdp_body_recovery')
        elif blocked == 'api_access_restricted':
            # Do not clear the lock: the runner clears it only after the exact
            # pending page succeeds and is durably archived.
            self.launch('api_rate_limit_elapsed')
        else:
            if blocked:
                self.acknowledge(len(recent), until)
            self.launch('after_cooldown' if blocked else 'resume')
        return 'collecting', '已启动采集', None

    def periodic_report(self, now, report):
        """Report from the existing scheduler; no extra watcher or platform calls."""
        if self.report_interval <= 0 or now < self.state.get('next_progress_report_at', 0):
            return
        row = self.query("""SELECT count(*), sum(body='saved'),
            sum(body='saved' AND comments='complete_observed' AND media='success' AND comment_media='success')
            FROM tasks""")[0]
        counts = dict(zip(('total', 'bodies', 'complete'), (value or 0 for value in row)))
        previous = self.state.get('last_progress_report_counts', counts)
        delta = {key: counts[key] - previous.get(key, counts[key]) for key in counts}
        last = self.query("""SELECT max(seen) FROM events WHERE kind='network_json' AND valid=1
            AND json_extract(metadata, '$.business_success')=1""")[0][0]
        status = '采集中' if report['collector_pid'] else '已完成' if report['state'] == 'finished' else '已停止/等待'
        message = (f"{status}；正文 {counts['bodies']}/{counts['total']}（本期+{delta['bodies']}），"
                   f"全量完成 {counts['complete']}（本期+{delta['complete']}）。{report['message']}")
        entry = {'at': report['checked_at'], 'state': report['state'], 'message': message,
                 'counts': counts, 'delta': delta, 'collector_pid': report['collector_pid'],
                 'last_successful_response': last, 'next_report_at': clock(now + self.report_interval)}
        script = f'display notification {json.dumps(message, ensure_ascii=False)} with title "小红书采集：30分钟进度"'
        try:
            result = subprocess.run(['osascript', '-e', script], capture_output=True, timeout=5)
            entry['notification_exit_code'] = result.returncode
        except (OSError, subprocess.TimeoutExpired) as exc:
            entry['notification_error'] = type(exc).__name__
        with (self.batch / 'progress-reports.jsonl').open('a') as output:
            output.write(json.dumps(entry, ensure_ascii=False) + '\n')
        self.state.update(next_progress_report_at=now + self.report_interval,
                          last_progress_report_counts=counts)
        self.log(action='periodic_progress_report', **entry)

    def tick(self):
        self.children = [c for c in self.children if c.poll() is None]
        now = time.time()
        if self.reset.exists():
            self.state.update(reset_at=now)
            self.state.pop('notified', None)
            self.reset.unlink()
            self.log(action='reset_no_progress_streak')
        controls = self.controls()
        state, message, next_at = self.decide(now, controls)
        counts = dict(self.query("SELECT comments, count(*) FROM tasks GROUP BY comments"))
        report = {'checked_at': datetime.now().isoformat(timespec='seconds'), 'state': state, 'message': message,
                  'next_at': clock(next_at) if next_at else None, 'collector_pid': self.worker(),
                  'restrictions_48h': len([t for t in json.loads(controls.get('pacing_restriction_log') or '[]') if t > now - WINDOW]),
                  'pacing_factor': controls.get('pacing_factor'), 'comments': counts,
                  'last_log_line': tail(self.batch / 'collection.log'), 'supervisor': self.state}
        self.periodic_report(now, report)
        atomic_write(self.path, encode(report))
        if (state, message) != self.previous:
            self.previous = (state, message)
            self.log(state=state, message=message)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--batch', type=Path, required=True)
    parser.add_argument('--reset', action='store_true', help='clear the no-progress streak and exit')
    args = parser.parse_args()
    supervisor = Supervisor(args.batch.expanduser().resolve())
    if args.reset:
        supervisor.reset.touch()  # applied by the running supervisor on its next tick
        return
    while True:
        try:
            supervisor.tick()
        except Exception as exc:  # keep supervising; launchd restarts us if this loop dies
            supervisor.log(error=type(exc).__name__, detail=str(exc)[:300])
        time.sleep(TICK)


if __name__ == '__main__':
    main()
