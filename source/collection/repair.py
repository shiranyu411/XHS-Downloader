"""Offline repair of derived reports. Never opens Store or changes the database."""
from contextlib import contextmanager
from datetime import datetime
import os
from pathlib import Path
import sqlite3
from uuid import uuid4

from .observations import ObservationIndex
from .store import atomic_write, encode


@contextmanager
def report_lock(root):
    # Same lock as Store: even an idle open workbench owns its batch.
    with (root / '.lock').open('a+b') as lock:
        try:
            if os.name == 'nt':
                import msvcrt
                lock.seek(0, 2)
                if lock.tell() == 0:
                    lock.write(b'0')
                    lock.flush()
                lock.seek(0)
                msvcrt.locking(lock.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            raise RuntimeError('批次仍被使用：请关闭占用它的工作台/采集进程后再修正报告。') from None
        yield


def rebuild_report(root):
    root = Path(root).resolve()
    db = sqlite3.connect((root / 'state.sqlite').as_uri() + '?mode=ro', uri=True)
    db.row_factory = sqlite3.Row
    try:
        db.execute('BEGIN')  # One consistent read snapshot, including a live WAL.
        report = ObservationIndex.rebuild(db, root).report()
        report['through_event_id'] = db.execute('SELECT coalesce(max(id),0) FROM events').fetchone()[0]
        report['rebuilt_at'] = datetime.now().isoformat()
        return report
    finally:
        db.close()


def repair_report(root, apply=False, output=None):
    root = Path(root).resolve()
    if not (root / 'state.sqlite').is_file():
        raise ValueError('请选择有效批次目录')
    if output is not None:
        output = Path(output).resolve()
        if output == root or root in output.parents:
            raise ValueError('只读重建的输出请放到批次之外；覆盖正式报告需要 --apply')
    if apply:
        with report_lock(root):
            report = rebuild_report(root)
            target = root / 'passive_report.json'
            backup = None
            if target.exists():
                backup = root / 'report_backups' / (datetime.now().strftime('%Y%m%d-%H%M%S-') + uuid4().hex + '.json')
                atomic_write(backup, target.read_bytes())
            atomic_write(target, encode(report))
            result = {'applied': True, 'backup': str(backup) if backup else None}
    else:
        report = rebuild_report(root)
        result = {'applied': False, 'backup': None}
    if output is not None:
        atomic_write(output, encode(report))
    return {**result, 'comments_total': report['comments_total'], 'capture_gaps': report['capture_gaps'],
            'partial_threads': sum(len(v['threads_partially_seen']) for v in report['notes'].values()),
            'through_event_id': report['through_event_id'], 'output': str(output) if output else None}
