"""Reuse verified complete comment chains from sibling, local collection archives.

The destination must already be open as a Store (holding its exclusive writer
lock). Sources are read-only snapshots. Only untouched pending tasks are seeded;
no new tasks, URLs, access permissions or completeness claims are invented.
"""
from contextlib import closing
import gzip
import hashlib
import json
from pathlib import Path
from uuid import uuid4

from .replay import chain_key, rebuild
from .results import blob, open_snapshot, complete_post
from .store import atomic_write, encode, now


def copy_blob(store, body):
    digest = hashlib.sha256(body).hexdigest()
    relative = f'raw/{digest[:2]}/{digest}.gz'
    target = store.root / relative
    if target.exists():
        try:
            existing = gzip.decompress(target.read_bytes())
        except (OSError, ValueError, EOFError) as exc:
            raise OSError('existing_destination_blob_unreadable') from exc
        if hashlib.sha256(existing).hexdigest() != digest:
            raise OSError('existing_destination_blob_corrupt')
    else:
        atomic_write(target, gzip.compress(body, mtime=0))
    return relative, digest


def reuse_complete_comments(store, roots):
    """Seed archived bodies/comments, leaving media completion to the collector.

    This is idempotent and deliberately conservative. A note with any existing
    destination attempt is left alone. Blobs are durable before a single SQLite
    transaction commits each note's attempts, events, chain and task state; an
    interrupted copy can at worst leave unindexed, content-addressed blobs.
    An exact source counter contradiction is not a complete cache: leave that
    task pending for a fresh collection, even when the old cursors reached an end.
    """
    if store.db.in_transaction:
        raise RuntimeError('reuse_requires_a_clean_destination_transaction')
    result = {'imported': 0, 'existing_attempts_skipped': 0, 'unverified_skipped': 0,
              'count_mismatch_skipped': 0, 'sources': []}
    for value in roots:
        root = Path(value).expanduser().resolve()
        if root == store.root or root.parent != store.root.parent or not (root / 'state.sqlite').is_file():
            raise ValueError('reuse_source_must_be_a_different_sibling_archive')
        imported, contradicted = [], []
        with closing(open_snapshot(root)) as source:
            candidates = {r[0] for r in source.execute("SELECT note_id FROM tasks WHERE body='saved' AND comments='complete_observed'")}
            tasks = [dict(r) for r in store.db.execute("SELECT rowid AS queue_order,* FROM tasks WHERE status='pending' ORDER BY rowid")]
            for task in tasks:
                id_ = task['note_id']
                if id_ not in candidates:
                    continue
                if store.db.execute('SELECT 1 FROM attempts WHERE note_id=?', (id_,)).fetchone():
                    result['existing_attempts_skipped'] += 1
                    continue
                content = complete_post(source, root, id_)
                if content['comment_count_evidence']['status'] == 'mismatch':
                    result['count_mismatch_skipped'] += 1
                    result['unverified_skipped'] += 1
                    contradicted.append(task['queue_order'])
                    continue
                chain = content['active_chain']
                ledger, stats = rebuild(source, root, id_, chain)
                if not content['active_complete_verified'] or content['warnings'] or not ledger.complete() or stats['unreadable']:
                    result['unverified_skipped'] += 1
                    continue
                attempts = {r['id']: dict(r) for r in source.execute('SELECT * FROM attempts WHERE note_id=?', (id_,))}
                if not chain or any(a not in attempts for a in chain):
                    result['unverified_skipped'] += 1
                    continue
                selected = []
                note = None
                for row in source.execute('SELECT * FROM events WHERE note_id=? ORDER BY id', (id_,)):
                    if row['attempt'] not in chain:
                        continue
                    meta = json.loads(row['metadata'])
                    if row['kind'] == 'network_json':
                        if meta.get('kind') not in {'comments', 'replies'} or meta.get('note') != id_:
                            continue
                    elif row['kind'] != 'note_object':
                        continue
                    body = blob(root, row)
                    if row['kind'] == 'note_object':
                        note = json.loads(body)
                        if note.get('noteId') != id_:
                            raise ValueError('reused_body_ownership_mismatch')
                    selected.append((dict(row), meta, body))
                if note is None:
                    result['unverified_skipped'] += 1
                    continue

                mapping = {attempt: uuid4().hex for attempt in chain}
                copied = []
                for row, meta, body in selected:
                    path, digest = copy_blob(store, body)
                    meta = {**meta, 'archive_reuse': {'archive': root.name, 'event_id': row['id'], 'attempt': row['attempt']}}
                    copied.append((mapping[row['attempt']], id_, row['kind'], row['seen'], path, digest, encode(meta).decode(), 1))
                provenance = {'source_archive': root.name, 'source_attempts': chain,
                              'destination_attempts': [mapping[a] for a in chain],
                              'source_events': [r['id'] for r, _, _ in selected],
                              'source_cutoff_event_id': source.execute('SELECT max(id) FROM events').fetchone()[0],
                              'body_and_comments_verified': True, 'media_not_claimed_complete': True,
                              'comment_count_evidence': content['comment_count_evidence']}
                path, digest = copy_blob(store, encode(provenance))
                timestamp = now()
                with store.db:
                    for old in chain:
                        a = attempts[old]
                        store.db.execute('INSERT INTO attempts(id,note_id,started,ended,status,result) VALUES(?,?,?,?,?,?)',
                                         (mapping[old], id_, a['started'], a['ended'], a['status'], a['result']))
                    store.db.executemany('INSERT INTO events(attempt,note_id,kind,seen,path,sha256,metadata,valid) VALUES(?,?,?,?,?,?,?,?)', copied)
                    store.db.execute('INSERT INTO events(attempt,note_id,kind,seen,path,sha256,metadata,valid) VALUES(?,?,?,?,?,?,?,?)',
                                     (None, id_, 'archive_reuse', timestamp, path, digest, '{}', 1))
                    store.db.execute('INSERT OR REPLACE INTO controls VALUES(?,?)',
                                     (chain_key(id_), encode(provenance['destination_attempts']).decode()))
                    store.db.execute('INSERT OR REPLACE INTO controls VALUES(?,?)',
                                     ('archive_reuse:' + id_, encode(provenance).decode()))
                    # Keep the newest discovered URL, pending state and unfinished
                    # media flags. The existing Runner will only fetch missing media.
                    store.db.execute("UPDATE tasks SET body='saved',comments='complete_observed',title=?,reason='reused_verified_archive',updated=? WHERE note_id=?",
                                     (str(note.get('title') or ''), timestamp, id_))
                    store.db.execute('INSERT INTO sources(note_id,url,metadata,seen) VALUES(?,?,?,?)',
                                     (id_, task['url'], encode({'mode': 'verified_archive_reuse', 'archive': root.name}).decode(), timestamp))
                imported.append(task['queue_order'])
                result['imported'] += 1
        result['sources'].append({'archive': root.name, 'imported_queue_positions': imported,
                                  'count_mismatch_queue_positions': contradicted})
    result['total_seeded_in_destination'] = store.db.execute(
        "SELECT count(DISTINCT note_id) FROM events WHERE kind='archive_reuse'").fetchone()[0]
    return result
