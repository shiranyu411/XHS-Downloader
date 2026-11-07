"""Rank a keyword batch's queue by search-card title (screening.classify_title, batch topic-profile.json).

Reads the batch's saved search responses, classifies each post's title with
all the keywords that found it, and writes task_priority: 0 core 保留,
1 core 待定, 2 ambiguous 保留, 3 ambiguous 待定, -1 剔除 (never opened).
New search pages are ranked by search.accept_page; run this once for posts
discovered before ranking existed. Needs the batch lock (collector stopped).
"""
import argparse
import collections
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from source.collection.replay import load_blob  # noqa: E402
from source.collection.screening import classify_title, priority, profile_for  # noqa: E402
from source.collection.store import Store  # noqa: E402


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('batch', type=Path)
    args = parser.parse_args()
    store = Store(args.batch)
    try:
        profile = profile_for(store.root)
        if profile is None:
            parser.error(f'{store.root} 下没有 topic-profile.json，无需排序')
        titles = {}
        for row in store.db.execute("SELECT * FROM events WHERE kind='network_json' AND valid=1 ORDER BY id").fetchall():
            meta = json.loads(row['metadata'])
            if not str(meta.get('path') or '').endswith('/search/notes') or meta.get('status') != 200:
                continue
            try:
                payload = json.loads(load_blob(store.root, row))
            except (OSError, ValueError, EOFError):
                continue
            data = payload.get('data') if isinstance(payload, dict) else None
            for item in (data or {}).get('items') or [] if isinstance(data, dict) else []:
                if isinstance(item, dict) and isinstance(item.get('id'), str):
                    titles[item['id'].lower()] = str((item.get('note_card') or {}).get('display_title') or '').strip()
        rows = []
        for (id_,) in store.db.execute('SELECT note_id FROM tasks ORDER BY rowid').fetchall():
            keywords = store.keywords(id_)
            verdict, why = classify_title(titles.get(id_, ''), keywords, profile)
            rows.append((id_, priority(verdict, keywords, profile), verdict, why))
        store.prioritize(rows)
        print(json.dumps({'tasks': len(rows), 'titles_found': sum(r[0] in titles for r in rows),
                          'verdicts': collections.Counter(r[2] for r in rows),
                          'priority': dict(sorted(collections.Counter(r[1] for r in rows).items()))}, ensure_ascii=False))
    finally:
        store.close()


if __name__ == '__main__':
    main()
