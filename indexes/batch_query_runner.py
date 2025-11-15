"""Run a batch of queries against the index and save results to CSV.

Produces: reports/queries_results.csv with columns:
  query, k, rank, doc_id, score, index_time, runner_time, bytes_read, title, snippet

Usage:
  python indexes/batch_query_runner.py --queries-file queries.txt --k 8 --out reports/queries_results.csv
If no queries file is provided, a default set is used.
"""
import argparse
import time
import os
import csv
import pandas as pd

from indexes.query_engine import QueryEngine


def snippet(text: str, length: int = 240) -> str:
    if not text:
        return ""
    s = str(text).strip()
    return (s[:length] + "...") if len(s) > length else s


def load_dataset_map(path: str):
    df = pd.read_csv(path)
    docs = {}
    for _, row in df.iterrows():
        docs[str(row['id'])] = {"title": row.get('title', ''), "text": row.get('text', '')}
    return docs


def run_batch(queries, index_dir, dataset_csv, k, out_csv):
    os.makedirs(os.path.dirname(out_csv), exist_ok=True)
    docs = load_dataset_map(dataset_csv)
    qe = QueryEngine(index_dir=index_dir)

    rows = []
    for q in queries:
        t0 = time.perf_counter()
        res = qe.query(q, k=k)
        t1 = time.perf_counter()
        results = res.get('results', [])
        index_time = res.get('time', 0.0)
        bytes_read = res.get('bytes_read', 0)
        runner_time = t1 - t0

        for rank, (doc_id, score) in enumerate(results, start=1):
            meta = docs.get(str(doc_id), {})
            rows.append({
                'query': q,
                'k': k,
                'rank': rank,
                'doc_id': doc_id,
                'score': score,
                'index_time': index_time,
                'runner_time': runner_time,
                'bytes_read': bytes_read,
                'title': meta.get('title', ''),
                'snippet': snippet(meta.get('text', ''), 240),
            })

    # write CSV
    df_out = pd.DataFrame(rows)
    df_out.to_csv(out_csv, index=False)
    return out_csv


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run batch queries and export results to CSV')
    parser.add_argument('--queries-file', help='Text file with one query per line', default=None)
    parser.add_argument('--index', default='indexes/text', help='Index directory')
    parser.add_argument('--dataset', default='data/news_text_dataset.csv', help='Dataset CSV with id,title,text')
    parser.add_argument('--k', type=int, default=8, help='Top-k to retrieve')
    parser.add_argument('--out', default='reports/queries_results.csv', help='Output CSV path')
    args = parser.parse_args()

    if args.queries_file and os.path.exists(args.queries_file):
        with open(args.queries_file, 'r', encoding='utf-8') as f:
            queries = [l.strip() for l in f.readlines() if l.strip()]
    else:
        queries = [
            'inflación', 'sostenibilidad', 'China', 'aviación', 'finanzas', 'cambio climático',
            'economía', 'empleo', 'tecnología', 'educación'
        ]

    out = run_batch(queries, args.index, args.dataset, args.k, args.out)
    print('Wrote results to', out)
