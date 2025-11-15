import argparse
import time
import pandas as pd
from indexes.query_engine import QueryEngine


def snippet(text: str, length: int = 200) -> str:
    if not text:
        return ""
    s = str(text).strip()
    return (s[:length] + "...") if len(s) > length else s


def main():
    parser = argparse.ArgumentParser(description="Run a text query against built index and show snippets")
    parser.add_argument("--query", required=True, help="Query text")
    parser.add_argument("--index", default="indexes/text", help="Index directory")
    parser.add_argument("--dataset", default="data/news_text_dataset.csv", help="CSV dataset with id,title,text columns")
    parser.add_argument("--k", type=int, default=10, help="Top-k")
    args = parser.parse_args()

    # load dataset mapping id -> {title, text}
    df = pd.read_csv(args.dataset)
    docs = {}
    for _, row in df.iterrows():
        docs[str(row['id'])] = {"title": row.get('title', ''), "text": row.get('text', '')}

    qe = QueryEngine(index_dir=args.index)
    t0 = time.perf_counter()
    res = qe.query(args.query, k=args.k)
    elapsed = time.perf_counter() - t0

    results = res.get('results', [])
    print(f"Query: {args.query}")
    print(f"Top-{args.k} results (time index query: {res.get('time'):.4f}s, runner overhead: {elapsed - res.get('time'):.4f}s, bytes_read: {res.get('bytes_read')})")
    print("-")
    for doc_id, score in results:
        meta = docs.get(str(doc_id), {})
        title = meta.get('title', '')
        text = meta.get('text', '')
        print(f"id: {doc_id}\nscore: {score:.6f}\ntitle: {title}\nsnippet: {snippet(text, 240)}\n---")


if __name__ == '__main__':
    main()
