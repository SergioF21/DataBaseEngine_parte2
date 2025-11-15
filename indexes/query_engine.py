import os
import json
import math
import heapq
import time
from typing import List, Tuple

from core.text_preprocessor import preprocess


class QueryEngine:
    def __init__(self, index_dir: str = "indexes/text"):
        self.index_dir = index_dir
        self.inverted_path = os.path.join(index_dir, "inverted.jsonl")
        self.vocab_path = os.path.join(index_dir, "vocab.json")
        self.doc_norms_path = os.path.join(index_dir, "doc_norms.json")

        if not os.path.exists(self.inverted_path) or not os.path.exists(self.vocab_path):
            raise FileNotFoundError("Index files not found. Run merge_blocks first.")

        with open(self.vocab_path, "r", encoding="utf-8") as f:
            self.vocab = json.load(f)
        with open(self.doc_norms_path, "r", encoding="utf-8") as f:
            self.doc_norms = json.load(f)

    def _read_postings(self, term: str):
        """Read postings list for a term from inverted.jsonl using vocab offsets."""
        meta = self.vocab.get(term)
        if not meta:
            return [], 0
        offset = meta["offset"]
        length = meta["length"]
        with open(self.inverted_path, "rb") as f:
            f.seek(offset)
            data = f.read(length)
        try:
            record = json.loads(data.decode("utf-8"))
        except Exception:
            return [], length
        return record.get("postings", []), length

    def query(self, qtext: str, k: int = 10):
        start = time.perf_counter()
        tokens = preprocess(qtext)
        if not tokens:
            return {"results": [], "time": 0.0, "bytes_read": 0}
        # compute query tf
        q_tf = {}
        for t in tokens:
            q_tf[t] = q_tf.get(t, 0) + 1

        # compute q weights and accumulate scores
        accum = {}
        q_sq_sum = 0.0
        bytes_read = 0

        for term, tf in q_tf.items():
            meta = self.vocab.get(term)
            if not meta:
                continue
            df = meta["df"]
            idf = math.log((len(self.doc_norms) / df), 10) if df > 0 else 0.0
            q_tf_weight = 1.0 + math.log(tf, 10) if tf > 0 else 0.0
            q_w = q_tf_weight * idf
            q_sq_sum += q_w * q_w

            postings, br = self._read_postings(term)
            bytes_read += br
            for doc_id, doc_w in postings:
                # doc_w is the tf-idf weight stored during merge
                accum[doc_id] = accum.get(doc_id, 0.0) + q_w * float(doc_w)

        q_norm = math.sqrt(q_sq_sum) if q_sq_sum > 0 else 1.0

        # compute final cosine scores
        heap = []
        for doc_id, dot in accum.items():
            doc_norm = float(self.doc_norms.get(doc_id, 0.0))
            if doc_norm == 0.0:
                continue
            score = dot / (q_norm * doc_norm)
            if len(heap) < k:
                heapq.heappush(heap, (score, doc_id))
            else:
                if score > heap[0][0]:
                    heapq.heapreplace(heap, (score, doc_id))

        # return sorted descending
        results = sorted([(doc_id, score) for score, doc_id in heap], key=lambda x: x[1], reverse=True)
        # convert to list of tuples (doc_id, score)
        final = [(doc, float(score)) for doc, score in [(doc_id, sc) for (doc_id, sc) in results]]
        # sort by score desc
        final = sorted(final, key=lambda x: x[1], reverse=True)
        elapsed = time.perf_counter() - start
        return {"results": final, "time": elapsed, "bytes_read": bytes_read}


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run a text query against the inverted index")
    parser.add_argument("query", help="Query text")
    parser.add_argument("--index", default="indexes/text", help="Index directory")
    parser.add_argument("--k", type=int, default=10, help="Top-k")
    args = parser.parse_args()

    qe = QueryEngine(index_dir=args.index)
    res = qe.query(args.query, k=args.k)
    print("Top-k results:")
    for doc_id, score in res:
        print(doc_id, score)
