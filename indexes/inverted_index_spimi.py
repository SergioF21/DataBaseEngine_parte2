import os
import json
from collections import defaultdict, Counter
from typing import Iterable

from core.text_preprocessor import preprocess


class SPIMIIndexer:
    """A simple SPIMI-style indexer that creates blocks and writes them to disk.

    This is an initial implementation to be extended. Blocks are JSON files mapping
    term -> list of [doc_id, tf].
    """

    def __init__(self, output_dir: str = "indexes/text"):
        self.output_dir = output_dir
        self.blocks_dir = os.path.join(output_dir, "blocks")
        os.makedirs(self.blocks_dir, exist_ok=True)
        self.doc_stats = {}  # doc_id -> {'len': int, 'norm': float}

    def _write_block(self, block_terms: dict, block_id: int):
        path = os.path.join(self.blocks_dir, f"block_{block_id}.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(block_terms, f, ensure_ascii=False)
        return path

    def build_from_documents(self, docs: Iterable[tuple], block_doc_limit: int = 1000):
        """Build index blocks from an iterable of (doc_id, text) tuples.

        Args:
            docs: iterable of pairs (doc_id, text)
            block_doc_limit: how many documents to process per block
        """
        block_terms = defaultdict(list)  # term -> list of [doc_id, tf]
        block_count = 0
        docs_in_block = 0

        for doc_id, text in docs:
            tokens = preprocess(text)
            if not tokens:
                continue
            tf = Counter(tokens)
            # compute doc norm (for tf-idf later we need idf; for now store raw tf vector norm)
            norm = sum((v ** 2 for v in tf.values())) ** 0.5
            self.doc_stats[doc_id] = {"len": sum(tf.values()), "norm": norm}

            for term, freq in tf.items():
                block_terms[term].append([doc_id, freq])

            docs_in_block += 1
            if docs_in_block >= block_doc_limit:
                self._write_block(block_terms, block_count)
                block_count += 1
                block_terms = defaultdict(list)
                docs_in_block = 0

        # write remaining
        if docs_in_block > 0 or block_count == 0:
            self._write_block(block_terms, block_count)

        # write doc stats
        docs_path = os.path.join(self.output_dir, "doc_stats.json")
        with open(docs_path, "w", encoding="utf-8") as f:
            json.dump(self.doc_stats, f, ensure_ascii=False)

        return {
            "blocks_dir": self.blocks_dir,
            "blocks_written": block_count + (1 if docs_in_block > 0 else 0),
            "doc_stats_path": docs_path,
        }


def iter_csv_documents(csv_path: str, id_col: str = None, text_col: str = "text"):
    """Yield (doc_id, text) tuples from a CSV file.

    If id_col is None, use the dataframe index as id.
    """
    import pandas as pd

    df = pd.read_csv(csv_path)
    if id_col is None:
        for idx, row in df.iterrows():
            text = row.get(text_col, None) if text_col in df.columns else None
            if text is None or (isinstance(text, float) and pd.isna(text)):
                # fall back to concatenate all text-like columns
                text = " ".join([str(v) for v in row.values if isinstance(v, str)])
            yield str(idx), text
    else:
        for _, row in df.iterrows():
            doc_id = row.get(id_col)
            text = row.get(text_col, None) if text_col in df.columns else None
            if text is None or (isinstance(text, float) and pd.isna(text)):
                text = " ".join([str(v) for v in row.values if isinstance(v, str)])
            yield str(doc_id), text


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Build SPIMI blocks from CSV")
    parser.add_argument("csv", help="Input CSV path")
    parser.add_argument("--id-col", help="Column to use as doc id", default=None)
    parser.add_argument("--text-col", help="Column containing text", default="text")
    parser.add_argument("--block-size", type=int, default=1000)
    parser.add_argument("--out", default="indexes/text")
    args = parser.parse_args()

    indexer = SPIMIIndexer(output_dir=args.out)
    res = indexer.build_from_documents(iter_csv_documents(args.csv, id_col=args.id_col, text_col=args.text_col), block_doc_limit=args.block_size)
    print("Done:", res)
