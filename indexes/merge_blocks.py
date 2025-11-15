import os
import json
import glob
import math
from collections import defaultdict


def merge_blocks(blocks_dir: str, out_dir: str = "indexes/text"):
    """Merge SPIMI JSON blocks into a final inverted index stored as JSONL

    Produces:
      - inverted.jsonl : each line is a JSON with {term, df, postings: [[doc_id, weight], ...]}
      - vocab.json : mapping term -> {df, offset, length}
      - doc_norms.json : mapping doc_id -> euclidean norm of its tf-idf vector
    
    Notes: This implementation loads all blocks into memory as it's intended for
    moderate dataset sizes. For very large datasets, implement multi-way external
    merge.
    """
    os.makedirs(out_dir, exist_ok=True)
    block_files = sorted(glob.glob(os.path.join(blocks_dir, "block_*.json")))
    if not block_files:
        raise FileNotFoundError(f"No block files found in {blocks_dir}")

    # aggregate term -> doc->tf
    term_doc_tf = defaultdict(lambda: defaultdict(int))
    doc_ids = set()

    for bf in block_files:
        with open(bf, "r", encoding="utf-8") as f:
            block = json.load(f)
        for term, postings in block.items():
            for doc_id, tf in postings:
                term_doc_tf[term][str(doc_id)] += int(tf)
                doc_ids.add(str(doc_id))

    N = len(doc_ids)
    if N == 0:
        raise RuntimeError("No documents found while merging blocks")

    inverted_path = os.path.join(out_dir, "inverted.jsonl")
    vocab = {}
    doc_sq_sums = defaultdict(float)

    with open(inverted_path, "w", encoding="utf-8") as invf:
        for term in sorted(term_doc_tf.keys()):
            docs = term_doc_tf[term]
            df = len(docs)
            idf = math.log((N / df), 10) if df > 0 else 0.0
            postings = []
            for doc_id, tf in docs.items():
                tf = int(tf)
                tf_weight = 1.0 + math.log(tf, 10) if tf > 0 else 0.0
                weight = tf_weight * idf
                postings.append([doc_id, weight])
                doc_sq_sums[doc_id] += weight * weight

            record = {"term": term, "df": df, "postings": postings}
            offset = invf.tell()
            line = json.dumps(record, ensure_ascii=False) + "\n"
            invf.write(line)
            length = len(line.encode("utf-8"))
            vocab[term] = {"df": df, "offset": offset, "length": length}

    # compute doc norms
    doc_norms = {doc: math.sqrt(sq) for doc, sq in doc_sq_sums.items()}
    with open(os.path.join(out_dir, "vocab.json"), "w", encoding="utf-8") as vf:
        json.dump(vocab, vf, ensure_ascii=False)
    with open(os.path.join(out_dir, "doc_norms.json"), "w", encoding="utf-8") as df:
        json.dump(doc_norms, df, ensure_ascii=False)

    return {"inverted": inverted_path, "vocab": os.path.join(out_dir, "vocab.json"), "doc_norms": os.path.join(out_dir, "doc_norms.json"), "N": N}


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Merge SPIMI blocks into final inverted index")
    parser.add_argument("blocks_dir", help="Directory containing block_*.json files")
    parser.add_argument("--out", default="indexes/text", help="Output directory for merged index")
    args = parser.parse_args()

    res = merge_blocks(args.blocks_dir, out_dir=args.out)
    print("Merge completed:", res)
