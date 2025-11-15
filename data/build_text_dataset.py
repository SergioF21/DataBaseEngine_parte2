"""Utility to build a simple textual dataset CSV by concatenating textual fields.

Usage:
    python data/build_text_dataset.py --input data/productos.csv --output data/text_dataset.csv
"""
import os
import argparse
import pandas as pd


def concat_text_from_row(row, exclude_cols=None):
    parts = []
    exclude_cols = set(exclude_cols or [])
    for col, v in row.items():
        if col in exclude_cols:
            continue
        if v is None:
            continue
        s = str(v).strip()
        if s and s.lower() not in ("nan", "none"):
            parts.append(s)
    return " ".join(parts)


def build(input_csv: str, output_csv: str, id_col: str = None, title_col: str = None):
    df = pd.read_csv(input_csv)
    out_rows = []
    for idx, row in df.iterrows():
        doc_id = row[id_col] if id_col and id_col in df.columns else idx
        title = row[title_col] if (title_col and title_col in df.columns) else None
        text = concat_text_from_row(row, exclude_cols=[id_col, title_col])
        out_rows.append({"id": doc_id, "title": title, "text": text})

    out_df = pd.DataFrame(out_rows)
    out_df.to_csv(output_csv, index=False)
    print(f"Wrote {len(out_df)} documents to {output_csv}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build a textual dataset CSV")
    parser.add_argument("--input", required=True, help="Input CSV path")
    parser.add_argument("--output", required=True, help="Output CSV path")
    parser.add_argument("--id-col", default=None, help="Column to use as id (optional)")
    parser.add_argument("--title-col", default=None, help="Column to use as title (optional)")
    args = parser.parse_args()

    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    build(args.input, args.output, id_col=args.id_col, title_col=args.title_col)
