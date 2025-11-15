"""Helper to download datasets from Kaggle using the Kaggle API.

Requires the `kaggle` package and that the user has configured their
KAGGLE_USERNAME and KAGGLE_KEY or placed `kaggle.json` in `~/.kaggle/kaggle.json`.

Usage:
  python data/fetch_kaggle.py <dataset-slug> --out data/raw

Example dataset-slug: 'zynicide/wine-reviews'
"""
import os
import argparse

def download_dataset(slug: str, out_dir: str = "data/raw"):
    try:
        from kaggle.api.kaggle_api_extended import KaggleApi
    except Exception as e:
        raise RuntimeError("kaggle package not installed. Install with `pip install kaggle` and configure credentials.")

    api = KaggleApi()
    api.authenticate()
    os.makedirs(out_dir, exist_ok=True)
    api.dataset_download_files(slug, path=out_dir, unzip=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('slug', help='Kaggle dataset slug (owner/dataset)')
    parser.add_argument('--out', default='data/raw', help='Output directory')
    args = parser.parse_args()

    print(f"Downloading {args.slug} to {args.out} ...")
    try:
        download_dataset(args.slug, args.out)
        print("Done.")
    except Exception as e:
        print("Error:", e)
        print("If you cannot use the Kaggle API, download manually and place files under the output directory.")
