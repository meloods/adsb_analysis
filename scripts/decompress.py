#!/usr/bin/env python3
"""Decompress all gzipped JSON files from subfolders in extracted/traces."""

import argparse
import gzip
import logging
from pathlib import Path
from typing import Generator
from tqdm import tqdm

from config import BASE_DIR
from utils import setup_logging, validate_date

setup_logging()


def iter_gz_json_files(traces_root: Path) -> Generator[Path, None, None]:
    """Yield all .json files inside traces/** subfolders."""
    for path in traces_root.rglob("*.json"):
        if path.is_file():
            yield path


def decompress_file(gz_path: Path, out_dir: Path):
    """Decompress gzipped .json file into flat output directory."""
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / gz_path.name

    try:
        with gzip.open(gz_path, "rb") as f_in, open(out_path, "wb") as f_out:
            f_out.write(f_in.read())
        # logging.info(f"Decompressed: {gz_path.name}")
    except Exception as e:
        logging.error(f"Failed to decompress {gz_path}: {e}")


def decompress_for_date(date_str: str, base_dir: Path = BASE_DIR):
    date_path = base_dir / date_str
    in_dir = date_path / "extracted" / "traces"
    out_dir = date_path / "traces"

    if not in_dir.exists():
        logging.warning(f"No input directory: {in_dir}")
        return

    count = 0
    files = list(iter_gz_json_files(in_dir))
    for gz_file in tqdm(files, desc=f"Decompressing {date_str}", unit="file"):
        decompress_file(gz_file, out_dir)
        count += 1

    if count == 0:
        logging.warning(f"No .json files found in {in_dir}")
    else:
        logging.info(f"Decompressed {count} files into {out_dir}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Decompress gunzipped JSON files into flat directory.",
        epilog="Example: python decompress.py 2025.05.27 2025.05.18",
    )
    parser.add_argument(
        "dates",
        nargs="+",
        type=validate_date,
        help="One or more dates in YYYY.MM.DD format",
    )

    args = parser.parse_args()

    for date_str in args.dates:
        logging.info(f"\nDecompressing traces for {date_str}...")
        decompress_for_date(date_str)

    logging.info("\nDecompression complete.")
