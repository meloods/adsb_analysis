#!/usr/bin/env python3
"""
Decompress gzip-compressed .json files from extracted/traces/ into a flat json/ directory.
"""

import argparse
import gzip
import logging
from pathlib import Path
from typing import Iterator

from tqdm import tqdm

from utils import setup_logging, validate_date, load_config, get_data_dir

# --- Setup ---
setup_logging()
config = load_config()
DATA_DIR: Path = get_data_dir(config)


def iter_trace_files(traces_dir: Path) -> Iterator[Path]:
    """
    Recursively yield all .json files from 256 subfolders under traces_dir.

    Args:
        traces_dir: Path to the 'extracted/traces/' directory.

    Yields:
        Path objects pointing to GZIP-compressed JSON files.
    """
    for subdir in sorted(traces_dir.iterdir()):
        if subdir.is_dir() and len(subdir.name) == 2:
            for json_file in subdir.glob("*.json"):
                yield json_file


def decompress_file(src_path: Path, dest_path: Path) -> None:
    """
    Decompress a GZIP-compressed JSON file to the specified destination path.

    Args:
        src_path: Source .json file (GZIP-compressed).
        dest_path: Target path for decompressed JSON.
    """
    try:
        with gzip.open(src_path, "rb") as f_in:
            with dest_path.open("wb") as f_out:
                f_out.write(f_in.read())
    except Exception as e:
        logging.error(f"Failed to decompress {src_path.name}: {e}")


def decompress_for_date(date_str: str, base_data_dir: Path = DATA_DIR) -> None:
    """
    Decompress all gzip-compressed .json files for the specified date.

    Args:
        date_str: Date string in YYYY.MM.DD format.
        base_data_dir: Base directory containing per-date subfolders.
    """
    base_path = base_data_dir / date_str
    traces_dir = base_path / "extracted" / "traces"
    output_dir = base_path / "json"
    output_dir.mkdir(parents=True, exist_ok=True)

    if not traces_dir.exists():
        logging.warning(f"No traces directory found for {date_str}, skipping.")
        return

    all_files = list(iter_trace_files(traces_dir))
    if not all_files:
        logging.warning(f"No .json files found under {traces_dir}")
        return

    for json_file in tqdm(all_files, desc=f"Decompressing {date_str}", unit="file"):
        output_path = output_dir / json_file.name
        decompress_file(json_file, output_path)


def main() -> None:
    """Command-line entry point."""
    parser = argparse.ArgumentParser(
        description="Decompress GZIP-compressed .json files from extracted traces.",
        epilog="Example: PYTHONPATH=src python src/data_eng/gzip_decompress.py 2025.01.01 2025.01.02",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "dates",
        nargs="+",
        type=validate_date,
        help="One or more dates in YYYY.MM.DD format.",
    )
    args = parser.parse_args()

    for date_str in args.dates:
        logging.info(f"\nDecompressing traces for {date_str}...")
        decompress_for_date(date_str)

    logging.info("\nAll decompression complete.")


if __name__ == "__main__":
    main()
