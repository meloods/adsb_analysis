#!/usr/bin/env python3
"""
Orchestrate the data engineering pipeline: download, extract, and decompress aircraft trace data.
"""

import argparse
import logging
from pathlib import Path
from typing import List

from utils import setup_logging, validate_date, load_config, get_data_dir
from download import download_for_date
from extract import extract_for_date
from gzip_decompress import decompress_for_date

# --- Setup ---
setup_logging()
config = load_config()
DATA_DIR: Path = get_data_dir(config)


def run_pipeline_for_date(date_str: str) -> None:
    """
    Run the full data pipeline (download, extract, decompress) for a given date.

    Args:
        date_str: Date in YYYY.MM.DD format.
    """
    logging.info(f"\n🚀 Starting data engineering pipeline for {date_str}")

    try:
        logging.info("📥 Step 1: Downloading files...")
        download_for_date(date_str, DATA_DIR)
    except Exception as e:
        logging.error(f"❌ Download failed for {date_str}: {e}")

    try:
        logging.info("📦 Step 2: Extracting archives...")
        extract_for_date(date_str, DATA_DIR)
    except Exception as e:
        logging.error(f"❌ Extraction failed for {date_str}: {e}")

    try:
        logging.info("🔧 Step 3: Decompressing .json files...")
        decompress_for_date(date_str, DATA_DIR)
    except Exception as e:
        logging.error(f"❌ Decompression failed for {date_str}: {e}")

    logging.info(f"✅ Finished data engineering pipeline for {date_str}")


def main() -> None:
    """Command-line entry point."""
    parser = argparse.ArgumentParser(
        description="Run the full data engineering pipeline: download, extract, decompress.",
        epilog="Example: PYTHONPATH=src python src/data_eng/orchestrate_pipeline.py 2025.01.01 2025.01.02",
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
        run_pipeline_for_date(date_str)

    logging.info("\n🎉 All data engineering pipeline steps complete.")


if __name__ == "__main__":
    main()
