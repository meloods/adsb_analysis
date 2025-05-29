#!/usr/bin/env python3
"""Orchestrate download, extract, and decompress for aircraft data."""

import argparse
import logging
from datetime import datetime
from pathlib import Path
from typing import List

from download import download_for_date
from extract import extract_for_date
from decompress import decompress_for_date


# --- Config ---
BASE_DIR = Path("data")


# --- Logging ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


def validate_date(date_str: str) -> str:
    """Validate date format (YYYY.MM.DD)."""
    try:
        datetime.strptime(date_str, "%Y.%m.%d")
        return date_str
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"Invalid date format: {date_str}. Use YYYY.MM.DD."
        )


def run_pipeline(dates: List[str]):
    for date_str in dates:
        logging.info(f"\nProcessing {date_str}")
        try:
            download_for_date(date_str, BASE_DIR)
            extract_for_date(date_str, BASE_DIR)
            decompress_for_date(date_str, BASE_DIR)
        except Exception as e:
            logging.error(f"Error processing {date_str}: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run full aircraft data pipeline: download + extract + decompress",
        epilog="Example: python main.py 2025.05.27 2025.05.18",
    )
    parser.add_argument(
        "dates",
        nargs="+",
        type=validate_date,
        help="One or more dates in YYYY.MM.DD format",
    )

    args = parser.parse_args()
    run_pipeline(args.dates)

    logging.info("\nAll tasks complete.")
