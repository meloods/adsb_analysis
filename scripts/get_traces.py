#!/usr/bin/env python3
"""Orchestrate download, extract, and decompress for aircraft data."""

import argparse
import logging
from typing import List

from config import BASE_DIR
from utils import setup_logging, validate_date
from download import download_for_date
from extract import extract_for_date
from decompress import decompress_for_date

setup_logging()


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
