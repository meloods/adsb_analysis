#!/usr/bin/env python3
"""Flatten trace JSON files into a single CSV per date."""

import argparse
import logging
from pathlib import Path

from utils import setup_logging, validate_date, load_config, get_data_dir

# --- Setup ---
setup_logging()
config = load_config()
DATA_DIR: Path = get_data_dir(config)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Flatten aircraft trace JSON files into CSV."
    )
    parser.add_argument(
        "--date",
        required=True,
        type=validate_date,
        help="Date to process, in YYYY.MM.DD format.",
    )
    parser.add_argument(
        "--metadata",
        default="false",
        choices=["true", "false"],
        help="Include flattened aircraft_metadata if true (default: false).",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    date_str = args.date
    include_metadata = args.metadata.lower() == "true"

    logging.info(f"📅 Processing date: {date_str}")
    logging.info(f"📦 Include metadata: {include_metadata}")

    input_dir = DATA_DIR / date_str / "json"
    output_csv = DATA_DIR / "processed " / f"{date_str.replace('.', '_')}.csv"

    if not input_dir.exists():
        logging.error(f"Input directory not found: {input_dir}")
        return

    # --- Placeholder for real logic ---
    from data_processing.processor import flatten_all_json_to_csv

    flatten_all_json_to_csv(input_dir, output_csv, include_metadata)


if __name__ == "__main__":
    main()
