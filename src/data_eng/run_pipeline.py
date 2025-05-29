#!/usr/bin/env python3
"""
Orchestrate the data engineering pipeline:
download, extract, decompress, and convert JSON to CSV for aircraft trace data.
"""

import argparse
import logging
from pathlib import Path
from typing import Optional

from utils import setup_logging, validate_date, load_config, get_data_dir
from download import download_for_date
from extract import extract_for_date
from gzip_decompress import decompress_for_date
from json_to_csv import flatten_all_json_to_csv
from time_filtering import process_time_filtering

# --- Setup ---
setup_logging()
config = load_config()
DATA_DIR: Path = get_data_dir(config)


def run_pipeline_for_date(
    date_str: str, step: Optional[int], include_metadata: bool
) -> None:
    """
    Run one or more steps of the data pipeline for a given date.

    Args:
        date_str: Date in YYYY.MM.DD format.
        include_metadata: Whether to flatten aircraft_metadata into CSV output.
        step: Optional integer to run only a specific step (1–5).
    """
    logging.info(f"\n🚀 Starting pipeline for {date_str} (step={step or 'all'})")

    if step is None or step == 1:
        try:
            logging.info("📥 Step 1: Downloading files...")
            download_for_date(date_str, DATA_DIR)
        except Exception as e:
            logging.error(f"❌ Download failed for {date_str}: {e}")

    if step is None or step == 2:
        try:
            logging.info("📦 Step 2: Extracting archives...")
            extract_for_date(date_str, DATA_DIR)
        except Exception as e:
            logging.error(f"❌ Extraction failed for {date_str}: {e}")

    if step is None or step == 3:
        try:
            logging.info("🔧 Step 3: Decompressing .json files...")
            decompress_for_date(date_str, DATA_DIR)
        except Exception as e:
            logging.error(f"❌ Decompression failed for {date_str}: {e}")

    if step is None or step == 4:
        try:
            logging.info("📄 Step 4: Flattening JSON to CSV...")
            flatten_all_json_to_csv(date_str, DATA_DIR, include_metadata)
        except Exception as e:
            logging.error(f"❌ JSON-to-CSV conversion failed for {date_str}: {e}")

    if step is None or step == 5:
        try:
            logging.info("🕐 Step 5: Filtering for specific timeslots only..")
            process_time_filtering(date_str, DATA_DIR)
        except Exception as e:
            logging.error(f"❌ Time filtering failed for {date_str}: {e}")

    logging.info(f"✅ Finished pipeline for {date_str}")


def main() -> None:
    """Command-line entry point."""
    parser = argparse.ArgumentParser(
        description="Run selected steps of the data engineering pipeline for aircraft trace data.",
        epilog=(
            "Examples:\n"
            "  Run steps 1-5:\n"
            "    PYTHONPATH=src python src/data_eng/run_pipeline.py 2025.01.01\n\n"
            "  Run only step 2 (extract):\n"
            "    PYTHONPATH=src python src/data_eng/run_pipeline.py 2025.01.01 --step 2\n\n"
            "  Run only step 4 with metadata:\n"
            "    PYTHONPATH=src python src/data_eng/run_pipeline.py 2025.01.01 --step 4 --include-metadata"
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "dates",
        nargs="+",
        type=validate_date,
        help="One or more dates in YYYY.MM.DD format.",
    )
    parser.add_argument(
        "--step",
        type=int,
        choices=[1, 2, 3, 4, 5],
        help="Run only a specific step (1=download, 2=extract, 3=decompress, 4=json_to_csv, 5=time_filtering).",
    )
    parser.add_argument(
        "--include-metadata",
        action="store_true",
        help="Include flattened aircraft_metadata fields in output.",
    )
    args = parser.parse_args()

    for date_str in args.dates:
        run_pipeline_for_date(
            date_str,
            args.step,
            args.include_metadata,
        )

    logging.info("\n🎉 Pipeline completed for all requested dates.")


if __name__ == "__main__":
    main()
