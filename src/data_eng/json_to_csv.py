#!/usr/bin/env python3
"""
Convert flattened trace JSON files into a single CSV per date.
"""

import argparse
import csv
import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, TextIO

from utils import setup_logging, load_config, get_data_dir, validate_date

# --- Setup ---
setup_logging()
config = load_config()
DATA_DIR: Path = get_data_dir(config)

# --- Constants ---
TRACE_COLUMNS: list[str] = [
    "seconds_after_timestamp",
    "latitude",
    "longitude",
    "altitude_ft",
    "ground_speed_kts",
    "track_deg",
    "flags_bitfield",
    "vertical_rate_fpm",
    "aircraft_metadata",
    "source_type",
    "geometric_altitude_ft",
    "geometric_vertical_rate_fpm",
    "indicated_airspeed_kts",
    "roll_angle_deg",
]


def flatten_trace_entry(
    trace_entry: List[Any],
    metadata: Dict[str, Any],
    include_metadata: bool,
) -> Dict[str, Any]:
    """
    Flatten one trace entry and attach aircraft-level metadata.

    Args:
        trace_entry: List of values for a single trace vector.
        metadata: Aircraft-level metadata from the top-level JSON.
        include_metadata: Whether to flatten aircraft_metadata details.

    Returns:
        A flattened dictionary row.
    """
    row: Dict[str, Any] = {}

    for idx, column in enumerate(TRACE_COLUMNS):
        if column == "aircraft_metadata" and not include_metadata:
            continue
        row[column] = trace_entry[idx] if idx < len(trace_entry) else None

    # Flatten aircraft_metadata if applicable
    if include_metadata:
        meta_dict: Optional[Dict[str, Any]] = (
            trace_entry[8] if len(trace_entry) > 8 else None
        )
        if isinstance(meta_dict, dict):
            for key, value in meta_dict.items():
                row[f"metadata.{key}"] = value
        row.pop("aircraft_metadata", None)

    # Attach file-level metadata
    for meta_key, meta_val in metadata.items():
        row[meta_key] = meta_val

    return row


def process_single_file(
    file_path: Path, include_metadata: bool
) -> List[Dict[str, Any]]:
    """
    Process a single trace JSON file.

    Args:
        file_path: Path to the trace JSON file.
        include_metadata: Whether to expand aircraft_metadata.

    Returns:
        A list of flattened row dictionaries.
    """
    rows: List[Dict[str, Any]] = []

    try:
        with file_path.open("r", encoding="utf-8") as f:
            data = json.load(f)

        metadata = {
            "icao": data.get("icao"),
            "timestamp": data.get("timestamp"),
            "r": data.get("r"),
            "t": data.get("t"),
            "desc": data.get("desc"),
            "dbFlags": data.get("dbFlags"),
            "year": data.get("year"),
            "ownOp": data.get("ownOp"),
        }

        for entry in data.get("trace", []):
            row = flatten_trace_entry(entry, metadata, include_metadata)
            rows.append(row)

    except Exception as e:
        logging.warning(f"❌ Failed to process {file_path.name}: {e}")

    return rows


class CSVRowWriter:
    """
    Manage writing rows to a CSV file, lazily initializing headers.
    """

    def __init__(self, output_path: Path):
        self.output_path = output_path
        self.file: Optional[TextIO] = None
        self.writer: Optional[csv.DictWriter] = None
        self.fieldnames: Optional[list[str]] = None
        self.rows_written = 0

    def write_row(self, row: Dict[str, Any]) -> None:
        if self.writer is None:
            self.fieldnames = list(row.keys())
            self.file = self.output_path.open("w", newline="", encoding="utf-8")
            self.writer = csv.DictWriter(self.file, fieldnames=self.fieldnames)
            self.writer.writeheader()

        self.writer.writerow(row)
        self.rows_written += 1

    def finalize(self) -> None:
        if self.file:
            self.file.close()


def flatten_all_json_to_csv(
    input_dir: Path,
    output_csv: Path,
    include_metadata: bool,
) -> None:
    """
    Flatten all aircraft trace JSON files in a directory into a CSV.

    Args:
        input_dir: Path to directory containing JSON trace files.
        output_csv: Path to final CSV output file.
        include_metadata: Whether to flatten aircraft_metadata fields.
    """
    file_paths = list(input_dir.glob("trace_full_*.json"))
    total_files = len(file_paths)

    if total_files == 0:
        logging.warning(f"No trace files found in {input_dir}")
        return

    writer = CSVRowWriter(output_csv)
    num_rows = 0

    logging.info(f"📄 Flattening {total_files} trace files from {input_dir}")
    for file_path in file_paths:
        rows = process_single_file(file_path, include_metadata)
        for row in rows:
            writer.write_row(row)
        num_rows += len(rows)

    writer.finalize()
    logging.info(f"✅ Wrote {num_rows:,} rows to {output_csv}")


def main() -> None:
    """Command-line entry point."""
    parser = argparse.ArgumentParser(
        description="Flatten aircraft trace JSON files into a single CSV.",
        epilog="Example: PYTHONPATH=src python src/data_eng/json_to_csv.py 2025.01.01 --include-metadata",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "dates",
        nargs="+",
        type=validate_date,
        help="One or more dates in YYYY.MM.DD format.",
    )
    parser.add_argument(
        "--include-metadata",
        action="store_true",
        help="Include flattened aircraft_metadata fields in output.",
    )
    args = parser.parse_args()

    for date_str in args.dates:
        logging.info(f"\n🧩 Converting JSON to CSV for {date_str}...")
        input_dir = DATA_DIR / date_str / "json"
        output_csv = DATA_DIR / date_str / f"{date_str}_flattened.csv"
        flatten_all_json_to_csv(input_dir, output_csv, args.include_metadata)

    logging.info("\n🎉 JSON-to-CSV conversion complete.")


if __name__ == "__main__":
    main()
