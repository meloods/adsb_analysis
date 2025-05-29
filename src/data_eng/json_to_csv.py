#!/usr/bin/env python3
"""
Convert JSON trace files to a single CSV file per date.
"""

import argparse
import csv
import json
import logging
import multiprocessing as mp
from pathlib import Path
from typing import List, Dict, Any, Optional, Iterator, Tuple

from tqdm import tqdm

from utils import (
    setup_logging,
    validate_date,
    load_config,
    get_data_dir,
    get_processed_dir,
)

# --- Setup ---
setup_logging()
config = load_config()
DATA_DIR: Path = get_data_dir(config)
PROCESSED_DIR: Path = get_processed_dir(config)

# --- Constants ---
BATCH_SIZE = 2000
MAX_WORKERS = 12

# Core trace fields (indices 0-13 in trace array)
TRACE_FIELDS = [
    "seconds_offset",
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

# Top-level metadata fields
TOP_LEVEL_FIELDS = ["icao", "base_timestamp", "r", "t", "desc", "dbFlags"]


def flatten_metadata(metadata: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Flatten aircraft_metadata dictionary with 'meta_' prefix.

    Args:
        metadata: Aircraft metadata dictionary or None.

    Returns:
        Flattened dictionary with prefixed keys.
    """
    if not metadata:
        return {}

    flattened = {}
    for key, value in metadata.items():
        # Handle nested dictionaries by converting to string
        if isinstance(value, dict):
            flattened[f"meta_{key}"] = json.dumps(value)
        else:
            flattened[f"meta_{key}"] = value

    return flattened


def process_json_file(file_path: Path, include_metadata: bool) -> List[Dict[str, Any]]:
    """
    Process a single JSON file and extract all trace entries.

    Args:
        file_path: Path to the JSON file.
        include_metadata: Whether to include aircraft_metadata fields.

    Returns:
        List of dictionaries representing CSV rows.
    """
    try:
        with file_path.open("r") as f:
            data = json.load(f)

        rows = []
        icao = data.get("icao", "")
        base_timestamp = data.get("timestamp", 0.0)

        # Optional top-level fields
        registration = data.get("r", "")
        aircraft_type = data.get("t", "")
        description = data.get("desc", "")
        db_flags = data.get("dbFlags", 0)

        trace_data = data.get("trace", [])

        for trace_entry in trace_data:
            if len(trace_entry) < 14:
                # Skip malformed trace entries
                continue

            row = {
                "icao": icao,
                "base_timestamp": base_timestamp,
                "r": registration,
                "t": aircraft_type,
                "desc": description,
                "dbFlags": db_flags,
                "seconds_offset": trace_entry[0],
                "latitude": trace_entry[1],
                "longitude": trace_entry[2],
                "altitude_ft": trace_entry[3],
                "ground_speed_kts": trace_entry[4],
                "track_deg": trace_entry[5],
                "flags_bitfield": trace_entry[6],
                "vertical_rate_fpm": trace_entry[7],
                "source_type": trace_entry[9],
                "geometric_altitude_ft": trace_entry[10],
                "geometric_vertical_rate_fpm": trace_entry[11],
                "indicated_airspeed_kts": trace_entry[12],
                "roll_angle_deg": trace_entry[13],
            }

            # Handle aircraft_metadata (index 8)
            if include_metadata:
                metadata = trace_entry[8] if len(trace_entry) > 8 else None
                flattened_meta = flatten_metadata(metadata)
                row.update(flattened_meta)

            rows.append(row)

        return rows

    except Exception as e:
        logging.warning(f"Failed to process {file_path.name}: {e}")
        return []


def process_batch(args: Tuple[List[Path], bool]) -> List[Dict[str, Any]]:
    """
    Process a batch of JSON files.

    Args:
        args: Tuple of (file_paths, include_metadata).

    Returns:
        Combined list of all rows from the batch.
    """
    file_paths, include_metadata = args
    all_rows = []

    for file_path in file_paths:
        rows = process_json_file(file_path, include_metadata)
        all_rows.extend(rows)

    return all_rows


def discover_files(json_dir: Path) -> List[Path]:
    """
    Discover all JSON files in the directory.

    Args:
        json_dir: Directory containing JSON files.

    Returns:
        List of JSON file paths.
    """
    return list(json_dir.glob("*.json"))


def get_all_metadata_fields(
    sample_files: List[Path], max_samples: int = 100
) -> List[str]:
    """
    Discover all possible metadata fields by sampling files.

    Args:
        sample_files: List of JSON file paths to sample.
        max_samples: Maximum number of files to sample.

    Returns:
        Sorted list of unique metadata field names with 'meta_' prefix.
    """
    metadata_fields = set()
    sample_count = min(len(sample_files), max_samples)

    for file_path in sample_files[:sample_count]:
        try:
            with file_path.open("r") as f:
                data = json.load(f)

            trace_data = data.get("trace", [])
            for trace_entry in trace_data:
                if len(trace_entry) > 8 and trace_entry[8]:
                    metadata = trace_entry[8]
                    if isinstance(metadata, dict):
                        for key in metadata.keys():
                            metadata_fields.add(f"meta_{key}")
        except Exception:
            continue

    return sorted(metadata_fields)


def write_csv_header(
    csv_file: Path, include_metadata: bool, metadata_fields: List[str]
) -> None:
    """
    Write CSV header to file.

    Args:
        csv_file: Path to the CSV file.
        include_metadata: Whether metadata fields should be included.
        metadata_fields: List of metadata field names.
    """
    base_fields = TOP_LEVEL_FIELDS + [
        "seconds_offset",
        "latitude",
        "longitude",
        "altitude_ft",
        "ground_speed_kts",
        "track_deg",
        "flags_bitfield",
        "vertical_rate_fpm",
        "source_type",
        "geometric_altitude_ft",
        "geometric_vertical_rate_fpm",
        "indicated_airspeed_kts",
        "roll_angle_deg",
    ]

    if include_metadata:
        all_fields = base_fields + metadata_fields
    else:
        all_fields = base_fields

    with csv_file.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(all_fields)


def write_csv_batch(
    csv_file: Path,
    rows: List[Dict[str, Any]],
    include_metadata: bool,
    metadata_fields: List[str],
) -> None:
    """
    Append a batch of rows to the CSV file.

    Args:
        csv_file: Path to the CSV file.
        rows: List of row dictionaries to write.
        include_metadata: Whether metadata fields should be included.
        metadata_fields: List of metadata field names.
    """
    if not rows:
        return

    base_fields = TOP_LEVEL_FIELDS + [
        "seconds_offset",
        "latitude",
        "longitude",
        "altitude_ft",
        "ground_speed_kts",
        "track_deg",
        "flags_bitfield",
        "vertical_rate_fpm",
        "source_type",
        "geometric_altitude_ft",
        "geometric_vertical_rate_fpm",
        "indicated_airspeed_kts",
        "roll_angle_deg",
    ]

    if include_metadata:
        all_fields = base_fields + metadata_fields
    else:
        all_fields = base_fields

    with csv_file.open("a", newline="") as f:
        writer = csv.writer(f)
        for row in rows:
            # Ensure all fields are present with default values
            csv_row = []
            for field in all_fields:
                csv_row.append(row.get(field, ""))
            writer.writerow(csv_row)


def create_batches(files: List[Path], batch_size: int) -> Iterator[List[Path]]:
    """
    Create batches of files for processing.

    Args:
        files: List of file paths.
        batch_size: Size of each batch.

    Yields:
        Batches of file paths.
    """
    for i in range(0, len(files), batch_size):
        yield files[i : i + batch_size]


def convert_date_to_csv(
    date_str: str,
    include_metadata: bool,
    base_data_dir: Path = DATA_DIR,
    processed_dir: Path = PROCESSED_DIR,
) -> None:
    """
    Convert all JSON files for a given date to a single CSV file.

    Args:
        date_str: Date in YYYY.MM.DD format.
        include_metadata: Whether to include aircraft_metadata fields.
        base_data_dir: Base directory containing per-date JSON folders.
        processed_dir: Directory for processed CSV output.
    """
    date_path = base_data_dir / date_str
    json_dir = date_path / "json"

    if not json_dir.exists():
        logging.warning(f"JSON directory not found for {date_str}, skipping.")
        return

    # Discover files
    json_files = discover_files(json_dir)
    if not json_files:
        logging.warning(f"No JSON files found for {date_str}")
        return

    logging.info(f"Found {len(json_files):,} JSON files for {date_str}")

    # Create output directory
    processed_dir.mkdir(parents=True, exist_ok=True)

    # Generate output filename (YYYY.MM.DD -> yyyy_mm_dd.csv)
    output_filename = date_str.replace(".", "_") + ".csv"
    csv_file = processed_dir / output_filename

    # Discover metadata fields if needed
    metadata_fields = []
    if include_metadata:
        logging.info("Discovering metadata fields...")
        metadata_fields = get_all_metadata_fields(json_files)
        logging.info(f"Found {len(metadata_fields)} unique metadata fields")

    # Write CSV header
    write_csv_header(csv_file, include_metadata, metadata_fields)

    # Process files in batches using multiprocessing
    batches = list(create_batches(json_files, BATCH_SIZE))
    logging.info(f"Processing {len(batches)} batches with {MAX_WORKERS} workers")

    with mp.Pool(processes=MAX_WORKERS) as pool:
        batch_args = [(batch, include_metadata) for batch in batches]

        with tqdm(
            total=len(batches), desc=f"Processing {date_str}", unit="batch"
        ) as pbar:
            for batch_rows in pool.imap(process_batch, batch_args):
                write_csv_batch(csv_file, batch_rows, include_metadata, metadata_fields)
                pbar.update(1)

    logging.info(f"CSV file saved to: {csv_file}")


def main() -> None:
    """Command-line entry point."""
    parser = argparse.ArgumentParser(
        description="Convert JSON trace files to CSV format.",
        epilog=(
            "Example: PYTHONPATH=src python src/data_eng/json_to_csv.py 2025.02.08 --metadata true\n"
            "Output: data/processed/2025_02_08.csv"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "dates",
        nargs="+",
        type=validate_date,
        help="One or more dates in YYYY.MM.DD format.",
    )
    parser.add_argument(
        "--metadata",
        type=str,
        choices=["true", "false"],
        default="false",
        help="Include aircraft_metadata fields in CSV output (default: false).",
    )

    args = parser.parse_args()
    include_metadata = args.metadata.lower() == "true"

    if include_metadata:
        logging.info(
            "Metadata inclusion enabled - this may significantly increase file size and processing time"
        )

    for date_str in args.dates:
        logging.info(f"\nConverting JSON files for {date_str}...")
        convert_date_to_csv(date_str, include_metadata)

    logging.info("\nAll conversions complete.")


if __name__ == "__main__":
    main()
