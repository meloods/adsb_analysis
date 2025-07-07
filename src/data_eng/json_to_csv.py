#!/usr/bin/env python3
"""
Convert flattened trace JSON files into a single CSV per date.
"""

import argparse
import csv
import json
import logging
from math import radians, cos, sin, asin, sqrt
from pathlib import Path
from typing import Any, Dict, List, Optional, TextIO
from tqdm import tqdm


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
TOP_LEVEL_KEYS: list[str] = [
    "icao",
    "timestamp",
    "r",
    "t",
    "desc",
    "dbFlags",
    "year",
    "ownOp",
]
SINGAPORE_COORDS = (1.3521, 103.8198)  # latitude, longitude
RADIUS_KM = 300


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

        metadata = {key: data.get(key) for key in TOP_LEVEL_KEYS}

        for entry in data.get("trace", []):
            row = flatten_trace_entry(entry, metadata, include_metadata)
            rows.append(row)

    except Exception as e:
        logging.warning(f"❌ Failed to process {file_path.name}: {e}")

    return rows


def haversine(lat1, lon1, lat2, lon2):
    # Earth radius in kilometers
    R = 6371.0
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * asin(sqrt(a))
    return R * c


def flatten_all_json_to_csv(
    date_str: str,
    base_download_dir: Path = DATA_DIR,
    include_metadata: bool = False,
) -> None:
    """
    Flatten all aircraft trace JSON files in a directory into a CSV.

    Args:
        date_str: Date in YYYY.MM.DD format (e.g., '2025.05.28').
        base_download_dir: Base directory to store downloads.
        include_metadata: Whether to flatten aircraft_metadata fields.
    """
    input_dir = DATA_DIR / date_str / "json"
    output_dir = DATA_DIR / date_str
    output_dir.mkdir(parents=True, exist_ok=True)
    output_csv = output_dir / f"{date_str.replace('.', '-')}_nearSG.csv"
    file_paths = list(input_dir.glob("trace_full_*.json"))
    total_files = len(file_paths)

    if total_files == 0:
        logging.warning(f"No trace files found in {input_dir}")
        return

    all_rows: List[Dict[str, Any]] = []
    all_fieldnames: set[str] = set()

    logging.info(f"📄 Flattening {total_files} trace files from {input_dir}")
    for file_path in tqdm(file_paths, desc="🔍 Filtering JSON files", unit="file"):
        rows = process_single_file(file_path, include_metadata)
        for row in rows:
            lat = row.get("latitude")
            lon = row.get("longitude")
            if (
                lat is not None
                and lon is not None
                and haversine(lat, lon, *SINGAPORE_COORDS) <= RADIUS_KM
            ):
                all_rows.append(row)
                all_fieldnames.update(row.keys())

    if not all_rows:
        logging.warning(f"No rows produced from {input_dir}")
        return

    with output_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=sorted(all_fieldnames))
        writer.writeheader()
        writer.writerows(all_rows)

    logging.info(f"✅ Wrote {len(all_rows):,} rows to {output_csv}")


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
        output_dir = DATA_DIR / date_str / "near_sg"
        output_dir.mkdir(parents=True, exist_ok=True)
        output_csv = output_dir / f"{date_str.replace('.', '_')}.csv"
        flatten_all_json_to_csv(input_dir, output_csv, args.include_metadata)

    logging.info("\n🎉 JSON-to-CSV conversion complete.")


if __name__ == "__main__":
    main()
