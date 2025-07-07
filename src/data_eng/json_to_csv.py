#!/usr/bin/env python3
"""
Convert flattened trace JSON files into raw CSV format.
This module handles ONLY the JSON to CSV conversion without any processing.
"""

import argparse
import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Iterator, Union
import pandas as pd
from tqdm import tqdm

# Assuming these utilities exist - replace with your actual imports
from src.utils import setup_logging, load_config, get_data_dir, validate_date

# --- Setup ---
setup_logging()
logger = logging.getLogger(__name__)
config = load_config()
DEFAULT_DATA_DIR: Path = get_data_dir(config)

# --- Constants ---
# Trace array indices according to documentation
TRACE_COLUMNS: List[str] = [
    "seconds_after_timestamp",  # 0
    "latitude",  # 1
    "longitude",  # 2
    "altitude_ft",  # 3
    "ground_speed_kts",  # 4
    "track_deg",  # 5
    "flags_bitfield",  # 6
    "vertical_rate_fpm",  # 7
    "aircraft_metadata",  # 8
    "source_type",  # 9
    "geometric_altitude_ft",  # 10
    "geometric_vertical_rate_fpm",  # 11
    "indicated_airspeed_kts",  # 12
    "roll_angle_deg",  # 13
]

# Top-level keys from JSON format spec
TOP_LEVEL_KEYS: List[str] = ["icao", "r", "t", "desc", "dbFlags", "timestamp"]

# Chunk size for memory-efficient processing
CHUNK_SIZE: int = 10000


# --- Utility Functions ---


def normalize_altitude(alt: Any) -> Union[int, str, None]:
    """Normalize altitude field handling 'ground', int, or None."""
    if alt is None:
        return None
    if isinstance(alt, str) and alt.lower() == "ground":
        return "ground"
    try:
        return int(alt)
    except (ValueError, TypeError):
        return None


def validate_coordinates(lat: Any, lon: Any) -> bool:
    """Basic coordinate validation."""
    try:
        lat_f, lon_f = float(lat), float(lon)
        return -90 <= lat_f <= 90 and -180 <= lon_f <= 180
    except (ValueError, TypeError):
        return False


def flatten_trace_entry(
    trace_entry: List[Any],
    aircraft_metadata: Dict[str, Any],
    include_metadata: bool = False,
) -> Dict[str, Any]:
    """Flatten one trace entry with aircraft-level metadata."""
    row: Dict[str, Any] = {}

    # Ensure trace entry has expected length
    if len(trace_entry) != len(TRACE_COLUMNS):
        logger.debug(
            f"Trace entry has {len(trace_entry)} elements, expected {len(TRACE_COLUMNS)}"
        )

    # Map trace entry values to columns
    for idx, column in enumerate(TRACE_COLUMNS):
        if column == "aircraft_metadata":
            continue  # Handle separately

        value = trace_entry[idx] if idx < len(trace_entry) else None

        # Minimal data cleaning
        if column == "altitude_ft":
            value = normalize_altitude(value)
        elif column in ["latitude", "longitude"]:
            # Keep original values, let processing step handle validation
            pass

        row[column] = value

    # Add aircraft-level metadata
    row.update(aircraft_metadata)

    # Handle nested aircraft_metadata if present and requested
    if include_metadata and len(trace_entry) > 8:
        nested_metadata = trace_entry[8]
        if isinstance(nested_metadata, dict):
            for key, value in nested_metadata.items():
                row[f"metadata_{key}"] = value

    return row


def process_single_file(
    file_path: Path,
    include_metadata: bool = False,
) -> Iterator[Dict[str, Any]]:
    """Process a single trace JSON file yielding rows."""
    try:
        with file_path.open("r", encoding="utf-8") as f:
            data = json.load(f)

        # Validate required fields
        if "icao" not in data or "trace" not in data:
            logger.warning(f"Missing required fields in {file_path.name}")
            return

        # Extract aircraft-level metadata
        aircraft_metadata = {key: data.get(key) for key in TOP_LEVEL_KEYS}
        trace_data = data.get("trace", [])

        # Process each trace entry
        for entry in trace_data:
            if not isinstance(entry, list):
                logger.warning(f"Invalid trace entry format in {file_path.name}")
                continue

            row = flatten_trace_entry(entry, aircraft_metadata, include_metadata)
            yield row

    except json.JSONDecodeError as e:
        logger.warning(f"❌ Invalid JSON in {file_path.name}: {e}")
    except (OSError, IOError) as e:
        logger.warning(f"❌ File I/O error in {file_path.name}: {e}")
    except Exception as e:
        logger.exception(f"❌ Unexpected error in {file_path.name}: {e}")


def create_datetime_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Create datetime columns from timestamp and offset."""
    if "timestamp" not in df.columns or "seconds_after_timestamp" not in df.columns:
        logger.warning("Missing timestamp columns for datetime creation")
        return df

    try:
        absolute_timestamp = df["timestamp"] + df["seconds_after_timestamp"].fillna(0)
        df["datetime_utc"] = pd.to_datetime(absolute_timestamp, unit="s", utc=True)
    except Exception as e:
        logger.warning(f"Error creating datetime columns: {e}")

    return df


def reorder_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Reorder columns for better readability."""
    priority_cols = [
        "datetime_utc",
        "icao",
        "r",
        "t",
        "latitude",
        "longitude",
        "altitude_ft",
        "geometric_altitude_ft",
        "ground_speed_kts",
        "track_deg",
        "vertical_rate_fpm",
    ]

    existing_priority = [col for col in priority_cols if col in df.columns]
    remaining_cols = [col for col in df.columns if col not in existing_priority]

    return df[existing_priority + remaining_cols]


def convert_json_to_csv(
    input_dir: Path,
    output_csv: Path,
    include_metadata: bool = False,
) -> None:
    """Convert all JSON files in directory to raw CSV format."""
    file_paths = list(input_dir.glob("trace_full_*.json"))
    total_files = len(file_paths)

    if total_files == 0:
        logger.warning(f"No trace files found in {input_dir}")
        return

    logger.info(f"📄 Converting {total_files} JSON files from {input_dir}")

    # Ensure output directory exists
    output_csv.parent.mkdir(parents=True, exist_ok=True)

    # Process files in chunks to manage memory
    all_rows = []
    row_count = 0

    with tqdm(file_paths, desc="🔄 Converting files", unit="file") as pbar:
        for file_path in pbar:
            file_rows = list(process_single_file(file_path, include_metadata))

            all_rows.extend(file_rows)
            row_count += len(file_rows)

            pbar.set_postfix({"rows": f"{row_count:,}"})

            # Write chunk if we've accumulated enough rows
            if len(all_rows) >= CHUNK_SIZE:
                df_chunk = pd.DataFrame(all_rows)
                if output_csv.exists():
                    df_chunk.to_csv(output_csv, mode="a", header=False, index=False)
                else:
                    df_chunk = create_datetime_columns(df_chunk)
                    df_chunk = reorder_columns(df_chunk)
                    df_chunk.to_csv(output_csv, index=False)
                all_rows = []

    # Write remaining rows
    if all_rows:
        df_chunk = pd.DataFrame(all_rows)
        if output_csv.exists():
            df_chunk.to_csv(output_csv, mode="a", header=False, index=False)
        else:
            df_chunk = create_datetime_columns(df_chunk)
            df_chunk = reorder_columns(df_chunk)
            df_chunk.to_csv(output_csv, index=False)

    logger.info(f"✅ Converted {row_count:,} rows to {output_csv}")


def main() -> None:
    """Main entry point for JSON to CSV conversion."""
    parser = argparse.ArgumentParser(
        description="Convert aircraft trace JSON files to raw CSV format.",
        epilog="Example: python json_to_csv.py 2025.01.01 --include-metadata",
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
        date_str_clean = str(date_str)
        logger.info(f"\n🧩 Converting JSON to CSV for {date_str_clean}...")

        input_dir = DEFAULT_DATA_DIR / date_str_clean / "json"
        output_dir = DEFAULT_DATA_DIR / date_str_clean / "csv"
        os.makedirs(output_dir, exists_ok=True)
        output_csv = output_dir / f"{date_str_clean.replace('.', '_')}_raw.csv"

        if not input_dir.exists():
            logger.error(f"Input directory does not exist: {input_dir}")
            continue

        convert_json_to_csv(input_dir, output_csv, args.include_metadata)

    logger.info("\n🎉 JSON-to-CSV conversion complete.")


if __name__ == "__main__":
    main()
