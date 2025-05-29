#!/usr/bin/env python3
"""Convert a single trace_full_<ICAO-hex-id>.json file to CSV format."""

import argparse
import json
import logging
from pathlib import Path
import pandas as pd
from typing import Dict, Any, Optional

from config import BASE_DIR
from utils import setup_logging

setup_logging()

# Define trace array field names based on documentation
TRACE_FIELD_NAMES = [
    "seconds_after_timestamp",  # 0
    "latitude",  # 1
    "longitude",  # 2
    "altitude_ft",  # 3
    "ground_speed_kts",  # 4
    "track_deg",  # 5
    "flags_bitfield",  # 6
    "vertical_rate_fpm",  # 7
    "aircraft_metadata",  # 8 - will be flattened
    "source_type",  # 9
    "geometric_altitude_ft",  # 10
    "geometric_vertical_rate_fpm",  # 11
    "indicated_airspeed_kts",  # 12
    "roll_angle_deg",  # 13
]


def flatten_aircraft_metadata(metadata: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Flatten aircraft metadata dict into separate columns with 'meta_' prefix."""
    if not metadata or not isinstance(metadata, dict):
        return {}

    flattened = {}
    for key, value in metadata.items():
        # Prefix with 'meta_' to avoid conflicts with other columns
        column_name = f"meta_{key}"
        flattened[column_name] = value

    return flattened


def convert_json_to_csv(json_path: Path, include_metadata: bool = False) -> bool:
    """Convert a single JSON trace file to CSV format."""
    try:
        # Read JSON file
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        if "trace" not in data:
            logging.error(f"No 'trace' key found in {json_path}")
            return False

        if not data["trace"]:
            logging.warning(f"Empty trace array in {json_path}")
            return False

        # Extract metadata (all top-level keys except 'trace')
        metadata = {k: v for k, v in data.items() if k != "trace"}
        base_timestamp = data.get("timestamp", 0)

        # Process trace entries
        rows = []
        for trace_entry in data["trace"]:
            if not isinstance(trace_entry, list):
                logging.warning(f"Skipping non-list trace entry: {trace_entry}")
                continue

            row = {}

            # Process each field in the trace array
            for i, field_name in enumerate(TRACE_FIELD_NAMES):
                if i < len(trace_entry):
                    if field_name == "aircraft_metadata":
                        # Handle metadata based on include_metadata flag
                        if include_metadata:
                            flattened_meta = flatten_aircraft_metadata(trace_entry[i])
                            row.update(flattened_meta)
                        # If include_metadata is False, skip this field entirely
                    else:
                        row[field_name] = trace_entry[i]
                else:
                    # Field not present in this trace entry
                    if field_name != "aircraft_metadata":
                        row[field_name] = None

            # Add top-level metadata columns (repeat for each row)
            row.update(metadata)

            # Calculate absolute timestamp
            seconds_offset = trace_entry[0] if len(trace_entry) > 0 else 0
            row["abs_timestamp"] = base_timestamp + seconds_offset

            rows.append(row)

        if not rows:
            logging.warning(f"No valid trace entries found in {json_path}")
            return False

        # Create DataFrame
        df = pd.DataFrame(rows)

        # Determine output path - extract date from path structure
        # Assume path is like: data/2025.05.27/traces/trace_full_abc123.json
        path_parts = json_path.parts
        date_part = None

        # Look for date pattern in path (YYYY.MM.DD format)
        for part in path_parts:
            if len(part) == 10 and part.count(".") == 2:
                try:
                    # Validate it's actually a date-like string
                    year, month, day = part.split(".")
                    if len(year) == 4 and len(month) == 2 and len(day) == 2:
                        date_part = part
                        break
                except ValueError:
                    continue

        if date_part:
            csv_dir = BASE_DIR / date_part / "csv"
        else:
            # Fallback: create csv subdirectory in same location as input
            csv_dir = json_path.parent / "csv"

        csv_dir.mkdir(parents=True, exist_ok=True)

        csv_filename = json_path.name.replace(".json", ".csv")
        csv_path = csv_dir / csv_filename

        # Write CSV
        df.to_csv(csv_path, index=False)

        logging.info(
            f"Converted {json_path.name} -> {csv_path} ({len(df)} rows, {len(df.columns)} columns)"
        )
        return True

    except json.JSONDecodeError as e:
        logging.error(f"Invalid JSON in {json_path}: {e}")
        return False
    except Exception as e:
        logging.error(f"Failed to convert {json_path}: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Convert a single trace JSON file to CSV format",
        epilog="Example: python json_to_csv.py data/2025.05.27/traces/trace_full_abc123.json --metadata true",
    )
    parser.add_argument(
        "json_file", type=Path, help="Path to the trace JSON file to convert"
    )
    parser.add_argument(
        "--metadata",
        type=str,
        choices=["true", "false"],
        default="false",
        help="Include and flatten aircraft metadata (default: false)",
    )

    args = parser.parse_args()

    if not args.json_file.exists():
        logging.error(f"File not found: {args.json_file}")
        return 1

    if not args.json_file.name.startswith("trace_full_"):
        logging.warning(
            f"File doesn't match expected naming pattern: {args.json_file.name}"
        )

    # Convert string flag to boolean
    include_metadata = args.metadata.lower() == "true"

    success = convert_json_to_csv(args.json_file, include_metadata)
    return 0 if success else 1


if __name__ == "__main__":
    exit(main())
