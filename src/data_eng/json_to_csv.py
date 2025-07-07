#!/usr/bin/env python3
"""
Convert flattened trace JSON files into hourly CSV files using streaming architecture.
"""

import argparse
import csv
import orjson
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
from tqdm import tqdm


from src.utils import setup_logging, load_config, get_data_dir, validate_date

# --- Setup ---
setup_logging()
config = load_config()
DATA_DIR: Path = get_data_dir(config)

# --- Constants ---
BUFFER_SIZE = 5000  # Number of rows to buffer before writing

# Predefined schema - includes datetime_utc for hour routing
PREDEFINED_COLUMNS = [
    # Top-level file metadata
    "icao",
    "timestamp",
    "r",
    "t",
    "desc",
    "dbFlags",
    "year",
    "ownOp",
    # Computed datetime column
    "datetime_utc",
    # Trace data columns (in order from the 14-element array)
    "seconds_after_timestamp",
    "latitude",
    "longitude",
    "altitude_ft",
    "ground_speed_kts",
    "track_deg",
    "flags_bitfield",
    "vertical_rate_fpm",
    "aircraft_metadata",  # Will be JSON string
    "source_type",
    "geometric_altitude_ft",
    "geometric_vertical_rate_fpm",
    "indicated_airspeed_kts",
    "roll_angle_deg",
]

TOP_LEVEL_KEYS = [
    "icao",
    "timestamp",
    "r",
    "t",
    "desc",
    "dbFlags",
    "year",
    "ownOp",
]


def calculate_datetime_utc(base_timestamp: float, seconds_offset: float) -> datetime:
    """
    Calculate UTC datetime from base timestamp and offset.

    Args:
        base_timestamp: Base UNIX timestamp from JSON file
        seconds_offset: Seconds after timestamp from trace entry

    Returns:
        UTC datetime object
    """
    actual_timestamp = base_timestamp + seconds_offset
    return datetime.fromtimestamp(actual_timestamp, tz=timezone.utc)


def get_hour_bucket(dt_utc: datetime) -> int:
    """
    Get the hour bucket (0-23) for a UTC datetime.

    Args:
        dt_utc: UTC datetime

    Returns:
        Hour bucket (0-23)
    """
    return dt_utc.hour


def flatten_trace_entry(
    trace_entry: List[Any],
    file_metadata: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Flatten one trace entry with predefined schema including datetime_utc.

    Args:
        trace_entry: List of values for a single trace vector.
        file_metadata: Top-level metadata from the JSON file.

    Returns:
        A flattened dictionary row matching PREDEFINED_COLUMNS.
    """
    row = {}

    # Add file-level metadata first
    for key in TOP_LEVEL_KEYS:
        row[key] = file_metadata.get(key)

    # Get timestamp components for datetime calculation
    base_timestamp = file_metadata.get("timestamp", 0)
    seconds_offset = trace_entry[0] if len(trace_entry) > 0 else 0

    # Calculate and add datetime_utc
    try:
        dt_utc = calculate_datetime_utc(base_timestamp, seconds_offset)
        row["datetime_utc"] = dt_utc.isoformat()
    except (ValueError, TypeError, OverflowError) as e:
        logging.warning(
            f"Invalid timestamp calculation: base={base_timestamp}, offset={seconds_offset}, error={e}"
        )
        row["datetime_utc"] = None

    # Add trace data in order
    trace_columns = [
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

    for idx, column in enumerate(trace_columns):
        if idx < len(trace_entry):
            value = trace_entry[idx]
            # Convert aircraft_metadata dict to JSON string
            if column == "aircraft_metadata" and isinstance(value, dict):
                row[column] = orjson.dumps(value).decode("utf-8")
            else:
                row[column] = value
        else:
            row[column] = None

    return row


def process_file_streaming(
    file_path: Path,
    file_metadata: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """
    Process a single JSON file and return rows.

    Args:
        file_path: Path to the trace JSON file.
        file_metadata: Shared metadata for this file.

    Returns:
        List of row dictionaries.
    """
    rows = []

    try:
        with file_path.open("r", encoding="utf-8") as f:
            content = f.read()
            data = orjson.loads(content)

        # Update file metadata with actual values
        for key in TOP_LEVEL_KEYS:
            if key in data:
                file_metadata[key] = data[key]

        # Process each trace entry
        for entry in data.get("trace", []):
            row = flatten_trace_entry(entry, file_metadata)
            rows.append(row)

    except Exception as e:
        logging.warning(f"❌ Failed to process {file_path.name}: {e}")

    return rows


class HourlyCSVWriter:
    """Handles buffered writing to a single hourly CSV file."""

    def __init__(self, output_path: Path, hour_range: str):
        self.output_path = output_path
        self.hour_range = hour_range
        self.buffer = []
        self.total_rows_written = 0
        self.csv_file = None
        self.writer = None
        self.is_initialized = False

    def _initialize(self):
        """Lazy initialization - only create file when first row is written."""
        if not self.is_initialized:
            self.csv_file = self.output_path.open("w", newline="", encoding="utf-8")
            self.writer = csv.DictWriter(self.csv_file, fieldnames=PREDEFINED_COLUMNS)
            self.writer.writeheader()
            self.is_initialized = True

    def add_rows(self, rows: List[Dict[str, Any]]) -> None:
        """Add rows to buffer, auto-flushing when needed."""
        if not rows:
            return

        self._initialize()
        self.buffer.extend(rows)

        if len(self.buffer) >= BUFFER_SIZE:
            self.flush()

    def flush(self) -> None:
        """Write buffer to CSV and clear it."""
        if self.buffer and self.writer:
            self.writer.writerows(self.buffer)
            self.total_rows_written += len(self.buffer)
            self.buffer.clear()

    def close(self):
        """Close the CSV file after flushing remaining buffer."""
        self.flush()
        if self.csv_file:
            self.csv_file.close()


class HourlyCSVManager:
    """Manages 24 hourly CSV writers and routes rows to appropriate files."""

    def __init__(self, output_dir: Path, date_str: str):
        self.output_dir = output_dir
        self.date_str = date_str
        self.writers = {}
        self.row_counts = {hour: 0 for hour in range(24)}

        # Create writers for all 24 hours
        for hour in range(24):
            hour_range = f"{hour:02d}00-{(hour + 1):02d}00"
            filename = f"{date_str.replace('.', '_')}_{hour_range}.csv"
            output_path = output_dir / filename
            self.writers[hour] = HourlyCSVWriter(output_path, hour_range)

    def add_row(self, row: Dict[str, Any]) -> None:
        """Route a single row to the appropriate hourly CSV."""
        # Extract datetime to determine hour bucket
        datetime_str = row.get("datetime_utc")
        if not datetime_str:
            logging.warning("Row missing datetime_utc, skipping")
            return

        try:
            dt_utc = datetime.fromisoformat(datetime_str.replace("Z", "+00:00"))
            hour_bucket = get_hour_bucket(dt_utc)

            # Add to appropriate writer
            self.writers[hour_bucket].add_rows([row])
            self.row_counts[hour_bucket] += 1

        except (ValueError, TypeError) as e:
            logging.warning(f"Invalid datetime_utc format: {datetime_str}, error: {e}")

    def add_rows(self, rows: List[Dict[str, Any]]) -> None:
        """Route multiple rows to appropriate hourly CSVs."""
        for row in rows:
            self.add_row(row)

    def close_all(self) -> None:
        """Close all writers and return statistics."""
        for writer in self.writers.values():
            writer.close()

    def get_statistics(self) -> Dict[str, Any]:
        """Get processing statistics."""
        total_rows = sum(writer.total_rows_written for writer in self.writers.values())
        active_hours = sum(1 for count in self.row_counts.values() if count > 0)

        return {
            "total_rows": total_rows,
            "active_hours": active_hours,
            "hourly_breakdown": self.row_counts,
            "files_created": [
                writer.output_path.name
                for writer in self.writers.values()
                if writer.total_rows_written > 0
            ],
        }


def flatten_all_json_to_hourly_csv(date_str: str) -> None:
    """
    Stream all aircraft trace JSON files into hourly CSV files.

    Args:
        date_str: Date in YYYY.MM.DD format (e.g., '2025.05.28').
    """
    input_dir = DATA_DIR / date_str / "json"
    output_dir = DATA_DIR / date_str / "csv"
    output_dir.mkdir(parents=True, exist_ok=True)

    # Get all JSON files
    file_paths = list(input_dir.glob("trace_full_*.json"))
    total_files = len(file_paths)

    if total_files == 0:
        logging.warning(f"No trace files found in {input_dir}")
        return

    logging.info(f"📄 Streaming {total_files} trace files from {input_dir}")
    logging.info(f"📂 Creating hourly CSV files in {output_dir}")

    # Statistics tracking
    processed_files = 0
    failed_files = 0

    # Stream processing with hourly CSV routing
    with HourlyCSVManager(output_dir, date_str) as csv_manager:
        # Process files with progress bar
        for file_path in tqdm(file_paths, desc="🔄 Processing files", unit="file"):
            # Prepare file metadata template
            file_metadata = {key: None for key in TOP_LEVEL_KEYS}

            # Process single file
            rows = process_file_streaming(file_path, file_metadata)

            if rows:
                csv_manager.add_rows(rows)
                processed_files += 1
            else:
                failed_files += 1

        # Get final statistics
        stats = csv_manager.get_statistics()

    # Report results
    logging.info(f"✅ Processed {processed_files}/{total_files} files successfully")
    if failed_files > 0:
        logging.warning(f"⚠️  Failed to process {failed_files} files")

    logging.info(f"📊 Total rows written: {stats['total_rows']:,}")
    logging.info(f"⏰ Active hours: {stats['active_hours']}/24")

    # Log hourly breakdown
    for hour, count in stats["hourly_breakdown"].items():
        if count > 0:
            hour_range = f"{hour:02d}00-{(hour + 1):02d}00"
            logging.info(f"   {hour_range}: {count:,} rows")

    logging.info(f"📁 Files created: {len(stats['files_created'])}")
    for filename in stats["files_created"]:
        file_path = output_dir / filename
        if file_path.exists():
            size_mb = file_path.stat().st_size / (1024 * 1024)
            logging.info(f"   {filename}: {size_mb:.1f} MB")


# Add context manager support to HourlyCSVManager
HourlyCSVManager.__enter__ = lambda self: self
HourlyCSVManager.__exit__ = lambda self, *args: self.close_all()


def main() -> None:
    """Command-line entry point."""
    parser = argparse.ArgumentParser(
        description="Stream aircraft trace JSON files into hourly CSV files (memory efficient).",
        epilog="Example: PYTHONPATH=src python src/data_eng/json_to_csv.py 2025.01.01",
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
        logging.info(f"\n🧩 Converting JSON to hourly CSV files for {date_str}...")
        flatten_all_json_to_hourly_csv(date_str)

    logging.info("\n🎉 JSON-to-CSV conversion complete.")


if __name__ == "__main__":
    main()
