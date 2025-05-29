#!/usr/bin/env python3
"""
Convert JSON trace files to a single CSV file per date.
Memory-efficient version with streaming processing.
"""

import argparse
import csv
import json
import logging
import threading
from pathlib import Path
from queue import Queue, Empty
from typing import List, Dict, Any, Optional, Set
from concurrent.futures import ThreadPoolExecutor, as_completed
import gc
import time

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
MAX_WORKERS = 4
QUEUE_SIZE = 1000  # Limit queue size to control memory
BATCH_WRITE_SIZE = 500  # Write every 500 rows instead of accumulating

# Top-level metadata fields
TOP_LEVEL_FIELDS = ["icao", "base_timestamp", "r", "t", "desc", "dbFlags"]

# Core trace fields
CORE_TRACE_FIELDS = [
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


class ThreadSafeCSVWriter:
    """Thread-safe CSV writer that handles concurrent writes."""

    def __init__(self, csv_file: Path, fieldnames: List[str]):
        self.csv_file = csv_file
        self.fieldnames = fieldnames
        self.lock = threading.Lock()
        self.row_count = 0

        # Write header
        with csv_file.open("w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()

    def write_rows(self, rows: List[Dict[str, Any]]) -> None:
        """Write multiple rows to CSV in a thread-safe manner."""
        if not rows:
            return

        with self.lock:
            with self.csv_file.open("a", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=self.fieldnames)
                for row in rows:
                    # Ensure all fields are present
                    clean_row = {field: row.get(field, "") for field in self.fieldnames}
                    writer.writerow(clean_row)
                self.row_count += len(rows)


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


def process_single_file(
    file_path: Path, include_metadata: bool
) -> List[Dict[str, Any]]:
    """
    Process a single JSON file and extract all trace entries.
    Optimized for memory efficiency.

    Args:
        file_path: Path to the JSON file.
        include_metadata: Whether to include aircraft_metadata fields.

    Returns:
        List of dictionaries representing CSV rows.
    """
    try:
        with file_path.open("r") as f:
            data = json.load(f)

        # Extract top-level metadata once
        icao = data.get("icao", "")
        base_timestamp = data.get("timestamp", 0.0)
        registration = data.get("r", "")
        aircraft_type = data.get("t", "")
        description = data.get("desc", "")
        db_flags = data.get("dbFlags", 0)

        trace_data = data.get("trace", [])

        # Process trace entries
        rows = []
        for trace_entry in trace_data:
            if len(trace_entry) < 14:
                continue  # Skip malformed entries

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

            # Handle metadata if requested
            if include_metadata:
                metadata = trace_entry[8] if len(trace_entry) > 8 else None
                flattened_meta = flatten_metadata(metadata)
                row.update(flattened_meta)

            rows.append(row)

        # Clear data from memory immediately
        del data
        gc.collect()

        return rows

    except Exception as e:
        logging.warning(f"Failed to process {file_path.name}: {e}")
        return []


def file_worker(
    file_queue: Queue,
    result_queue: Queue,
    include_metadata: bool,
    progress_queue: Queue,
) -> None:
    """
    Worker function that processes files from queue.

    Args:
        file_queue: Queue containing file paths to process.
        result_queue: Queue to put processed results.
        include_metadata: Whether to include metadata.
        progress_queue: Queue for progress updates.
    """
    processed_count = 0

    while True:
        try:
            file_path = file_queue.get(timeout=1)
            if file_path is None:  # Sentinel value to stop worker
                break

            rows = process_single_file(file_path, include_metadata)

            if rows:
                result_queue.put(rows)

            processed_count += 1
            progress_queue.put(1)  # Signal progress
            file_queue.task_done()

            # Periodic garbage collection
            if processed_count % 50 == 0:
                gc.collect()

        except Empty:
            continue  # Timeout, check for more work
        except Exception as e:
            logging.error(f"Worker error: {e}")
            file_queue.task_done()


def discover_metadata_fields(
    sample_files: List[Path], max_samples: int = 50
) -> Set[str]:
    """
    Discover metadata fields from a sample of files.
    Reduced sample size for memory efficiency.

    Args:
        sample_files: List of files to sample.
        max_samples: Maximum files to sample.

    Returns:
        Set of unique metadata field names with 'meta_' prefix.
    """
    metadata_fields = set()
    sample_count = min(len(sample_files), max_samples)

    logging.info(f"Sampling {sample_count} files to discover metadata fields...")

    for file_path in sample_files[:sample_count]:
        try:
            with file_path.open("r") as f:
                data = json.load(f)

            # Check first few trace entries only
            trace_data = data.get("trace", [])[:10]  # Limit to first 10 entries

            for trace_entry in trace_data:
                if len(trace_entry) > 8 and trace_entry[8]:
                    metadata = trace_entry[8]
                    if isinstance(metadata, dict):
                        for key in metadata.keys():
                            metadata_fields.add(f"meta_{key}")

            del data  # Free memory immediately

        except Exception as e:
            logging.warning(f"Error sampling {file_path.name}: {e}")
            continue

    return metadata_fields


def convert_date_to_csv(
    date_str: str,
    include_metadata: bool,
    base_data_dir: Path = DATA_DIR,
    processed_dir: Path = PROCESSED_DIR,
) -> None:
    """
    Convert all JSON files for a given date to a single CSV file.
    Memory-efficient version with streaming processing.

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
    json_files = list(json_dir.glob("*.json"))
    if not json_files:
        logging.warning(f"No JSON files found for {date_str}")
        return

    logging.info(f"Found {len(json_files):,} JSON files for {date_str}")

    # Create output directory
    processed_dir.mkdir(parents=True, exist_ok=True)

    # Generate output filename
    output_filename = date_str.replace(".", "_") + ".csv"
    csv_file = processed_dir / output_filename

    # Discover metadata fields if needed
    metadata_fields = set()
    if include_metadata:
        metadata_fields = discover_metadata_fields(json_files)
        logging.info(f"Found {len(metadata_fields)} unique metadata fields")

    # Prepare CSV fieldnames
    all_fieldnames = TOP_LEVEL_FIELDS + CORE_TRACE_FIELDS + sorted(metadata_fields)

    # Initialize CSV writer
    csv_writer = ThreadSafeCSVWriter(csv_file, all_fieldnames)

    # Set up queues
    file_queue = Queue(maxsize=QUEUE_SIZE)
    result_queue = Queue(maxsize=QUEUE_SIZE)
    progress_queue = Queue()

    # Fill file queue
    for file_path in json_files:
        file_queue.put(file_path)

    # Add sentinel values to stop workers
    for _ in range(MAX_WORKERS):
        file_queue.put(None)

    # Start worker threads
    worker_threads = []
    for i in range(MAX_WORKERS):
        thread = threading.Thread(
            target=file_worker,
            args=(file_queue, result_queue, include_metadata, progress_queue),
            name=f"Worker-{i + 1}",
        )
        thread.start()
        worker_threads.append(thread)

    # Process results and write to CSV
    logging.info(f"Processing with {MAX_WORKERS} workers...")

    processed_files = 0
    pending_rows = []

    with tqdm(
        total=len(json_files), desc=f"Processing {date_str}", unit="file"
    ) as pbar:
        # Result collection loop
        while processed_files < len(json_files):
            try:
                # Check for progress updates
                while True:
                    try:
                        progress_queue.get_nowait()
                        processed_files += 1
                        pbar.update(1)
                    except Empty:
                        break

                # Get results and batch write
                try:
                    rows = result_queue.get(timeout=0.1)
                    pending_rows.extend(rows)

                    # Write batch when it gets large enough
                    if len(pending_rows) >= BATCH_WRITE_SIZE:
                        csv_writer.write_rows(pending_rows)
                        pending_rows = []
                        gc.collect()  # Clean up memory

                except Empty:
                    pass  # No results ready yet

                # Small sleep to prevent busy waiting
                time.sleep(0.01)

            except KeyboardInterrupt:
                logging.info("Interrupted by user")
                break

    # Write any remaining rows
    if pending_rows:
        csv_writer.write_rows(pending_rows)

    # Wait for all workers to complete
    for thread in worker_threads:
        thread.join()

    logging.info(f"CSV file saved to: {csv_file}")
    logging.info(f"Total rows written: {csv_writer.row_count:,}")


def main() -> None:
    """Command-line entry point."""
    parser = argparse.ArgumentParser(
        description="Convert JSON trace files to CSV format (memory-efficient version).",
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
            "Metadata inclusion enabled - processing with reduced sample size for efficiency"
        )

    for date_str in args.dates:
        logging.info(f"\nConverting JSON files for {date_str}...")
        convert_date_to_csv(date_str, include_metadata)

    logging.info("\nAll conversions complete.")


if __name__ == "__main__":
    main()
