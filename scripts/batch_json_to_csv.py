#!/usr/bin/env python3
"""Batch convert all JSON trace files for a given date to CSV format."""

import argparse
import logging
import time
from pathlib import Path
from typing import List, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

from config import BASE_DIR, MAX_WORKERS
from utils import setup_logging, validate_date
from json_to_csv import convert_json_to_csv

setup_logging()


def find_json_files(traces_dir: Path) -> List[Path]:
    """Find all trace_full_*.json files in the traces directory."""
    if not traces_dir.exists():
        logging.error(f"Traces directory does not exist: {traces_dir}")
        return []

    # Look for JSON files recursively (they may be in subdirectories like 00/, 01/, etc.)
    json_files = list(traces_dir.rglob("trace_full_*.json"))

    logging.info(f"Found {len(json_files)} JSON files in {traces_dir}")
    return json_files


def csv_already_exists(json_path: Path, csv_dir: Path) -> bool:
    """Check if corresponding CSV file already exists and is not empty."""
    csv_filename = json_path.name.replace(".json", ".csv")
    csv_path = csv_dir / csv_filename

    if not csv_path.exists():
        return False

    # Check if file is not empty
    try:
        return csv_path.stat().st_size > 0
    except OSError:
        return False


def convert_single_file(
    json_path: Path, force: bool = False, include_metadata: bool = False
) -> Tuple[Path, bool, float]:
    """Convert a single JSON file to CSV and return (path, success, duration)."""
    start_time = time.time()

    try:
        # Extract date from path for CSV directory
        path_parts = json_path.parts
        date_part = None

        for part in path_parts:
            if len(part) == 10 and part.count(".") == 2:
                try:
                    year, month, day = part.split(".")
                    if len(year) == 4 and len(month) == 2 and len(day) == 2:
                        date_part = part
                        break
                except ValueError:
                    continue

        if not date_part:
            logging.error(f"Could not extract date from path: {json_path}")
            return json_path, False, time.time() - start_time

        csv_dir = BASE_DIR / date_part / "csv"

        # Skip if CSV already exists and force is not enabled
        if not force and csv_already_exists(json_path, csv_dir):
            return json_path, True, time.time() - start_time

        # Convert the file
        success = convert_json_to_csv(json_path, include_metadata)
        duration = time.time() - start_time

        return json_path, success, duration

    except Exception as e:
        logging.error(f"Unexpected error processing {json_path.name}: {e}")
        return json_path, False, time.time() - start_time


def batch_convert_sequential(
    json_files: List[Path], force: bool = False, include_metadata: bool = False
) -> Tuple[int, int, List[float]]:
    """Convert JSON files sequentially with progress bar."""
    successful = 0
    failed = 0
    durations = []

    # Temporarily suppress INFO logs to keep progress bar clean
    original_level = logging.getLogger().level
    logging.getLogger().setLevel(logging.WARNING)

    try:
        with tqdm(
            total=len(json_files), desc="Converting JSON files", unit="file"
        ) as pbar:
            for json_path in json_files:
                path, success, duration = convert_single_file(
                    json_path, force, include_metadata
                )
                durations.append(duration)

                if success:
                    successful += 1
                else:
                    failed += 1

                pbar.set_postfix(
                    {
                        "success": successful,
                        "failed": failed,
                        "current": json_path.name[:20] + "..."
                        if len(json_path.name) > 20
                        else json_path.name,
                    }
                )
                pbar.update(1)
    finally:
        # Restore original logging level
        logging.getLogger().setLevel(original_level)

    return successful, failed, durations


def batch_convert_parallel(
    json_files: List[Path],
    force: bool = False,
    include_metadata: bool = False,
    max_workers: int = MAX_WORKERS,
) -> Tuple[int, int, List[float]]:
    """Convert JSON files in parallel with progress bar."""
    successful = 0
    failed = 0
    durations = []

    # Temporarily suppress INFO logs to keep progress bar clean
    original_level = logging.getLogger().level
    logging.getLogger().setLevel(logging.WARNING)

    try:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_path = {
                executor.submit(
                    convert_single_file, json_path, force, include_metadata
                ): json_path
                for json_path in json_files
            }

            with tqdm(
                total=len(json_files), desc="Converting JSON files", unit="file"
            ) as pbar:
                for future in as_completed(future_to_path):
                    json_path = future_to_path[future]

                    try:
                        path, success, duration = future.result()
                        durations.append(duration)

                        if success:
                            successful += 1
                        else:
                            failed += 1

                        pbar.set_postfix(
                            {
                                "success": successful,
                                "failed": failed,
                                "workers": max_workers,
                            }
                        )
                        pbar.update(1)

                    except Exception as e:
                        # Restore logging temporarily for errors
                        logging.getLogger().setLevel(original_level)
                        logging.error(
                            f"Error in parallel processing for {json_path.name}: {e}"
                        )
                        logging.getLogger().setLevel(logging.WARNING)
                        failed += 1
                        pbar.update(1)
    finally:
        # Restore original logging level
        logging.getLogger().setLevel(original_level)

    return successful, failed, durations


def batch_json_to_csv(
    date_str: str,
    force: bool = False,
    include_metadata: bool = False,
    parallel: bool = True,
    max_workers: int = MAX_WORKERS,
) -> bool:
    """Batch convert all JSON files for a given date to CSV format."""

    # Setup paths - JSON files should be in the decompressed 'traces' directory
    traces_dir = BASE_DIR / date_str / "traces"  # NOT "extracted/traces"
    csv_dir = BASE_DIR / date_str / "csv"

    # Create CSV directory if it doesn't exist
    csv_dir.mkdir(parents=True, exist_ok=True)

    # Find all JSON files
    json_files = find_json_files(traces_dir)

    if not json_files:
        logging.warning(f"No JSON files found for {date_str}")
        return False

    # Filter files if not forcing reconversion
    if not force:
        original_count = len(json_files)
        json_files = [f for f in json_files if not csv_already_exists(f, csv_dir)]
        skipped = original_count - len(json_files)

        if skipped > 0:
            logging.info(f"Skipping {skipped} files that already have CSV outputs")

        if not json_files:
            logging.info("All JSON files have already been converted to CSV")
            return True

    start_time = time.time()

    logging.info(
        f"Converting {len(json_files)} JSON files to CSV (parallel={parallel})"
    )

    # Convert files
    if parallel and len(json_files) > 1:
        successful, failed, durations = batch_convert_parallel(
            json_files, force, include_metadata, max_workers
        )
    else:
        successful, failed, durations = batch_convert_sequential(
            json_files, force, include_metadata
        )

    total_time = time.time() - start_time

    # Performance summary
    logging.info(f"\n{'=' * 50}")
    logging.info(f"BATCH CONVERSION SUMMARY for {date_str}")
    logging.info(f"{'=' * 50}")
    logging.info(f"Total files processed: {len(json_files)}")
    logging.info(f"Successful conversions: {successful}")
    logging.info(f"Failed conversions: {failed}")
    logging.info(f"Success rate: {successful / (successful + failed) * 100:.1f}%")
    logging.info(f"Total processing time: {total_time:.2f} seconds")

    if durations:
        avg_duration = sum(durations) / len(durations)
        logging.info(f"Average time per file: {avg_duration:.3f} seconds")
        logging.info(f"Fastest file: {min(durations):.3f} seconds")
        logging.info(f"Slowest file: {max(durations):.3f} seconds")

    if parallel and len(json_files) > 1:
        logging.info(
            f"Parallelization efficiency: {len(json_files) * sum(durations) / total_time / max_workers:.1f}x"
        )

    return failed == 0


def main():
    parser = argparse.ArgumentParser(
        description="Batch convert all JSON trace files for a given date to CSV format",
        epilog="Example: python batch_json_to_csv.py --date 2025.05.27 --metadata true --parallel",
    )
    parser.add_argument(
        "--date", type=validate_date, required=True, help="Date in YYYY.MM.DD format"
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Reprocess all files even if CSV outputs already exist",
    )
    parser.add_argument(
        "--metadata",
        type=str,
        choices=["true", "false"],
        default="false",
        help="Include and flatten aircraft metadata (default: false)",
    )
    parser.add_argument(
        "--parallel",
        action="store_true",
        default=True,
        help="Use parallel processing (default: True)",
    )
    parser.add_argument(
        "--sequential",
        action="store_true",
        help="Force sequential processing (overrides --parallel)",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=MAX_WORKERS,
        help=f"Maximum number of parallel workers (default: {MAX_WORKERS})",
    )

    args = parser.parse_args()

    # Determine processing mode
    parallel = args.parallel and not args.sequential

    # Convert string flag to boolean
    include_metadata = args.metadata.lower() == "true"

    if args.sequential:
        logging.info("Sequential processing mode enabled")
    elif parallel:
        logging.info(
            f"Parallel processing mode enabled (max_workers={args.max_workers})"
        )

    if include_metadata:
        logging.info(
            "Metadata flattening enabled - aircraft metadata will be expanded into separate columns"
        )
    else:
        logging.info(
            "Metadata omitted - aircraft metadata will not be included in CSV output"
        )

    logging.info(f"Starting batch conversion for {args.date}")

    success = batch_json_to_csv(
        date_str=args.date,
        force=args.force,
        include_metadata=include_metadata,
        parallel=parallel,
        max_workers=args.max_workers,
    )

    return 0 if success else 1


if __name__ == "__main__":
    exit(main())
