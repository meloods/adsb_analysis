#!/usr/bin/env python3
"""Batch convert all JSON trace files for a given date to CSV format."""

import argparse
import logging
import time
from pathlib import Path
from typing import List, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

from config import BASE_DIR, MAX_WORKERS, SUBDIR_TRACES, SUBDIR_CSV, TRACE_FILE_PREFIX
from utils import (
    setup_logging,
    validate_date,
    create_output_path,
)
from trace_processor import TraceProcessor, ProcessingConfig

setup_logging()


class BatchProcessor:
    """Handles batch conversion of JSON trace files to CSV format."""

    def __init__(self, date_str: str, config: ProcessingConfig):
        self.date_str = date_str
        self.config = config
        self.logger = logging.getLogger(__name__)

        # Setup directory paths
        self.traces_dir = BASE_DIR / date_str / SUBDIR_TRACES
        self.csv_dir = BASE_DIR / date_str / SUBDIR_CSV

    def find_json_files(self) -> List[Path]:
        """Find all trace_full_*.json files in the traces directory."""
        if not self.traces_dir.exists():
            self.logger.error(f"Traces directory does not exist: {self.traces_dir}")
            return []

        # Look for JSON files recursively (they may be in subdirectories)
        json_files = list(self.traces_dir.rglob(f"{TRACE_FILE_PREFIX}*.json"))

        self.logger.info(f"Found {len(json_files)} JSON files in {self.traces_dir}")
        return json_files

    def csv_already_exists(self, json_path: Path) -> bool:
        """Check if corresponding CSV file already exists and is not empty."""
        csv_filename = json_path.name.replace(".json", ".csv")
        csv_path = self.csv_dir / csv_filename

        if not csv_path.exists():
            return False

        # Check if file is not empty
        try:
            return csv_path.stat().st_size > 0
        except OSError:
            return False

    def convert_single_file(self, json_path: Path) -> Tuple[Path, bool, float]:
        """Convert a single JSON file to CSV and return (path, success, duration)."""
        start_time = time.time()

        try:
            # Skip if CSV already exists and force is not enabled
            if not self.config.force_reprocess and self.csv_already_exists(json_path):
                return json_path, True, time.time() - start_time

            # Create output path
            output_path = create_output_path(
                json_path, self.date_str, SUBDIR_CSV, ".csv"
            )

            # Create processor for this file
            processor = TraceProcessor(self.config)

            # Convert the file
            success = processor.process_single_trace(json_path, output_path)
            duration = time.time() - start_time

            return json_path, success, duration

        except Exception as e:
            self.logger.error(f"Unexpected error processing {json_path.name}: {e}")
            return json_path, False, time.time() - start_time

    def batch_convert_sequential(
        self, json_files: List[Path]
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
                    path, success, duration = self.convert_single_file(json_path)
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
        self, json_files: List[Path]
    ) -> Tuple[int, int, List[float]]:
        """Convert JSON files in parallel with progress bar."""
        successful = 0
        failed = 0
        durations = []

        # Temporarily suppress INFO logs to keep progress bar clean
        original_level = logging.getLogger().level
        logging.getLogger().setLevel(logging.WARNING)

        try:
            with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
                # Submit all tasks
                future_to_path = {
                    executor.submit(self.convert_single_file, json_path): json_path
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
                                    "workers": self.config.max_workers,
                                }
                            )
                            pbar.update(1)

                        except Exception as e:
                            # Restore logging temporarily for errors
                            logging.getLogger().setLevel(original_level)
                            self.logger.error(
                                f"Error in parallel processing for {json_path.name}: {e}"
                            )
                            logging.getLogger().setLevel(logging.WARNING)
                            failed += 1
                            pbar.update(1)
        finally:
            # Restore original logging level
            logging.getLogger().setLevel(original_level)

        return successful, failed, durations

    def run_batch_conversion(self) -> bool:
        """Execute the complete batch conversion process."""
        # Find all JSON files
        json_files = self.find_json_files()
        if not json_files:
            self.logger.warning(f"No JSON files found for {self.date_str}")
            return False

        # Filter files if not forcing reconversion
        if not self.config.force_reprocess:
            original_count = len(json_files)
            json_files = [f for f in json_files if not self.csv_already_exists(f)]
            skipped = original_count - len(json_files)

            if skipped > 0:
                self.logger.info(
                    f"Skipping {skipped} files that already have CSV outputs"
                )

            if not json_files:
                self.logger.info("All JSON files have already been converted to CSV")
                return True

        # Log processing configuration
        metadata_status = "enabled" if self.config.include_metadata else "disabled"
        processing_mode = (
            "parallel" if self.config.parallel and len(json_files) > 1 else "sequential"
        )

        self.logger.info(f"Converting {len(json_files)} JSON files to CSV")
        self.logger.info(f"Processing mode: {processing_mode}")
        self.logger.info(f"Metadata flattening: {metadata_status}")

        # Convert files
        if self.config.parallel and len(json_files) > 1:
            successful, failed, durations = self.batch_convert_parallel(json_files)
        else:
            successful, failed, durations = self.batch_convert_sequential(json_files)

        # Report simple results
        self.logger.info(
            f"Conversion complete: {successful} successful, {failed} failed"
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

    # Create processing configuration
    config = ProcessingConfig(
        include_metadata=args.metadata.lower() == "true",
        force_reprocess=args.force,
        parallel=args.parallel and not args.sequential,
        max_workers=args.max_workers,
    )

    # Create and run batch processor
    processor = BatchProcessor(args.date, config)
    success = processor.run_batch_conversion()

    return 0 if success else 1


if __name__ == "__main__":
    exit(main())
