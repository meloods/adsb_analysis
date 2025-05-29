#!/usr/bin/env python3
"""Common utility functions for aircraft data processing."""

import argparse
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional

from config import LOG_FORMAT, LOG_LEVEL


def setup_logging():
    """Set up logging configuration (only if not already configured)."""
    # Check if logging is already configured
    if logging.root.handlers:
        return

    logging.basicConfig(
        level=getattr(logging, LOG_LEVEL),
        format=LOG_FORMAT,
    )


def validate_date(date_str: str) -> str:
    """Validate date format (YYYY.MM.DD)."""
    try:
        datetime.strptime(date_str, "%Y.%m.%d")
        return date_str
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"Invalid date format: {date_str}. Use YYYY.MM.DD."
        )


def extract_date_from_path(file_path: Path) -> Optional[str]:
    """Extract date string (YYYY.MM.DD) from file path."""
    path_parts = file_path.parts

    for part in path_parts:
        if len(part) == 10 and part.count(".") == 2:
            try:
                year, month, day = part.split(".")
                if len(year) == 4 and len(month) == 2 and len(day) == 2:
                    # Validate it's actually a date
                    datetime.strptime(part, "%Y.%m.%d")
                    return part
            except ValueError:
                continue

    return None


def flatten_metadata(metadata: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Flatten aircraft metadata dict into separate columns with 'meta_' prefix."""
    if not metadata or not isinstance(metadata, dict):
        return {}

    flattened = {}
    for key, value in metadata.items():
        # Prefix with 'meta_' to avoid conflicts with other columns
        column_name = f"meta_{key}"
        flattened[column_name] = value

    return flattened


def create_output_path(
    input_path: Path, date_str: str, output_subdir: str, extension: str
) -> Path:
    """Create standardized output path for processed files."""
    from config import BASE_DIR

    output_dir = BASE_DIR / date_str / output_subdir
    output_dir.mkdir(parents=True, exist_ok=True)

    output_filename = input_path.name.replace(input_path.suffix, extension)
    return output_dir / output_filename


class PerformanceTracker:
    """Track and report performance metrics for batch operations."""

    def __init__(self):
        self.start_time = time.time()
        self.phase_times = {}
        self.counters = {}

    def start_phase(self, phase_name: str):
        """Start timing a processing phase."""
        self.phase_times[phase_name] = {"start": time.time()}

    def end_phase(self, phase_name: str):
        """End timing a processing phase."""
        if phase_name in self.phase_times:
            self.phase_times[phase_name]["duration"] = (
                time.time() - self.phase_times[phase_name]["start"]
            )

    def increment(self, counter_name: str, amount: int = 1):
        """Increment a counter."""
        self.counters[counter_name] = self.counters.get(counter_name, 0) + amount

    def get_total_time(self) -> float:
        """Get total elapsed time since creation."""
        return time.time() - self.start_time

    def report_summary(self, operation_name: str, logger: logging.Logger):
        """Log a comprehensive performance summary."""
        total_time = self.get_total_time()

        logger.info(f"\n{'=' * 50}")
        logger.info(f"{operation_name.upper()} PERFORMANCE SUMMARY")
        logger.info(f"{'=' * 50}")

        # Report counters
        for name, count in self.counters.items():
            logger.info(f"{name.replace('_', ' ').title()}: {count:,}")

        # Report phase times
        for phase, timing in self.phase_times.items():
            if "duration" in timing:
                logger.info(
                    f"{phase.replace('_', ' ').title()} time: {timing['duration']:.2f}s"
                )

        logger.info(f"Total time: {total_time:.2f}s")

        # Calculate rates if we have relevant counters
        if "files_processed" in self.counters and total_time > 0:
            rate = self.counters["files_processed"] / total_time
            logger.info(f"Processing rate: {rate:.1f} files/second")

        if "rows_processed" in self.counters and total_time > 0:
            rate = self.counters["rows_processed"] / total_time
            logger.info(f"Data rate: {rate:,.0f} rows/second")
