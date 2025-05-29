#!/usr/bin/env python3
"""Core business logic for processing aircraft trace data."""

import json
import logging
from pathlib import Path
from typing import Dict, Any, Optional
from dataclasses import dataclass
import pandas as pd

from utils import flatten_metadata


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
    "aircraft_metadata",  # 8 - will be flattened or omitted
    "source_type",  # 9
    "geometric_altitude_ft",  # 10
    "geometric_vertical_rate_fpm",  # 11
    "indicated_airspeed_kts",  # 12
    "roll_angle_deg",  # 13
]


@dataclass
class ProcessingConfig:
    """Configuration for trace processing operations."""

    include_metadata: bool = False
    force_reprocess: bool = False
    parallel: bool = True
    max_workers: int = 4


class TraceProcessor:
    """Core processor for aircraft trace data with consistent column handling."""

    def __init__(self, config: ProcessingConfig):
        self.config = config
        self.logger = logging.getLogger(__name__)

    def load_json_data(self, json_path: Path) -> Optional[Dict[str, Any]]:
        """Load and validate JSON trace data."""
        try:
            with open(json_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            if "trace" not in data:
                self.logger.error(f"No 'trace' key found in {json_path}")
                return None

            if not data["trace"]:
                self.logger.warning(f"Empty trace array in {json_path}")
                return None

            return data

        except json.JSONDecodeError as e:
            self.logger.error(f"Invalid JSON in {json_path}: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Failed to load {json_path}: {e}")
            return None

    def json_to_dataframe(self, data: Dict[str, Any]) -> pd.DataFrame:
        """Convert JSON trace data to standardized DataFrame."""
        # Extract metadata (all top-level keys except 'trace')
        metadata = {k: v for k, v in data.items() if k != "trace"}
        base_timestamp = data.get("timestamp", 0)

        # Process trace entries
        rows = []
        for trace_entry in data["trace"]:
            if not isinstance(trace_entry, list):
                self.logger.warning(f"Skipping non-list trace entry: {trace_entry}")
                continue

            row = {}

            # Process each field in the trace array
            for i, field_name in enumerate(TRACE_FIELD_NAMES):
                if i < len(trace_entry):
                    if field_name == "aircraft_metadata":
                        # Handle metadata based on configuration
                        if self.config.include_metadata:
                            flattened_meta = flatten_metadata(trace_entry[i])
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

        return pd.DataFrame(rows)

    def standardize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply consistent column ordering and add derived fields."""
        if df.empty:
            return df

        # Create datetime column from abs_timestamp
        try:
            df["datetime"] = pd.to_datetime(df["abs_timestamp"], unit="s", utc=True)
            self.logger.debug("Created datetime column from abs_timestamp")
        except Exception as e:
            self.logger.error(f"Failed to create datetime column: {e}")
            df["datetime"] = pd.NaT

        # Define desired column order
        priority_columns = ["datetime", "abs_timestamp", "icao"]
        ending_columns = ["timestamp", "seconds_after_timestamp"]

        # Get all columns and determine the middle columns
        all_columns = list(df.columns)

        # Start with priority columns (if they exist)
        ordered_columns = []
        for col in priority_columns:
            if col in all_columns:
                ordered_columns.append(col)
            else:
                self.logger.debug(f"Priority column '{col}' not found in DataFrame")

        # Add middle columns (everything except priority and ending columns)
        middle_columns = [
            col
            for col in all_columns
            if col not in priority_columns and col not in ending_columns
        ]
        ordered_columns.extend(middle_columns)

        # Add ending columns (if they exist)
        for col in ending_columns:
            if col in all_columns:
                ordered_columns.append(col)
            else:
                self.logger.debug(f"Ending column '{col}' not found in DataFrame")

        # Reorder the DataFrame
        df = df.reindex(columns=ordered_columns)

        self.logger.debug(
            f"Standardized columns: first 3 = {ordered_columns[:3]}, last 2 = {ordered_columns[-2:]}"
        )

        return df

    def process_single_trace(self, json_path: Path, output_path: Path) -> bool:
        """Process a single trace file from JSON to standardized CSV."""
        try:
            # Load and validate data
            data = self.load_json_data(json_path)
            if data is None:
                return False

            # Convert to DataFrame
            df = self.json_to_dataframe(data)
            if df.empty:
                self.logger.warning(f"No valid trace entries found in {json_path}")
                return False

            # Standardize columns
            df = self.standardize_columns(df)

            # Write CSV
            output_path.parent.mkdir(parents=True, exist_ok=True)
            df.to_csv(output_path, index=False)

            self.logger.debug(
                f"Processed {json_path.name} -> {output_path} ({len(df)} rows, {len(df.columns)} columns)"
            )
            return True

        except Exception as e:
            self.logger.error(f"Failed to process {json_path}: {e}")
            return False

    def validate_csv_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate and clean CSV data for aggregation."""
        if df.empty:
            return df

        # Check for abs_timestamp column
        if "abs_timestamp" not in df.columns:
            self.logger.warning("Missing 'abs_timestamp' column, skipping DataFrame")
            return pd.DataFrame()

        # Remove rows with missing abs_timestamp values
        initial_rows = len(df)
        df = df.dropna(subset=["abs_timestamp"])
        dropped_rows = initial_rows - len(df)

        if dropped_rows > 0:
            self.logger.warning(
                f"Dropped {dropped_rows} rows with missing abs_timestamp"
            )

        if len(df) == 0:
            self.logger.warning("No valid rows remaining after cleaning")
            return pd.DataFrame()

        # Ensure abs_timestamp is numeric
        df["abs_timestamp"] = pd.to_numeric(df["abs_timestamp"], errors="coerce")
        df = df.dropna(subset=["abs_timestamp"])

        if len(df) == 0:
            self.logger.warning("No valid abs_timestamp values found")
            return pd.DataFrame()

        return df
