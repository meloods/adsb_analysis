#!/usr/bin/env python3
"""
Perform time-based filtering on aircraft movement data.
Filters data into two UTC time windows: 01:00-04:00 and 11:00-13:00.
"""

import argparse
import logging
from pathlib import Path
from typing import Optional

import pandas as pd
from tqdm import tqdm

from utils import setup_logging, validate_date, load_config, get_data_dir

# --- Setup ---
setup_logging()
config = load_config()
DATA_DIR: Path = get_data_dir(config)


def create_utc_datetime(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create a timezone-aware UTC datetime column from timestamp and seconds_after_timestamp.

    Args:
        df: DataFrame containing 'timestamp' and 'seconds_after_timestamp' columns.

    Returns:
        DataFrame with new 'datetime_utc' column added.
    """
    # Calculate absolute timestamp in seconds
    absolute_timestamp = df["timestamp"] + df["seconds_after_timestamp"]

    # Convert to timezone-aware UTC datetime
    df["datetime_utc"] = pd.to_datetime(absolute_timestamp, unit="s", utc=True)

    return df


def filter_by_time_range(
    df: pd.DataFrame, start_hour: int, end_hour: int
) -> pd.DataFrame:
    """
    Filter DataFrame to keep only rows within specified UTC hour range.

    Args:
        df: DataFrame with 'datetime_utc' column.
        start_hour: Start hour (0-23) in UTC.
        end_hour: End hour (0-23) in UTC.

    Returns:
        Filtered DataFrame.
    """
    # Create boolean mask for time range filtering
    mask = (df["datetime_utc"].dt.hour >= start_hour) & (
        df["datetime_utc"].dt.hour < end_hour
    )

    # Apply boolean indexing and create copy - this always returns a DataFrame
    filtered_df = df[mask].copy()

    logging.info(
        f"Filtered {len(df):,} rows to {len(filtered_df):,} rows "
        f"for time range {start_hour:02d}:00-{end_hour:02d}:00 UTC"
    )

    return filtered_df


def reorder_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Move datetime_utc and icao columns to the first two positions.

    Args:
        df: Input DataFrame.

    Returns:
        DataFrame with reordered columns.
    """
    cols_to_fix = [
        "datetime_utc",
        "icao",
        "latitude",
        "longitude",
        "altitude_ft",
        "geometric_altitude_ft",
        "track_deg",
        "flags_bitfield",
    ]
    # Get all columns except datetime_utc and icao
    other_cols = [col for col in df.columns if col not in cols_to_fix]

    # Create new column order
    new_column_order = cols_to_fix + other_cols

    return df[new_column_order]


def process_time_filtering(date_str: str, base_data_dir: Path = DATA_DIR) -> None:
    """
    Process time-based filtering for aircraft movement data for a given date.

    Args:
        date_str: Date in YYYY.MM.DD format.
        base_data_dir: Base directory containing data.
    """
    # Convert date format from YYYY.MM.DD to YYYY-MM-DD
    formatted_date = date_str.replace(".", "-")

    # Define input file path
    input_file = base_data_dir / date_str / f"{formatted_date}_nearSG.csv"

    if not input_file.exists():
        logging.error(f"Input file not found: {input_file}")
        return

    # Create processed directory
    processed_dir = base_data_dir / "processed"
    processed_dir.mkdir(parents=True, exist_ok=True)

    logging.info(f"Reading data from: {input_file}")

    try:
        # Read the CSV file
        df = pd.read_csv(input_file)
        logging.info(f"Loaded {len(df):,} rows from {input_file.name}")

        # Validate required columns
        required_cols = ["timestamp", "seconds_after_timestamp", "icao"]
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            logging.error(f"Missing required columns: {missing_cols}")
            return

        # Create UTC datetime column
        df = create_utc_datetime(df)

        # Define time windows
        time_windows = [
            {"start": 1, "end": 4, "suffix": "0100-0400"},
            {"start": 11, "end": 13, "suffix": "1100-1300"},
        ]

        # Process each time window
        for window in tqdm(time_windows, desc=f"Processing {date_str}", unit="window"):
            # Filter by time range
            filtered_df = filter_by_time_range(df, window["start"], window["end"])

            if filtered_df.empty:
                logging.warning(f"No data found for time window {window['suffix']}")
                continue

            # Reorder columns
            filtered_df = reorder_columns(filtered_df)

            # Sort by time
            filtered_df = filtered_df.sort_values(by="datetime_utc").reset_index(
                drop=True
            )

            # Define output file path
            output_file = (
                processed_dir / f"{formatted_date}_{window['suffix']}_nearSG.csv"
            )

            # Save filtered data
            filtered_df.to_csv(output_file, index=False)
            logging.info(f"Saved {len(filtered_df):,} rows to: {output_file}")

            # Log some basic statistics
            unique_aircraft = filtered_df["icao"].nunique()
            time_range = (
                filtered_df["datetime_utc"].min().strftime("%Y-%m-%d %H:%M:%S UTC"),
                filtered_df["datetime_utc"].max().strftime("%Y-%m-%d %H:%M:%S UTC"),
            )
            logging.info(f"  - Unique aircraft: {unique_aircraft:,}")
            logging.info(f"  - Time range: {time_range[0]} to {time_range[1]}")

    except Exception as e:
        logging.error(f"Error processing {date_str}: {e}")
        raise


def main() -> None:
    """Command-line entry point."""
    parser = argparse.ArgumentParser(
        description="Perform time-based filtering on aircraft movement data.",
        epilog="Example: PYTHONPATH=src python src/data_eng/time_filtering.py 2025.02.08",
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
        logging.info(f"\nProcessing time filtering for {date_str}...")
        process_time_filtering(date_str)

    logging.info("\nAll time filtering complete.")


if __name__ == "__main__":
    main()
