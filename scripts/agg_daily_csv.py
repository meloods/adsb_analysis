#!/usr/bin/env python3
"""Aggregate all per-aircraft CSV files for a given date into a single consolidated CSV."""

import argparse
import logging
from pathlib import Path
import pandas as pd
from typing import Generator, List, Optional

from config import BASE_DIR, BATCH_SIZE, CHUNK_SIZE
from utils import setup_logging, validate_date

setup_logging()


def read_csv_safely(csv_path: Path) -> Optional[pd.DataFrame]:
    """Safely read a CSV file with error handling."""
    try:
        # Read with basic error handling
        df = pd.read_csv(csv_path)

        # Check for abs_timestamp column
        if "abs_timestamp" not in df.columns:
            logging.warning(
                f"Missing 'abs_timestamp' column in {csv_path.name}, skipping file"
            )
            return None

        # Remove rows with missing abs_timestamp values
        initial_rows = len(df)
        df = df.dropna(subset=["abs_timestamp"])
        dropped_rows = initial_rows - len(df)

        if dropped_rows > 0:
            logging.warning(
                f"Dropped {dropped_rows} rows with missing abs_timestamp in {csv_path.name}"
            )

        if len(df) == 0:
            logging.warning(
                f"No valid rows remaining in {csv_path.name}, skipping file"
            )
            return None

        # Ensure abs_timestamp is numeric
        df["abs_timestamp"] = pd.to_numeric(df["abs_timestamp"], errors="coerce")
        df = df.dropna(subset=["abs_timestamp"])

        if len(df) == 0:
            logging.warning(
                f"No valid abs_timestamp values in {csv_path.name}, skipping file"
            )
            return None

        return df

    except pd.errors.EmptyDataError:
        logging.warning(f"Empty CSV file: {csv_path.name}")
        return None
    except pd.errors.ParserError as e:
        logging.error(f"Failed to parse CSV {csv_path.name}: {e}")
        return None
    except Exception as e:
        logging.error(f"Unexpected error reading {csv_path.name}: {e}")
        return None


def batch_csv_reader(
    csv_files: List[Path], batch_size: int = BATCH_SIZE
) -> Generator[pd.DataFrame, None, None]:
    """Generator that yields concatenated DataFrames from batches of CSV files."""
    for i in range(0, len(csv_files), batch_size):
        batch_files = csv_files[i : i + batch_size]
        batch_dataframes = []

        logging.info(
            f"Processing batch {i // batch_size + 1}: files {i + 1}-{min(i + batch_size, len(csv_files))} of {len(csv_files)}"
        )

        for csv_path in batch_files:
            df = read_csv_safely(csv_path)
            if df is not None:
                batch_dataframes.append(df)

        if batch_dataframes:
            # Concatenate batch and sort by abs_timestamp
            try:
                batch_df = pd.concat(batch_dataframes, ignore_index=True, sort=False)
                batch_df = batch_df.sort_values("abs_timestamp", ascending=True)
                yield batch_df
            except Exception as e:
                logging.error(
                    f"Failed to concatenate batch starting at file {i + 1}: {e}"
                )


def merge_sorted_dataframes(sorted_dfs: List[pd.DataFrame]) -> pd.DataFrame:
    """Merge multiple sorted DataFrames maintaining sort order using k-way merge."""
    if not sorted_dfs:
        return pd.DataFrame()

    if len(sorted_dfs) == 1:
        return sorted_dfs[0]

    # Use pandas concat for simplicity, then sort
    # For very large datasets, this could be optimized with a true k-way merge
    logging.info(f"Merging {len(sorted_dfs)} sorted batches")
    final_df = pd.concat(sorted_dfs, ignore_index=True, sort=False)
    final_df = final_df.sort_values("abs_timestamp", ascending=True)
    return final_df


def process_final_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Process the final DataFrame: add datetime column and reorder columns."""

    # Create datetime column from abs_timestamp
    try:
        df["datetime"] = pd.to_datetime(df["abs_timestamp"], unit="s", utc=True)
        logging.info("Created datetime column from abs_timestamp")
    except Exception as e:
        logging.error(f"Failed to create datetime column: {e}")
        # Create a fallback datetime column with NaT values
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
            logging.warning(f"Priority column '{col}' not found in DataFrame")

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
            logging.warning(f"Ending column '{col}' not found in DataFrame")

    # Reorder the DataFrame using reindex (always returns DataFrame)
    df = df.reindex(columns=ordered_columns)

    logging.info(
        f"Reordered columns: first 3 = {ordered_columns[:3]}, last 2 = {ordered_columns[-2:]}"
    )

    return df


def write_chunked_csv(
    df: pd.DataFrame, output_path: Path, chunk_size: int = CHUNK_SIZE
):
    """Write DataFrame to CSV in chunks to manage memory."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Write header first
    header_written = False

    for start_idx in range(0, len(df), chunk_size):
        end_idx = min(start_idx + chunk_size, len(df))
        chunk_df = df.iloc[start_idx:end_idx]

        # Write chunk
        chunk_df.to_csv(
            output_path,
            mode="a" if header_written else "w",
            header=not header_written,
            index=False,
        )
        header_written = True

        logging.info(f"Written rows {start_idx + 1}-{end_idx} of {len(df)}")


def aggregate_daily_csv(date_str: str, base_dir: Path = BASE_DIR) -> bool:
    """Aggregate all aircraft CSV files for a given date into a single file."""

    # Setup paths
    csv_dir = base_dir / date_str / "csv"
    processed_dir = base_dir / date_str / "processed"

    # Convert date format: YYYY.MM.DD -> YYYY_MM_DD
    output_filename = date_str.replace(".", "_") + ".csv"
    output_path = processed_dir / output_filename

    if not csv_dir.exists():
        logging.error(f"CSV directory does not exist: {csv_dir}")
        return False

    # Find all CSV files
    csv_files = list(csv_dir.glob("*.csv"))

    if not csv_files:
        logging.warning(f"No CSV files found in {csv_dir}")
        return False

    logging.info(f"Found {len(csv_files)} CSV files to aggregate for {date_str}")

    try:
        # Process files in batches to manage memory
        sorted_batches = []

        for batch_df in batch_csv_reader(csv_files, BATCH_SIZE):
            if not batch_df.empty:
                sorted_batches.append(batch_df)

        if not sorted_batches:
            logging.error("No valid data found in any CSV files")
            return False

        # Merge all sorted batches
        final_df = merge_sorted_dataframes(sorted_batches)

        if final_df.empty:
            logging.error("Final DataFrame is empty after merging")
            return False

        # Verify final sort order
        if not final_df["abs_timestamp"].is_monotonic_increasing:
            logging.warning("Final DataFrame not properly sorted, re-sorting...")
            final_df = final_df.sort_values("abs_timestamp", ascending=True)

        # Process final DataFrame: add datetime column and reorder columns
        final_df = process_final_dataframe(final_df)

        # Write output
        logging.info(f"Writing {len(final_df)} rows to {output_path}")

        if len(final_df) > CHUNK_SIZE:
            write_chunked_csv(final_df, output_path, CHUNK_SIZE)
        else:
            processed_dir.mkdir(parents=True, exist_ok=True)
            final_df.to_csv(output_path, index=False)

        # Summary statistics
        logging.info(
            f"Successfully aggregated {len(csv_files)} files into {output_path}"
        )
        logging.info(f"Total rows: {len(final_df)}, Columns: {len(final_df.columns)}")
        logging.info(
            f"Timestamp range: {final_df['datetime'].min()} to {final_df['datetime'].max()}"
        )

        return True

    except Exception as e:
        logging.error(f"Failed to aggregate CSV files for {date_str}: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Aggregate all per-aircraft CSV files for a given date into a single file",
        epilog="Example: python aggregate_daily_csv.py 2025.05.27",
    )
    parser.add_argument("date", type=validate_date, help="Date in YYYY.MM.DD format")

    args = parser.parse_args()

    logging.info(
        f"Starting aggregation for {args.date} (batch_size={BATCH_SIZE}, chunk_size={CHUNK_SIZE})"
    )

    success = aggregate_daily_csv(args.date)
    return 0 if success else 1


if __name__ == "__main__":
    exit(main())
