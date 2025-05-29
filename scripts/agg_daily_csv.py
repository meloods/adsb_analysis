#!/usr/bin/env python3
"""Aggregate all per-aircraft CSV files for a given date into a single consolidated CSV."""

import argparse
import logging
from pathlib import Path
import pandas as pd
from typing import Generator, List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

from config import BASE_DIR, BATCH_SIZE, CHUNK_SIZE, MAX_WORKERS
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


def aggregate_daily_csv(
    date_str: str, base_dir: Path = BASE_DIR, use_parallel: bool = True
) -> bool:
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

    import time

    start_time = time.time()

    try:
        # Process files in batches to manage memory
        sorted_batches = []

        # Choose processing method based on dataset size and user preference
        if use_parallel and len(csv_files) > BATCH_SIZE:
            logging.info(f"Using parallel processing with {MAX_WORKERS} workers")
            batch_reader = parallel_batch_csv_reader(csv_files, BATCH_SIZE)
        else:
            logging.info("Using sequential processing")
            batch_reader = batch_csv_reader(csv_files, BATCH_SIZE)

        for batch_df in batch_reader:
            if not batch_df.empty:
                sorted_batches.append(batch_df)

        processing_time = time.time() - start_time

        if not sorted_batches:
            logging.error("No valid data found in any CSV files")
            return False

        logging.info(f"Batch processing completed in {processing_time:.2f} seconds")

        # Merge all sorted batches
        merge_start = time.time()
        final_df = merge_sorted_dataframes(sorted_batches)
        merge_time = time.time() - merge_start

        if final_df.empty:
            logging.error("Final DataFrame is empty after merging")
            return False

        logging.info(f"Batch merging completed in {merge_time:.2f} seconds")

        # Verify final sort order
        if not final_df["abs_timestamp"].is_monotonic_increasing:
            logging.warning("Final DataFrame not properly sorted, re-sorting...")
            sort_start = time.time()
            final_df = final_df.sort_values("abs_timestamp", ascending=True)
            sort_time = time.time() - sort_start
            logging.info(f"Final sorting completed in {sort_time:.2f} seconds")

        # Process final DataFrame: add datetime column and reorder columns
        process_start = time.time()
        final_df = process_final_dataframe(final_df)
        process_time = time.time() - process_start
        logging.info(f"DataFrame processing completed in {process_time:.2f} seconds")

        # Write output
        write_start = time.time()
        logging.info(f"Writing {len(final_df)} rows to {output_path}")

        if len(final_df) > CHUNK_SIZE:
            write_chunked_csv(final_df, output_path, CHUNK_SIZE)
        else:
            processed_dir.mkdir(parents=True, exist_ok=True)
            final_df.to_csv(output_path, index=False)

        write_time = time.time() - write_start
        total_time = time.time() - start_time

        # Performance summary
        logging.info(f"Writing completed in {write_time:.2f} seconds")
        logging.info(f"\n{'=' * 50}")
        logging.info(f"AGGREGATION PERFORMANCE SUMMARY for {date_str}")
        logging.info(f"{'=' * 50}")
        logging.info(f"Files processed: {len(csv_files)}")
        logging.info(
            f"Processing method: {'Parallel' if use_parallel and len(csv_files) > BATCH_SIZE else 'Sequential'}"
        )
        if use_parallel and len(csv_files) > BATCH_SIZE:
            logging.info(f"Workers used: {MAX_WORKERS}")
        logging.info(f"Batch processing time: {processing_time:.2f}s")
        logging.info(f"Merging time: {merge_time:.2f}s")
        logging.info(f"Final processing time: {process_time:.2f}s")
        logging.info(f"Writing time: {write_time:.2f}s")
        logging.info(f"Total time: {total_time:.2f}s")
        logging.info(f"Rows: {len(final_df):,}, Columns: {len(final_df.columns)}")
        logging.info(f"Processing rate: {len(csv_files) / total_time:.1f} files/second")
        logging.info(f"Data rate: {len(final_df) / total_time:,.0f} rows/second")

        # Memory efficiency info
        if use_parallel and len(csv_files) > BATCH_SIZE:
            theoretical_sequential_time = processing_time * MAX_WORKERS
            speedup = (
                theoretical_sequential_time / processing_time
                if processing_time > 0
                else 1
            )
            logging.info(f"Estimated speedup: {speedup:.1f}x")

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
