#!/usr/bin/env python3
"""Configuration constants for aircraft data processing."""

from pathlib import Path

# Base directory for all data operations
BASE_DIR = Path("data")

# Logging configuration
LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
LOG_LEVEL = "INFO"

# CSV aggregation configuration
BATCH_SIZE = 50  # Number of files to process in each batch
CHUNK_SIZE = 10000  # Rows per chunk when reading/writing large files

# Parallel processing configuration
MAX_WORKERS = 4  # Number of parallel threads/processes

# Directory structure constants
SUBDIR_TRACES = "traces"  # Decompressed JSON files
SUBDIR_CSV = "csv"  # Individual CSV files
SUBDIR_PROCESSED = "processed"  # Final aggregated CSV
SUBDIR_EXTRACTED = "extracted"  # Extracted but compressed files
SUBDIR_DOWNLOADED = "downloaded"  # Raw downloaded files

# File processing constants
TRACE_FILE_PREFIX = "trace_full_"  # Expected prefix for trace files
