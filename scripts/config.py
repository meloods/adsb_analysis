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
