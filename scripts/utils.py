#!/usr/bin/env python3
"""Common utility functions for aircraft data processing."""

import argparse
import logging
from datetime import datetime

from config import LOG_FORMAT, LOG_LEVEL


def setup_logging():
    """Set up logging configuration."""
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
