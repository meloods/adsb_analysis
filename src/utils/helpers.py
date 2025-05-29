import yaml
from pathlib import Path
from typing import Any


def load_config(path: Path = Path("src/config/config.yaml")) -> dict[str, Any]:
    with path.open("r") as file:
        return yaml.safe_load(file)


def get_data_dir(config: dict[str, Any]) -> Path:
    return Path(config["paths"]["data_dir"]).resolve()


def get_processed_dir(config: dict[str, Any]) -> Path:
    return Path(config["paths"]["processed_dir"]).resolve()


def setup_logging() -> None:
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )


def validate_date(date_str: str) -> str:
    """
    Validate that the date string is in strict YYYY.MM.DD format
    with zero-padded two-digit month and day (e.g., 2024.01.09).

    Args:
        date_str: Date string provided via CLI or script.

    Returns:
        The original date string if valid.

    Raises:
        argparse.ArgumentTypeError: If the format is incorrect.
    """
    import argparse
    from datetime import datetime
    import re

    pattern = r"^\d{4}\.\d{2}\.\d{2}$"
    if not re.match(pattern, date_str):
        raise argparse.ArgumentTypeError(
            f"Invalid date format: '{date_str}'. Use YYYY.MM.DD with zero-padded month and day."
        )

    try:
        datetime.strptime(date_str, "%Y.%m.%d")
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"Invalid date: '{date_str}'. Ensure it represents a real calendar date in YYYY.MM.DD format."
        )

    return date_str
