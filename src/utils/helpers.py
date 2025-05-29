import yaml
from pathlib import Path
from typing import Any


def load_config(path: Path = Path("src/config/config.yaml")) -> dict[str, Any]:
    with path.open("r") as file:
        return yaml.safe_load(file)


def get_data_dir(config: dict[str, Any]) -> Path:
    return Path(config["paths"]["data_dir"]).resolve()


def setup_logging() -> None:
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )


def validate_date(date_str: str) -> str:
    from datetime import datetime
    import argparse

    try:
        datetime.strptime(date_str, "%Y.%m.%d")
        return date_str
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"Invalid date format: {date_str}. Use YYYY.MM.DD."
        )
