#!/usr/bin/env python3
"""
Download aircraft data files from GitHub releases by date.
"""

import argparse
import logging
import re
from pathlib import Path
from typing import List

import requests
from tqdm import tqdm

from utils import setup_logging, validate_date, load_config, get_data_dir

# --- Setup ---
setup_logging()
config = load_config()
DATA_DIR: Path = get_data_dir(config)

# --- Constants ---
VALID_SUFFIXES: list[str] = [".tar"] + [
    f".tar.{chr1}{chr2}"
    for chr1 in "abcdefghijklmnopqrstuvwxyz"
    for chr2 in "abcdefghijklmnopqrstuvwxyz"
]
IGNORED_SUFFIXES: list[str] = [".tar.gz", ".zip"]


def get_asset_urls(release_url: str, tag: str) -> List[str]:
    """
    Extract downloadable asset URLs from the GitHub expanded_assets page for a given tag.

    Args:
        release_url: Full URL to the GitHub expanded_assets HTML page.
        tag: The release tag used to identify relevant assets.

    Returns:
        List of full GitHub URLs pointing to downloadable asset files.
    """
    logging.info(f"Fetching asset list from: {release_url}")
    response = requests.get(release_url)
    if response.status_code != 200:
        logging.warning(f"Failed to fetch asset list: {response.status_code}")
        return []

    matches = re.findall(
        rf'href=["\'](/.*?{re.escape(tag)}.*?)["\']',
        response.text,
        re.IGNORECASE,
    )
    asset_paths = list(set(matches))  # Deduplicate

    asset_urls: list[str] = []
    for path in asset_paths:
        if any(path.endswith(suffix) for suffix in IGNORED_SUFFIXES):
            continue
        if any(path.endswith(suffix) for suffix in VALID_SUFFIXES):
            asset_urls.append(f"https://github.com{path}")

    return asset_urls


def download_file(url: str, dest_dir: Path) -> None:
    """
    Download a single file from a URL to a target directory, with progress bar.

    Args:
        url: URL of the file to download.
        dest_dir: Target directory where the file will be saved.
    """
    dest_file = dest_dir / Path(url).name

    logging.info(f"Downloading: {Path(url).name}")
    response = requests.get(url, stream=True)
    if response.status_code != 200:
        logging.error(f"Failed to download {url}: {response.status_code}")
        return

    total_size = int(response.headers.get("content-length", 0))
    progress = tqdm(
        total=total_size,
        unit="B",
        unit_scale=True,
        unit_divisor=1024,
        desc=dest_file.name,
    )

    with open(dest_file, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)
                progress.update(len(chunk))
    progress.close()
    logging.info(f"Saved to: {dest_file}")


def download_for_date(date_str: str, base_download_dir: Path = DATA_DIR) -> None:
    """
    Download all relevant GitHub release files for a given date.

    Args:
        date_str: Date in YYYY.MM.DD format (e.g., '2025.05.28').
        base_download_dir: Base directory to store downloads.
    """
    year = int(date_str.split(".")[0])
    if year not in (2024, 2025):
        logging.warning(f"Unsupported year: {year}")
        return

    repo = f"globe_history_{year}"
    release_tag = f"v{date_str}-planes-readsb-prod-0"
    release_url = (
        f"https://github.com/adsblol/{repo}/releases/expanded_assets/{release_tag}"
    )

    dest_dir = base_download_dir / date_str / "downloaded"
    dest_dir.mkdir(parents=True, exist_ok=True)

    asset_urls = get_asset_urls(release_url, release_tag)
    if not asset_urls:
        logging.warning(
            f"No downloadable assets found at {release_url} for tag: {release_tag}"
        )
        return

    for asset_url in asset_urls:
        download_file(asset_url, dest_dir)


def main() -> None:
    """Command-line entry point."""
    parser = argparse.ArgumentParser(
        description="Download aircraft data files from GitHub releases by date.",
        epilog="Example: PYTHONPATH=src python src/data_eng/download.py 2024.12.21 2025.01.11",
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
        logging.info(f"\nDownloading data from {date_str}...")
        download_for_date(date_str)

    logging.info("\nAll downloads complete.")


if __name__ == "__main__":
    main()
