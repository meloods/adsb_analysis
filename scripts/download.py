import argparse
import re
import logging
import requests
from pathlib import Path
from tqdm import tqdm
from typing import List

from config import BASE_DIR
from utils import setup_logging, validate_date

setup_logging()


def get_asset_urls(release_url: str, tag: str) -> List[str]:
    logging.info(f"Fetching asset list from: {release_url}")
    response = requests.get(release_url)
    if response.status_code != 200:
        logging.warning(f"Failed to fetch asset list: {response.status_code}")
        return []

    # Extract href links containing the tag
    matches = re.findall(
        rf'href=["\'](/.*?{re.escape(tag)}.*?)["\']', response.text, re.IGNORECASE
    )
    asset_paths = list(set(matches))  # Remove duplicates

    # Allowed suffixes: .tar and .tar.aa to .tar.zz
    valid_suffixes = [".tar"] + [
        f".tar.{chr1}{chr2}"
        for chr1 in "abcdefghijklmnopqrstuvwxyz"
        for chr2 in "abcdefghijklmnopqrstuvwxyz"
    ]

    # Explicitly ignore these unwanted suffixes - these belong to the source code files
    ignored_suffixes = [".tar.gz", ".zip"]

    # Filter assets: must match a valid suffix, and not an ignored suffix
    asset_urls = []
    for path in asset_paths:
        if any(path.endswith(ignored) for ignored in ignored_suffixes):
            continue
        if any(path.endswith(valid) for valid in valid_suffixes):
            asset_urls.append(f"https://github.com{path}")

    return asset_urls


def download_file(url: str, dest_dir: Path):
    dest_file = dest_dir / Path(url).name
    if dest_file.exists():
        logging.info(f"Already downloaded: {dest_file.name}")
        return

    logging.info(f"Downloading: {url.split('/')[-1]}")
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


def download_for_date(date_str: str, base_download_dir: Path = BASE_DIR):
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
        logging.warning(f"No assets found for {release_tag}")
        return

    for asset_url in asset_urls:
        download_file(asset_url, dest_dir)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Download aircraft data files from GitHub releases by date",
        epilog="Example: python download_data.py 2024.12.21 2025.01.11",
    )
    parser.add_argument(
        "dates",
        nargs="+",
        type=validate_date,
        help="One or more dates in YYYY.MM.DD format",
    )

    args = parser.parse_args()

    for date_str in args.dates:
        logging.info(f"\nDownloading data from {date_str}...")
        download_for_date(date_str)

    logging.info("\nDownloading complete.")
