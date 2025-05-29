#!/usr/bin/env python3
"""
Extract aircraft data .tar files (including split .tar.aa, .tar.ab, etc.) by date.
"""

import argparse
import logging
import subprocess
from pathlib import Path
from typing import List, Dict

from tqdm import tqdm

from utils import setup_logging, validate_date, load_config, get_data_dir

# --- Setup ---
setup_logging()
config = load_config()
DATA_DIR: Path = get_data_dir(config)


def find_tar_groups(download_dir: Path) -> List[List[Path]]:
    """
    Group .tar and .tar.* parts by base name.

    Args:
        download_dir: Directory to search for .tar and split parts.

    Returns:
        List of grouped paths, each representing a single archive to extract.
    """
    all_parts = sorted(download_dir.glob("*.tar*"))
    grouped: Dict[str, List[Path]] = {}

    for file in all_parts:
        base = file.name.split(".tar")[0]
        grouped.setdefault(base, []).append(file)

    return [sorted(group) for group in grouped.values()]


def extract_tar_group(parts: List[Path], extract_dir: Path) -> None:
    """
    Extract a tar group (either standalone .tar or split parts) to a directory.

    Args:
        parts: List of tar-related files for a single archive.
        extract_dir: Destination directory for extraction.
    """
    extract_dir.mkdir(parents=True, exist_ok=True)

    if len(parts) == 1 and parts[0].suffix == ".tar":
        logging.info(f"Extracting standalone archive: {parts[0].name}")
        subprocess.run(
            ["tar", "-xf", str(parts[0]), "-C", str(extract_dir)],
            check=True,
        )
    else:
        logging.info(f"Extracting multipart archive: {' '.join(p.name for p in parts)}")
        cat_cmd = ["cat"] + [str(p) for p in parts]
        proc_cat = subprocess.Popen(cat_cmd, stdout=subprocess.PIPE)

        try:
            proc_tar = subprocess.Popen(
                ["tar", "-xf", "-", "-C", str(extract_dir)],
                stdin=proc_cat.stdout,
            )

            if proc_cat.stdout:
                proc_cat.stdout.close()  # Avoid broken pipe

            proc_tar.communicate()

            if proc_tar.returncode != 0:
                raise RuntimeError(
                    f"Extraction failed for group: {[p.name for p in parts]}"
                )
        finally:
            proc_cat.terminate()


def extract_for_date(date_str: str, base_data_dir: Path = DATA_DIR) -> None:
    """
    Extract all .tar or split tar files for a given date.

    Args:
        date_str: Date in YYYY.MM.DD format.
        base_data_dir: Base path to the data directory.
    """
    date_path = base_data_dir / date_str
    download_dir = date_path / "downloaded"
    extract_dir = date_path / "extracted"

    if not download_dir.exists():
        logging.warning(f"Download folder missing for {date_str}, skipping.")
        return

    groups = find_tar_groups(download_dir)
    if not groups:
        logging.warning(f"No .tar or .tar.* files found for {date_str}")
        return

    for group in tqdm(groups, desc=f"Extracting {date_str}", unit="group"):
        try:
            extract_tar_group(group, extract_dir)
        except Exception as e:
            logging.error(f"Failed to extract {group}: {e}")


def main() -> None:
    """Command-line entry point."""
    parser = argparse.ArgumentParser(
        description="Extract .tar or split .tar.* files by date.",
        epilog="Example: PYTHONPATH=src python src/data_eng/extract.py 2025.01.01 2025.01.02",
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
        logging.info(f"\nExtracting files for {date_str}...")
        extract_for_date(date_str)

    logging.info("\nAll extractions complete.")


if __name__ == "__main__":
    main()
