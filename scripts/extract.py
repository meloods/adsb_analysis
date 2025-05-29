#!/usr/bin/env python3
"""Extract .tar or split .tar.* files into target folders."""

import argparse
import logging
import subprocess
from datetime import datetime
from pathlib import Path
from typing import List, Dict

BASE_DIR = Path("data")


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
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


def find_tar_parts(download_dir: Path) -> List[List[Path]]:
    """
    Identify grouped tar parts:
    - Return a list of groups: each group is either a [single .tar] or [.tar.aa, .tar.ab, ...]
    """
    all_parts = sorted(download_dir.glob("*.tar*"))

    grouped: Dict[str, List[Path]] = {}
    for f in all_parts:
        base = f.name.split(".tar")[0]
        grouped.setdefault(base, []).append(f)

    return [sorted(files) for files in grouped.values()]


def extract_tar_group(parts: List[Path], extract_dir: Path):
    extract_dir.mkdir(parents=True, exist_ok=True)
    output_path = extract_dir

    if len(parts) == 1 and parts[0].suffix == ".tar":
        logging.info(f"Extracting standalone tar: {parts[0].name}")
        subprocess.run(
            ["tar", "-xf", str(parts[0]), "-C", str(output_path)],
            check=True,
        )
    else:
        # Multiple parts (e.g., .tar.aa .tar.ab ...)
        cat_cmd = ["cat"] + [str(p) for p in parts]
        logging.info(f"Extracting split archive: {' '.join(p.name for p in parts)}")

        proc_cat = subprocess.Popen(cat_cmd, stdout=subprocess.PIPE)
        try:
            proc_tar = subprocess.Popen(
                ["tar", "-xf", "-", "-C", str(output_path)],
                stdin=proc_cat.stdout,
            )

            if proc_cat.stdout:
                proc_cat.stdout.close()  # allow cat to receive SIGPIPE if tar exits

            proc_tar.communicate()

            if proc_tar.returncode != 0:
                raise RuntimeError(f"Extraction failed for group: {parts}")
        finally:
            proc_cat.terminate()


def extract_for_date(date_str: str, base_dir: Path = BASE_DIR):
    base_path = base_dir / date_str
    download_dir = base_path / "downloaded"
    extract_dir = base_path / "extracted"

    if not download_dir.exists():
        logging.warning(f"No downloads found for {date_str}, skipping.")
        return

    groups = find_tar_parts(download_dir)
    if not groups:
        logging.warning(f"No .tar files found in {download_dir}")
        return

    for group in groups:
        try:
            extract_tar_group(group, extract_dir)
        except Exception as e:
            logging.error(f"Failed to extract {group}: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Extract .tar or split .tar.* files by date",
        epilog="Example: python extract.py 2024.12.21 2025.01.11",
    )
    parser.add_argument(
        "dates",
        nargs="+",
        type=validate_date,
        help="One or more dates in YYYY.MM.DD format",
    )

    args = parser.parse_args()

    for date_str in args.dates:
        logging.info(f"\nExtracting data from {date_str}...")
        extract_for_date(date_str)

    logging.info("\nExtraction complete.")
