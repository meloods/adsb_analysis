import csv
from pathlib import Path
from typing import Dict, Optional, TextIO


class CSVRowWriter:
    def __init__(self, output_path: Path):
        self.output_path = output_path
        self.file: Optional[TextIO] = None
        self.writer: Optional[csv.DictWriter] = None
        self.fieldnames: Optional[list[str]] = None
        self.rows_written = 0

    def write_row(self, row: Dict[str, any]) -> None:
        if self.writer is None:
            # Open file and initialize writer on first row
            self.fieldnames = list(row.keys())
            self.file = self.output_path.open("w", newline="", encoding="utf-8")
            self.writer = csv.DictWriter(self.file, fieldnames=self.fieldnames)
            self.writer.writeheader()

        self.writer.writerow(row)
        self.rows_written += 1

    def finalize(self) -> None:
        if self.file:
            self.file.close()
