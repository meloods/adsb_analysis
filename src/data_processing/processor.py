import gzip
import json
import logging
from pathlib import Path
from typing import Any, Dict, List

from data_processing.transformer import flatten_trace_entry
from data_processing.writer import CSVRowWriter


def flatten_all_json_to_csv(
    input_dir: Path, output_csv: Path, include_metadata: bool
) -> None:
    """Flatten all aircraft trace JSON files into a single CSV."""
    writer = CSVRowWriter(output_csv)

    num_files = 0
    num_rows = 0

    for file_path in input_dir.glob("trace_full_*.json"):
        num_files += 1
        try:
            with gzip.open(file_path, "rt", encoding="utf-8") as f:
                data = json.load(f)

            icao = data.get("icao")
            timestamp = data.get("timestamp")
            trace = data.get("trace", [])

            top_level_metadata = {
                "icao": icao,
                "timestamp": timestamp,
                "r": data.get("r"),
                "t": data.get("t"),
                "desc": data.get("desc"),
                "dbFlags": data.get("dbFlags"),
            }

            for entry in trace:
                flattened = flatten_trace_entry(
                    entry, top_level_metadata, include_metadata
                )
                writer.write_row(flattened)
                num_rows += 1

        except Exception as e:
            logging.warning(f"❌ Failed to process {file_path.name}: {e}")
            continue

    writer.finalize()
    logging.info(
        f"✅ Flattened {num_files} files, wrote {num_rows} rows to: {output_csv}"
    )
