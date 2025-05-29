from typing import Any, Dict, List, Optional


TRACE_COLUMNS = [
    "seconds_after_timestamp",
    "latitude",
    "longitude",
    "altitude_ft",
    "ground_speed_kts",
    "track_deg",
    "flags_bitfield",
    "vertical_rate_fpm",
    "aircraft_metadata",
    "source_type",
    "geometric_altitude_ft",
    "geometric_vertical_rate_fpm",
    "indicated_airspeed_kts",
    "roll_angle_deg",
]


def flatten_trace_entry(
    trace_entry: List[Any], metadata: Dict[str, Any], include_metadata: bool
) -> Dict[str, Any]:
    """Flatten one trace entry and attach top-level metadata."""
    row: Dict[str, Any] = {}

    # --- Flatten core trace fields ---
    for idx, column in enumerate(TRACE_COLUMNS):
        if column == "aircraft_metadata" and not include_metadata:
            continue
        row[column] = trace_entry[idx] if idx < len(trace_entry) else None

    # --- Expand aircraft_metadata if requested ---
    if include_metadata:
        meta_dict: Optional[Dict[str, Any]] = (
            trace_entry[8] if len(trace_entry) > 8 else None
        )
        if isinstance(meta_dict, dict):
            for key, value in meta_dict.items():
                row[f"metadata.{key}"] = value
        row.pop("aircraft_metadata", None)  # remove nested dict

    # --- Attach top-level metadata ---
    for meta_key, meta_val in metadata.items():
        row[meta_key] = meta_val

    return row
