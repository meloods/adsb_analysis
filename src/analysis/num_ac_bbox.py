#!/usr/bin/env python3

import argparse
import pandas as pd
from shapely.geometry import Point, Polygon
from typing import List, Tuple
import re
from pathlib import Path


def dms_to_decimal(dms: str) -> float:
    match = re.match(r"(\d{2,3})(\d{2})(\d{2})([NSEW])", dms.strip().upper())
    if not match:
        raise ValueError(f"Invalid DMS format: {dms}")
    degrees, minutes, seconds, direction = match.groups()
    decimal = int(degrees) + int(minutes) / 60 + int(seconds) / 3600
    if direction in ["S", "W"]:
        decimal = -decimal
    return decimal


def parse_polygon_from_dms_file(bounds_file: Path) -> Polygon:
    df = pd.read_csv(bounds_file, header=None)
    coords: List[Tuple[float, float]] = []
    for dms_pair in df[0].dropna():
        lat_dms, lon_dms = dms_pair.strip().split()
        lat = dms_to_decimal(lat_dms)
        lon = dms_to_decimal(lon_dms)
        coords.append((lon, lat))
    if len(coords) < 3:
        raise ValueError("Need at least 3 coordinates to form a polygon.")
    if coords[0] != coords[-1]:
        coords.append(coords[0])
    return Polygon(coords)


def count_unique_aircraft_in_region(csv_file: Path, polygon: Polygon) -> int:
    df = pd.read_csv(
        csv_file, usecols=["datetime_utc", "icao", "latitude", "longitude"]
    )
    df.dropna(subset=["latitude", "longitude", "icao"], inplace=True)

    inside = df.apply(
        lambda row: polygon.contains(Point(row["longitude"], row["latitude"])), axis=1
    )

    unique_ids = df.loc[inside, "icao"].unique()
    print(f"✅ {len(unique_ids)} unique aircraft found within region.")
    return len(unique_ids)


def main():
    parser = argparse.ArgumentParser(
        description="Count unique aircraft within a polygonal region."
    )
    parser.add_argument("csv_file", type=Path, help="Path to input aircraft CSV file")
    parser.add_argument(
        "bounds_file",
        type=Path,
        help="Path to polygon bounds file (CSV, 1 coord pair per row)",
    )

    args = parser.parse_args()

    polygon = parse_polygon_from_dms_file(args.bounds_file)
    count_unique_aircraft_in_region(args.csv_file, polygon)


if __name__ == "__main__":
    main()
