#!/usr/bin/env python3

import argparse
from pathlib import Path
import pandas as pd
import folium
from folium.plugins import MarkerCluster
from matplotlib.cm import get_cmap
from matplotlib.colors import to_hex
import re


def dms_to_decimal(dms: str) -> float:
    """Convert a DMS coordinate (e.g., '013000N') to decimal degrees."""
    match = re.match(r"(\d{2,3})(\d{2})(\d{2})([NSEW])", dms.strip().upper())
    if not match:
        raise ValueError(f"Invalid DMS format: {dms}")
    degrees, minutes, seconds, direction = match.groups()
    decimal = int(degrees) + int(minutes) / 60 + int(seconds) / 3600
    if direction in ["S", "W"]:
        decimal = -decimal
    return decimal


def parse_dms_polygon(csv_path: Path) -> list:
    """Reads a CSV with DMS coordinate rows and returns list of [lat, lon] pairs."""
    df = pd.read_csv(csv_path, header=None)
    coords = []
    for raw in df[0].dropna():
        lat_dms, lon_dms = raw.strip().split()
        lat = dms_to_decimal(lat_dms)
        lon = dms_to_decimal(lon_dms)
        coords.append([lat, lon])
    if coords[0] != coords[-1]:
        coords.append(coords[0])  # Close the polygon
    return coords


def generate_color_map(n: int):
    cmap = get_cmap("tab20")
    return [to_hex(cmap(i % 20)) for i in range(n)]


def plot_aircraft_movements_folium(
    csv_file: Path,
    output_html: Path = Path("aircraft_tracks_map.html"),
    bbox_files: list = None,
):
    df = pd.read_csv(csv_file, parse_dates=["datetime_utc"])
    df.dropna(subset=["icao", "latitude", "longitude"], inplace=True)

    if df.empty:
        print("No valid data to plot.")
        return

    # Center map on median coordinates
    center_lat = df["latitude"].median()
    center_lon = df["longitude"].median()
    fmap = folium.Map(
        location=[center_lat, center_lon], zoom_start=6, tiles="CartoDB positron"
    )

    # Plot aircraft tracks
    icao_list = df["icao"].unique()
    colors = generate_color_map(len(icao_list))

    for idx, icao in enumerate(icao_list):
        aircraft = df[df["icao"] == icao].sort_values("datetime_utc")
        coords = list(zip(aircraft["latitude"], aircraft["longitude"]))
        popup_text = f"ICAO: {icao}<br>Points: {len(coords)}"

        folium.PolyLine(
            locations=coords,
            color=colors[idx],
            weight=3,
            opacity=0.8,
            popup=popup_text,
            tooltip=icao,
        ).add_to(fmap)

        # Optional: add markers at start/end
        folium.CircleMarker(coords[0], radius=4, color=colors[idx], fill=True).add_to(
            fmap
        )
        folium.CircleMarker(coords[-1], radius=4, color=colors[idx], fill=True).add_to(
            fmap
        )

    # Add bounding box polygons
    if bbox_files:
        for bbox_file in bbox_files:
            try:
                polygon_coords = parse_dms_polygon(Path(bbox_file))
                folium.Polygon(
                    locations=polygon_coords,
                    color="red",
                    weight=2,
                    fill=True,
                    fill_opacity=0.1,
                    tooltip=bbox_file.stem,
                ).add_to(fmap)
            except Exception as e:
                print(f"⚠️ Failed to add polygon from {bbox_file}: {e}")

    fmap.save(str(output_html))
    print(f"✅ Map saved to {output_html.resolve()}")


def main():
    parser = argparse.ArgumentParser(
        description="Plot aircraft movements and shaded polygons on a folium map."
    )
    parser.add_argument(
        "csv_file", type=Path, help="Path to aircraft movement CSV file"
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("aircraft_tracks_map.html"),
        help="Output HTML file",
    )
    parser.add_argument(
        "--bbox",
        type=Path,
        nargs="*",
        help="Optional list of bounding box files to overlay",
    )

    args = parser.parse_args()
    plot_aircraft_movements_folium(args.csv_file, args.output, args.bbox)


if __name__ == "__main__":
    main()
