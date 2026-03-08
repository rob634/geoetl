"""AOI-specific cloud cover estimation for Vantor/Maxar scenes.

Maxar's eo:cloud_cover is strip-level (entire image strip, often 50+ km).
This module estimates cloud cover within a custom AOI by fetching browse
COGs and analyzing pixel brightness/saturation.

Two-stage approach:
    1. Discovery pre-filter: strip cloud <= 30%, off-nadir <= 25° (generous)
    2. Browse COG pixel analysis: download browse COG, clip to AOI, detect clouds

Cloud detection heuristic:
    Pixel is "cloud" if mean brightness > 180 AND HSV saturation < 0.2
    (clouds are bright + grayish-white; vegetation/buildings are bright but saturated)

Gotchas:
    - Browse assets use Range headers — endpoint returns 400 without them
    - GDAL vsicurl cannot open these URLs (encoded query params break it)
    - Workaround: download full COG to temp file, open with rasterio locally
"""

import os
import tempfile

import numpy as np
import requests
import rasterio
from rasterio.windows import from_bounds
from shapely.geometry import box, shape

from .config import maxar_headers


def _fetch_browse_cog(browse_url: str, timeout: int = 30) -> str:
    """Download a browse COG via range request. Returns path to temp file.

    Falls back to a plain GET if the server returns 200 instead of 206
    (no content-range header).
    """
    headers = maxar_headers()

    # Probe total size via single-byte range request
    r = requests.get(browse_url, headers={**headers, "Range": "bytes=0-0"}, timeout=timeout)
    r.raise_for_status()

    content_range = r.headers.get("content-range")
    if content_range:
        total = int(content_range.split("/")[-1])
        r = requests.get(
            browse_url, headers={**headers, "Range": f"bytes=0-{total - 1}"}, timeout=timeout,
        )
        r.raise_for_status()
    else:
        # Server returned 200 (full content) instead of 206 — use it directly,
        # or fall back to a plain GET without Range header.
        if len(r.content) <= 1:
            r = requests.get(browse_url, headers=headers, timeout=timeout)
            r.raise_for_status()

    tmp = tempfile.NamedTemporaryFile(suffix=".tif", delete=False)
    tmp.write(r.content)
    tmp.close()
    return tmp.name


def estimate_cloud_cover(
    browse_url: str,
    bbox: list[float] | None = None,
    geometry: dict | None = None,
    bright_thresh: int = 180,
    sat_thresh: float = 0.2,
) -> dict:
    """
    Estimate cloud cover percentage within an AOI for a single scene.

    Args:
        browse_url: URL to the scene's browse COG asset.
        bbox: [west, south, east, north] — simple rectangular AOI.
        geometry: GeoJSON geometry dict — arbitrary polygon AOI.
            Provide bbox OR geometry, not both.
        bright_thresh: Mean RGB brightness above which a pixel is
            considered cloud-candidate (0-255). Default 180.
        sat_thresh: HSV saturation below which a bright pixel is
            considered cloud (clouds are bright + gray). Default 0.2.

    Returns:
        dict with cloud_pct, valid_pixels, total_pixels, mean_brightness, coverage_pct
    """
    if bbox is None and geometry is None:
        raise ValueError("Provide bbox or geometry")

    if geometry is not None:
        aoi = shape(geometry)
        west, south, east, north = aoi.bounds
    else:
        west, south, east, north = bbox

    tmp_path = _fetch_browse_cog(browse_url)
    try:
        with rasterio.open(tmp_path) as ds:
            scene_box = box(*ds.bounds)
            aoi_box = box(west, south, east, north)
            if not scene_box.intersects(aoi_box):
                return {
                    "cloud_pct": None,
                    "valid_pixels": 0,
                    "total_pixels": 0,
                    "mean_brightness": None,
                    "coverage_pct": 0.0,
                }

            window = from_bounds(west, south, east, north, ds.transform)
            data = ds.read(window=window)

            if data.size == 0 or ds.count < 3:
                return {
                    "cloud_pct": None,
                    "valid_pixels": 0,
                    "total_pixels": 0,
                    "mean_brightness": None,
                    "coverage_pct": 0.0,
                }

            rgb = data[:3].astype(np.float32)
            nodata = np.all(rgb == 0, axis=0)
            valid = ~nodata
            total_pixels = valid.size
            valid_count = int(valid.sum())

            if valid_count == 0:
                return {
                    "cloud_pct": None,
                    "valid_pixels": 0,
                    "total_pixels": total_pixels,
                    "mean_brightness": None,
                    "coverage_pct": 0.0,
                }

            r_ch, g_ch, b_ch = rgb[0][valid], rgb[1][valid], rgb[2][valid]
            brightness = (r_ch + g_ch + b_ch) / 3.0
            max_ch = np.maximum(np.maximum(r_ch, g_ch), b_ch)
            min_ch = np.minimum(np.minimum(r_ch, g_ch), b_ch)
            saturation = np.where(max_ch > 0, (max_ch - min_ch) / max_ch, 0)

            cloud_mask = (brightness > bright_thresh) & (saturation < sat_thresh)
            cloud_pct = float(cloud_mask.sum()) / valid_count * 100.0

            return {
                "cloud_pct": round(cloud_pct, 1),
                "valid_pixels": valid_count,
                "total_pixels": total_pixels,
                "mean_brightness": round(float(brightness.mean()), 1),
                "coverage_pct": round(valid_count / total_pixels * 100.0, 1),
            }
    finally:
        os.unlink(tmp_path)


def filter_by_aoi_cloud(
    features: list[dict],
    bbox: list[float] | None = None,
    geometry: dict | None = None,
    max_aoi_cloud: float = 15,
    bright_thresh: int = 180,
    sat_thresh: float = 0.2,
) -> list[tuple[dict, dict]]:
    """
    Filter STAC search results by actual cloud cover within an AOI.

    Returns:
        list of (feature, cloud_info) tuples for scenes passing the filter.
    """
    passed = []
    for i, feat in enumerate(features):
        fid = feat["id"]
        props = feat["properties"]
        strip_cloud = props.get("eo:cloud_cover", "?")

        browse_url = feat.get("assets", {}).get("browse", {}).get("href")
        if not browse_url:
            print(f"  [{i+1}/{len(features)}] {fid} — no browse asset, skipping")
            continue

        print(
            f"  [{i+1}/{len(features)}] {fid} | {props.get('vehicle_name', '?')} | "
            f"strip cloud: {strip_cloud}% — checking AOI...",
            end="",
            flush=True,
        )

        try:
            info = estimate_cloud_cover(
                browse_url,
                bbox=bbox,
                geometry=geometry,
                bright_thresh=bright_thresh,
                sat_thresh=sat_thresh,
            )
        except Exception as e:
            print(f" ERROR: {e}")
            continue

        aoi_cloud = info["cloud_pct"]
        if aoi_cloud is None:
            print(" no coverage")
            continue

        status = "PASS" if aoi_cloud <= max_aoi_cloud else "FAIL"
        print(f" AOI cloud: {aoi_cloud}% [{status}]")

        if aoi_cloud <= max_aoi_cloud:
            passed.append((feat, info))

    return passed


def search_clear_aoi(
    bbox: list[float] | None = None,
    geometry: dict | None = None,
    datetime_range: str | None = None,
    collections: list[str] | None = None,
    max_strip_cloud: int = 30,
    max_aoi_cloud: float = 15,
    max_off_nadir: int = 25,
    limit: int = 20,
) -> list[tuple[dict, dict]]:
    """
    End-to-end: search for imagery that is actually clear over your AOI.

    Two-stage filter:
        1. Discovery API pre-filter (strip-level cloud + off-nadir)
        2. Browse COG pixel analysis (AOI-level cloud)

    Returns:
        list of (feature, cloud_info) tuples for clear scenes
    """
    from .discovery import search

    search_bbox = None if geometry is not None else bbox

    cql_args = [
        {"op": "<=", "args": [{"property": "eo:cloud_cover"}, max_strip_cloud]},
        {"op": "<=", "args": [{"property": "view:off_nadir"}, max_off_nadir]},
    ]

    results = search(
        bbox=search_bbox,
        intersects=geometry,
        datetime_range=datetime_range,
        collections=collections,
        limit=limit,
        cql2_filter={"op": "and", "args": cql_args},
    )

    features = results.get("features", [])
    print(f"Discovery returned {len(features)} candidates (strip cloud <= {max_strip_cloud}%)")

    if not features:
        return []

    return filter_by_aoi_cloud(
        features,
        bbox=bbox,
        geometry=geometry,
        max_aoi_cloud=max_aoi_cloud,
    )
