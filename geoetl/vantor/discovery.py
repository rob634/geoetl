"""Vantor/Maxar Discovery API client (STAC-compliant)."""

import requests

from .config import MAXAR_BASE_URL, maxar_headers

DISCOVERY_URL = f"{MAXAR_BASE_URL}/discovery/v1"


def list_collections() -> list[dict]:
    """List all available collections."""
    r = requests.get(f"{DISCOVERY_URL}/collections", headers=maxar_headers())
    r.raise_for_status()
    return r.json()["collections"]


def get_queryables(collection_id: str = "cloud-optimized-archive") -> dict:
    """Get filterable fields for a collection."""
    r = requests.get(
        f"{DISCOVERY_URL}/collections/{collection_id}/queryables",
        headers=maxar_headers(),
    )
    r.raise_for_status()
    return r.json()


def search(
    bbox: list[float] | None = None,
    intersects: dict | None = None,
    datetime_range: str | None = None,
    collections: list[str] | None = None,
    limit: int = 10,
    cql2_filter: dict | None = None,
) -> dict:
    """
    Search the STAC catalog.

    Args:
        bbox: [west, south, east, north] in EPSG:4326
        intersects: GeoJSON geometry (alternative to bbox — mutually exclusive)
        datetime_range: ISO-8601 range string, e.g. "2024-01-01T00:00:00Z/2024-12-31T23:59:59Z"
        collections: list of collection IDs (default: cloud-optimized-archive)
        limit: max results per page
        cql2_filter: CQL2-JSON filter dict

    Returns:
        GeoJSON FeatureCollection of STAC items
    """
    body: dict = {
        "collections": collections or ["cloud-optimized-archive"],
        "limit": limit,
    }
    if bbox:
        body["bbox"] = bbox
    if intersects:
        body["intersects"] = intersects
    if datetime_range:
        body["datetime"] = datetime_range
    if cql2_filter:
        body["filter-lang"] = "cql2-json"
        body["filter"] = cql2_filter

    r = requests.post(
        f"{DISCOVERY_URL}/search",
        headers={**maxar_headers(), "Content-Type": "application/json"},
        json=body,
    )
    r.raise_for_status()
    return r.json()


def search_low_cloud(
    bbox: list[float],
    datetime_range: str,
    max_cloud: int = 15,
    max_off_nadir: int = 25,
    limit: int = 10,
) -> dict:
    """Convenience: search for clear, near-nadir imagery."""
    return search(
        bbox=bbox,
        datetime_range=datetime_range,
        limit=limit,
        cql2_filter={
            "op": "and",
            "args": [
                {"op": "<=", "args": [{"property": "eo:cloud_cover"}, max_cloud]},
                {"op": "<=", "args": [{"property": "view:off_nadir"}, max_off_nadir]},
            ],
        },
    )
