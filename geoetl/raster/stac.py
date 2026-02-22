"""STAC item and catalog generation from COGs."""

import logging
from pathlib import Path
from typing import Optional

from rio_stac.stac import create_stac_item as _rio_create_item

logger = logging.getLogger(__name__)


def create_stac_item(
    cog_path: Path,
    collection_id: str = "local",
    properties: Optional[dict] = None,
) -> dict:
    """Create a STAC item for a COG file.

    Args:
        cog_path: Path to a Cloud Optimized GeoTIFF.
        collection_id: STAC collection identifier.
        properties: Additional STAC properties.

    Returns:
        STAC item as a dictionary.
    """
    item = _rio_create_item(
        str(cog_path),
        collection=collection_id,
        properties=properties or {},
        with_proj=True,
        with_raster=True,
    )
    logger.info("STAC item created for: %s", cog_path.name)
    return item.to_dict()


def create_stac_catalog(
    items: list[dict],
    output_path: Path,
    catalog_id: str = "local-catalog",
    description: str = "Local geoetl STAC catalog",
) -> Path:
    """Write a minimal STAC catalog JSON referencing a list of items.

    Args:
        items: List of STAC item dictionaries.
        output_path: Path to write the catalog JSON.
        catalog_id: Catalog identifier.
        description: Catalog description.

    Returns:
        Path to the catalog JSON file.
    """
    import json

    catalog = {
        "type": "Catalog",
        "id": catalog_id,
        "stac_version": "1.0.0",
        "description": description,
        "links": [
            {"rel": "self", "href": str(output_path)},
            {"rel": "root", "href": str(output_path)},
        ],
    }

    # Write catalog
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(catalog, indent=2))

    # Write items alongside catalog
    items_dir = output_path.parent / "items"
    items_dir.mkdir(exist_ok=True)
    for item in items:
        item_id = item.get("id", "unknown")
        item_path = items_dir / f"{item_id}.json"
        item_path.write_text(json.dumps(item, indent=2))

    logger.info("STAC catalog created: %s (%d items)", output_path.name, len(items))
    return output_path
