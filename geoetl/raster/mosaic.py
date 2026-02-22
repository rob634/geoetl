"""MosaicJSON generation from a collection of COGs."""

import logging
from pathlib import Path

from cogeo_mosaic.mosaic import MosaicJSON

logger = logging.getLogger(__name__)


def create_mosaic_json(cog_paths: list[Path], output_path: Path) -> Path:
    """Create a MosaicJSON index from a list of COG files.

    Args:
        cog_paths: List of paths to Cloud Optimized GeoTIFFs.
        output_path: Path to write the MosaicJSON file.

    Returns:
        Path to the created MosaicJSON file.
    """
    if not cog_paths:
        raise ValueError("No COG paths provided")

    urls = [str(p) for p in cog_paths]
    mosaic = MosaicJSON.from_urls(urls)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(mosaic.model_dump_json())

    logger.info("MosaicJSON created: %s (%d COGs)", output_path.name, len(cog_paths))
    return output_path
