"""Parallel raster operations: batch COG creation and tile extraction."""

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from geoetl.config import BatchConfig, COGConfig, COGQuality
from geoetl.batch.pool import parallel_map
from geoetl.raster.types import TileSpec

logger = logging.getLogger(__name__)


@dataclass
class _COGTask:
    input_path: Path
    output_path: Path
    quality: COGQuality
    config: COGConfig


def _create_cog_worker(task: _COGTask) -> Path:
    """Worker function for parallel COG creation (must be top-level for pickling)."""
    from geoetl.raster.cog import create_cog

    return create_cog(
        task.input_path,
        task.output_path,
        quality=task.quality,
        config=task.config,
    )


def batch_create_cogs(
    input_paths: list[Path],
    output_dir: Path,
    quality: COGQuality = COGQuality.ANALYSIS,
    config: Optional[BatchConfig] = None,
    cog_config: Optional[COGConfig] = None,
) -> list[Path]:
    """Create COGs from multiple rasters in parallel.

    Args:
        input_paths: List of input raster paths.
        output_dir: Directory for output COGs.
        quality: COG compression quality tier.
        config: Batch processing configuration.
        cog_config: COG creation configuration.

    Returns:
        List of paths to created COGs.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    cog_config = cog_config or COGConfig(quality=quality)

    tasks = [
        _COGTask(
            input_path=p,
            output_path=output_dir / f"{p.stem}_cog.tif",
            quality=quality,
            config=cog_config,
        )
        for p in input_paths
    ]

    return parallel_map(_create_cog_worker, tasks, config=config, desc="Creating COGs")


@dataclass
class _TileTask:
    raster_path: Path
    spec: TileSpec
    output_dir: Path


def _extract_tile_worker(task: _TileTask) -> Path:
    """Worker function for parallel tile extraction."""
    from geoetl.raster.tiling import extract_tile

    return extract_tile(task.raster_path, task.spec, task.output_dir)


def batch_extract_tiles(
    raster_path: Path,
    tile_specs: list[TileSpec],
    output_dir: Path,
    config: Optional[BatchConfig] = None,
) -> list[Path]:
    """Extract tiles from a raster in parallel.

    Args:
        raster_path: Path to the source raster.
        tile_specs: List of tile specifications.
        output_dir: Directory for output tiles.
        config: Batch processing configuration.

    Returns:
        List of paths to extracted tiles.
    """
    tasks = [
        _TileTask(raster_path=raster_path, spec=spec, output_dir=output_dir)
        for spec in tile_specs
    ]

    return parallel_map(_extract_tile_worker, tasks, config=config, desc="Extracting tiles")
