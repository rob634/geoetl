"""Parallel raster operations: batch COG creation, tile extraction, merging, and zonal stats."""

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import geopandas as gpd

from geoetl.config import BatchConfig, COGConfig, COGQuality
from geoetl.batch.pool import parallel_map
from geoetl.raster.types import CogMergeResult, TileSpec
from geoetl.raster.zonal import AggMethod, ZonalResult

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


# --- Batch COG merge from VRT ---


@dataclass
class _MergeTask:
    key: str
    input_paths: list[Path]
    output_path: Path
    compression: str


def _merge_cog_worker(task: _MergeTask) -> CogMergeResult:
    """Worker function for parallel COG merging (must be top-level for pickling)."""
    from geoetl.raster.cog import create_cog_from_vrt

    return create_cog_from_vrt(
        task.input_paths,
        task.output_path,
        compression=task.compression,
    )


def batch_merge_cogs(
    groups: dict[str, list[Path]],
    output_dir: Path,
    compression: str = "DEFLATE",
    config: Optional[BatchConfig] = None,
) -> list[CogMergeResult]:
    """Merge multiple groups of rasters into COGs in parallel.

    Each group key becomes the output filename (key.tif).

    Args:
        groups: Mapping of output name -> list of input raster paths.
        output_dir: Directory for output COGs.
        compression: GDAL compression method.
        config: Batch processing configuration.

    Returns:
        List of CogMergeResult, one per group.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    tasks = [
        _MergeTask(
            key=key,
            input_paths=list(paths),
            output_path=output_dir / f"{key}.tif",
            compression=compression,
        )
        for key, paths in groups.items()
    ]

    return parallel_map(_merge_cog_worker, tasks, config=config, desc="Merging COGs")


# --- Batch zonal statistics ---


@dataclass
class _ZonalChunkTask:
    raster_path: str
    zones_path: str  # path to serialized zones (parquet)
    zone_id_col: str
    band: int
    agg: str
    chunk_start: int
    chunk_end: int


def _zonal_chunk_worker(task: _ZonalChunkTask) -> tuple[str, list]:
    """Worker: open raster once, process a chunk of zones.

    Each worker gets a slice of the zones and opens the raster independently.
    This is much more efficient than one IPC call per zone.
    """
    import warnings
    warnings.filterwarnings("ignore")

    import numpy as np
    import rasterio
    import rasterio.mask
    from geoetl.raster.zonal import AggMethod

    agg_funcs = {
        AggMethod.SUM: np.nansum,
        AggMethod.MEAN: np.nanmean,
        AggMethod.MEDIAN: np.nanmedian,
        AggMethod.MIN: np.nanmin,
        AggMethod.MAX: np.nanmax,
        AggMethod.STD: np.nanstd,
        AggMethod.COUNT: lambda a: np.count_nonzero(~np.isnan(a)),
    }

    agg_method = AggMethod(task.agg)
    agg_func = agg_funcs[agg_method]

    zones = gpd.read_parquet(task.zones_path)
    chunk = zones.iloc[task.chunk_start:task.chunk_end]

    results = []
    try:
        with rasterio.open(task.raster_path) as src:
            nodata = src.nodata
            for _, row in chunk.iterrows():
                zone_id = str(row[task.zone_id_col])
                try:
                    masked, _ = rasterio.mask.mask(
                        src, [row.geometry], crop=True,
                        nodata=nodata, all_touched=False,
                    )
                    data = masked[task.band - 1].astype(float)
                    if nodata is not None:
                        from geoetl.raster._utils import nodata_mask
                        data[nodata_mask(data, nodata)] = np.nan
                    valid = data[~np.isnan(data)]
                    pixel_count = len(valid)
                    value = float(agg_func(valid)) if pixel_count > 0 else None
                    nodata_count = int(np.count_nonzero(np.isnan(masked[task.band - 1].astype(float)))) if nodata is not None else 0
                except (ValueError, Exception):
                    value = None
                    pixel_count = 0
                    nodata_count = 0

                results.append({
                    "zone_id": zone_id,
                    "value": value,
                    "pixel_count": pixel_count,
                    "nodata_count": nodata_count,
                })
    except Exception as e:
        logger.error("Zonal chunk worker error: %s", e)

    return (task.raster_path, results)


def batch_zonal_stats(
    raster_paths: list[Path],
    zones_gdf: gpd.GeoDataFrame,
    zone_id_col: str,
    agg: AggMethod = AggMethod.MEAN,
    band: int = 1,
    config: Optional[BatchConfig] = None,
    zones_cache_dir: Optional[Path] = None,
) -> dict[str, list[ZonalResult]]:
    """Compute zonal statistics for multiple rasters with chunked inner parallelism.

    Parallelizes both across rasters AND within each raster (across zone chunks).
    Each worker opens the raster independently and processes its chunk of zones.
    Zones are serialized to parquet for efficient cross-process sharing.

    Args:
        raster_paths: List of raster paths.
        zones_gdf: GeoDataFrame with polygon zones.
        zone_id_col: Column name for zone identifiers.
        agg: Aggregation method.
        band: Raster band index (1-based).
        config: Batch processing configuration.
        zones_cache_dir: Directory for the temporary zones file. Defaults to /tmp.

    Returns:
        Dict mapping raster path string -> list of ZonalResult.
    """
    import multiprocessing as mp
    import tempfile
    from tqdm import tqdm

    config = config or BatchConfig()
    cache_dir = zones_cache_dir or Path(tempfile.gettempdir())
    cache_dir.mkdir(parents=True, exist_ok=True)
    zones_path = cache_dir / "_geoetl_zones_cache.parquet"
    zones_gdf.to_parquet(zones_path)

    try:
        # Build chunk tasks across all rasters
        n_zones = len(zones_gdf)
        chunk_size = config.zone_chunk_size
        all_tasks = []

        for raster_path in raster_paths:
            for i in range(0, n_zones, chunk_size):
                all_tasks.append(_ZonalChunkTask(
                    raster_path=str(raster_path),
                    zones_path=str(zones_path),
                    zone_id_col=zone_id_col,
                    band=band,
                    agg=agg.value,
                    chunk_start=i,
                    chunk_end=min(i + chunk_size, n_zones),
                ))

        logger.info(
            "Batch zonal stats: %d rasters x %d zone chunks = %d tasks, %d workers",
            len(raster_paths), max(1, n_zones // chunk_size), len(all_tasks), config.max_workers,
        )

        # Execute
        if config.max_workers == 1:
            raw_results = [_zonal_chunk_worker(t) for t in tqdm(all_tasks, desc="Zonal stats")]
        else:
            ctx = mp.get_context("spawn")
            raw_results = []
            with ctx.Pool(processes=config.max_workers) as pool:
                for result in tqdm(
                    pool.imap_unordered(_zonal_chunk_worker, all_tasks),
                    total=len(all_tasks),
                    desc="Zonal stats",
                ):
                    raw_results.append(result)

        # Combine chunks per raster
        combined: dict[str, list[dict]] = {}
        for raster_key, chunk_results in raw_results:
            combined.setdefault(raster_key, []).extend(chunk_results)

        return {
            key: [ZonalResult(**d) for d in dicts]
            for key, dicts in combined.items()
        }
    finally:
        zones_path.unlink(missing_ok=True)
