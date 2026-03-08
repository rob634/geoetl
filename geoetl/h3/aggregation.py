"""Raster to H3 aggregation: zonal statistics and point sampling.

Two methods based on source-vs-hex resolution ratio:
- zonal_aggregate: source pixels <= H3 hex size (rasterio.mask per hex chunk)
- point_sample: source cells > H3 hex size (nearest-neighbor centroid lookup)

For datasets like MapSPAM where production/area are extensive (per-pixel totals)
and yield is intensive (a derived ratio), use aggregate_extensive() to sum
production and area rasters, then derive_yield() to compute area-weighted yield.

See docs/mapspam-h3-aggregation-methodology.md for full methodology.
"""

import logging
import tempfile
from dataclasses import dataclass
from pathlib import Path

import geopandas as gpd
import numpy as np
import rasterio
import rasterio.mask

from geoetl.config import BatchConfig
from geoetl.exceptions import H3Error
from geoetl.h3.grid import extract_centroids
from geoetl.raster.zonal import AggMethod

logger = logging.getLogger(__name__)

_AGG_FUNCS = {
    AggMethod.SUM: np.nansum,
    AggMethod.MEAN: np.nanmean,
    AggMethod.MEDIAN: np.nanmedian,
    AggMethod.MIN: np.nanmin,
    AggMethod.MAX: np.nanmax,
    AggMethod.STD: np.nanstd,
    AggMethod.COUNT: lambda a: np.count_nonzero(~np.isnan(a)),
}


@dataclass
class _ChunkTask:
    """Serializable task for a worker processing a chunk of hexagons."""
    zones_path: str
    raster_path: str
    agg: str
    band: int
    all_touched: bool
    chunk_start: int
    chunk_end: int
    exclude_zero: bool


def _zonal_chunk_worker(task: _ChunkTask) -> list[dict]:
    """Worker: open raster once, mask each hexagon in the chunk.

    Must be top-level for pickling. Imports inside to ensure
    clean GDAL state in spawn-context workers.
    """
    import warnings
    warnings.filterwarnings("ignore")

    import geopandas as _gpd
    import numpy as _np
    import rasterio as _rio
    import rasterio.mask as _mask

    from geoetl.raster.zonal import AggMethod

    agg_funcs = {
        AggMethod.SUM: _np.nansum,
        AggMethod.MEAN: _np.nanmean,
        AggMethod.MEDIAN: _np.nanmedian,
        AggMethod.MIN: _np.nanmin,
        AggMethod.MAX: _np.nanmax,
        AggMethod.STD: _np.nanstd,
        AggMethod.COUNT: lambda a: _np.count_nonzero(~_np.isnan(a)),
    }

    agg_method = AggMethod(task.agg)
    agg_func = agg_funcs[agg_method]

    zones = _gpd.read_parquet(task.zones_path)
    chunk = zones.iloc[task.chunk_start:task.chunk_end]

    results = []
    try:
        with _rio.open(task.raster_path) as src:
            nodata = src.nodata
            for _, row in chunk.iterrows():
                try:
                    masked, _ = _mask.mask(
                        src,
                        [row.geometry],
                        crop=True,
                        nodata=nodata,
                        all_touched=task.all_touched,
                    )
                    data = masked[task.band - 1].astype(float)

                    if nodata is not None:
                        from geoetl.raster._utils import nodata_mask as _nodata_mask
                        data[_nodata_mask(data, nodata)] = _np.nan

                    valid = data[~_np.isnan(data)]
                    if task.exclude_zero and agg_method != AggMethod.COUNT:
                        valid = valid[valid != 0]

                    if len(valid) == 0:
                        results.append({"h3_index": row["h3_index"], "value": float("nan"), "pixel_count": 0})
                        continue

                    results.append({
                        "h3_index": row["h3_index"],
                        "value": float(agg_func(valid)),
                        "pixel_count": len(valid),
                    })
                except Exception as e:
                    import logging as _logging
                    _logging.getLogger(__name__).debug("Hex %s mask failed: %s", row["h3_index"], e)
                    results.append({"h3_index": row["h3_index"], "value": float("nan"), "pixel_count": 0})
    except Exception as e:
        logger.error("Chunk worker error on %s: %s", task.raster_path, e)
        raise

    return results


def zonal_aggregate(
    raster_path: Path,
    h3_gdf: gpd.GeoDataFrame,
    agg: AggMethod = AggMethod.SUM,
    band: int = 1,
    all_touched: bool = False,
    config: BatchConfig | None = None,
    exclude_zero: bool = False,
) -> gpd.GeoDataFrame:
    """Aggregate raster values into H3 hexagons using zonal statistics.

    Designed for source pixels similar or smaller than H3 hex size.
    Chunks hexagons across workers — each worker opens the raster independently
    and processes its chunk of hexagons.

    Args:
        raster_path: Path to the raster file.
        h3_gdf: GeoDataFrame with h3_index column and polygon geometry.
        agg: Aggregation method (SUM for production/area, MEAN for yield).
        band: Raster band index (1-based).
        all_touched: Include all pixels touched by geometry (vs centroid-in-polygon).
        config: Batch config for worker count and chunk size.
        exclude_zero: If True, exclude zero-valued pixels before aggregation.
            Default False preserves legitimate zero values.

    Returns:
        GeoDataFrame with h3_index, value, pixel_count columns + original geometry.
    """
    import multiprocessing as mp
    from tqdm import tqdm

    config = config or BatchConfig()
    raster_path = Path(raster_path)

    if not raster_path.exists():
        raise H3Error(f"Raster not found: {raster_path}")

    if "h3_index" not in h3_gdf.columns:
        raise H3Error("GeoDataFrame must have 'h3_index' column")

    # Bbox pre-filter: only hexagons overlapping raster extent
    with rasterio.open(raster_path) as src:
        bounds = src.bounds

    buffer = 0.1
    filtered = h3_gdf.cx[
        bounds.left - buffer:bounds.right + buffer,
        bounds.bottom - buffer:bounds.top + buffer,
    ]

    if len(filtered) == 0:
        logger.warning("No hexagons overlap raster extent")
        return gpd.GeoDataFrame(columns=["h3_index", "value", "pixel_count", "geometry"])

    logger.info(
        "Zonal aggregate: %d/%d hexagons overlap raster, %d workers, chunks of %d",
        len(filtered), len(h3_gdf), config.max_workers, config.zone_chunk_size,
    )

    # Serialize zones to temp parquet for workers
    with tempfile.TemporaryDirectory() as tmpdir:
        zones_path = Path(tmpdir) / "zones.parquet"
        filtered.to_parquet(zones_path)

        # Build chunk tasks
        n = len(filtered)
        chunk_size = config.zone_chunk_size
        tasks = [
            _ChunkTask(
                zones_path=str(zones_path),
                raster_path=str(raster_path),
                agg=agg.value,
                band=band,
                all_touched=all_touched,
                chunk_start=i,
                chunk_end=min(i + chunk_size, n),
                exclude_zero=exclude_zero,
            )
            for i in range(0, n, chunk_size)
        ]

        if config.max_workers == 1:
            # Sequential for debugging
            all_results = []
            for task in tqdm(tasks, desc="Zonal stats"):
                all_results.extend(_zonal_chunk_worker(task))
        else:
            ctx = mp.get_context("spawn")
            all_results = []
            with ctx.Pool(processes=config.max_workers) as pool:
                for chunk_results in tqdm(
                    pool.imap_unordered(_zonal_chunk_worker, tasks),
                    total=len(tasks),
                    desc="Zonal stats",
                ):
                    all_results.extend(chunk_results)

    if not all_results:
        logger.warning("No valid data found in any hexagon")
        return gpd.GeoDataFrame(columns=["h3_index", "value", "pixel_count", "geometry"])

    # Join results back to geometry
    results_df = gpd.GeoDataFrame(all_results)
    geom_lookup = filtered[["h3_index", "geometry"]].set_index("h3_index")
    results_df = results_df.set_index("h3_index").join(geom_lookup).reset_index()
    results_df = gpd.GeoDataFrame(results_df, geometry="geometry", crs=h3_gdf.crs)

    logger.info("Zonal aggregate complete: %d hexagons with data", len(results_df))
    return results_df


def point_sample(
    raster_path: Path,
    h3_gdf: gpd.GeoDataFrame,
    band: int = 1,
    method: str = "nearest",
) -> gpd.GeoDataFrame:
    """Sample raster values at H3 hex centroids using nearest-neighbor lookup.

    Designed for source cells larger than H3 hexes (e.g. SPEI 0.25deg ~28km
    vs H3 L5 ~8.5km). Multiple hexes will sample from the same source cell.

    For NetCDF files, uses xarray.sel(method='nearest') for vectorized lookup.
    For GeoTIFFs, uses rasterio windowed reads.

    Args:
        raster_path: Path to raster file (.tif, .nc).
        h3_gdf: GeoDataFrame with h3_index column.
        band: Raster band index (1-based, for GeoTIFF only).
        method: Interpolation method ('nearest' supported).

    Returns:
        GeoDataFrame with h3_index and sampled value column.
    """
    raster_path = Path(raster_path)
    if not raster_path.exists():
        raise H3Error(f"Raster not found: {raster_path}")

    h3_gdf = extract_centroids(h3_gdf)
    lats = h3_gdf["lat"].values
    lons = h3_gdf["lon"].values

    suffix = raster_path.suffix.lower()
    if suffix in (".nc", ".nc4"):
        values = _sample_netcdf(raster_path, lats, lons)
    else:
        values = _sample_geotiff(raster_path, lats, lons, band)

    result = h3_gdf.copy()
    result["value"] = values.astype("float32")

    valid_count = np.count_nonzero(~np.isnan(values))
    logger.info("Point sample: %d/%d hexagons with valid data", valid_count, len(h3_gdf))
    return result


def _sample_netcdf(path: Path, lats: np.ndarray, lons: np.ndarray) -> np.ndarray:
    """Sample NetCDF values at lat/lon points using xarray nearest-neighbor."""
    import xarray as xr

    ds = xr.open_dataset(path)

    # Find the data variable (exclude coordinate-like vars)
    data_vars = [v for v in ds.data_vars if ds[v].ndim >= 2]
    if not data_vars:
        raise H3Error(f"No 2D+ data variables found in {path.name}")
    data = ds[data_vars[0]]

    # Collapse time dimension if present
    if "time" in data.dims:
        data = data.isel(time=0)

    lat_da = xr.DataArray(lats, dims="points")
    lon_da = xr.DataArray(lons, dims="points")
    values = data.sel(lat=lat_da, lon=lon_da, method="nearest").values

    ds.close()
    return values


def _sample_geotiff(path: Path, lats: np.ndarray, lons: np.ndarray, band: int) -> np.ndarray:
    """Sample GeoTIFF values at lat/lon points."""
    with rasterio.open(path) as src:
        # Convert lat/lon to pixel coordinates
        rows, cols = rasterio.transform.rowcol(src.transform, lons, lats)
        rows = np.array(rows)
        cols = np.array(cols)

        # Clamp to valid range
        rows = np.clip(rows, 0, src.height - 1)
        cols = np.clip(cols, 0, src.width - 1)

        data = src.read(band)
        values = data[rows, cols].astype(float)

        if src.nodata is not None:
            values[values == src.nodata] = np.nan

    return values


# ---------------------------------------------------------------------------
# Extensive/intensive variable support (MapSPAM and similar datasets)
# ---------------------------------------------------------------------------


def aggregate_extensive(
    raster_paths: dict[str, Path],
    h3_gdf: gpd.GeoDataFrame,
    band: int = 1,
    config: BatchConfig | None = None,
    exclude_zero: bool = False,
) -> gpd.GeoDataFrame:
    """Aggregate multiple extensive-variable rasters into H3 hexagons by summing.

    Use this for datasets like MapSPAM where production and area are per-pixel
    totals that should be summed across pixels within each hex.

    Uses DuckDB FULL OUTER JOIN to merge results from multiple rasters.

    Args:
        raster_paths: Mapping of column name -> raster path.
            e.g. {"production_mt": Path("P_WHEA_A.tif"), "harv_area_ha": Path("H_WHEA_A.tif")}
        h3_gdf: GeoDataFrame with h3_index column and polygon geometry.
        band: Raster band index (1-based).
        config: Batch config for parallelism.

    Returns:
        GeoDataFrame with h3_index, geometry, and one column per raster (summed values).

    Example:
        >>> result = aggregate_extensive(
        ...     {"production_mt": prod_path, "harv_area_ha": area_path},
        ...     h3_grid,
        ... )
        >>> result = derive_yield(result, "production_mt", "harv_area_ha", "yield_kgha")
    """
    from geoetl.duckdb.engine import get_connection

    if not raster_paths:
        raise H3Error("raster_paths must not be empty")

    # Run zonal aggregation for each raster (rasterio I/O — unchanged)
    agg_results: dict[str, gpd.GeoDataFrame] = {}
    for col_name, raster_path in raster_paths.items():
        logger.info("Aggregating extensive variable: %s from %s", col_name, Path(raster_path).name)
        agg_results[col_name] = zonal_aggregate(
            raster_path=raster_path,
            h3_gdf=h3_gdf,
            agg=AggMethod.SUM,
            band=band,
            config=config,
            exclude_zero=exclude_zero,
        )

    # If only one raster, no join needed
    col_names = list(raster_paths.keys())
    if len(col_names) == 1:
        result_gdf = agg_results[col_names[0]].rename(columns={"value": col_names[0]})
        result_gdf[col_names[0]] = result_gdf[col_names[0]].fillna(0).astype("float32")
        return result_gdf

    # Register each result as a DuckDB table and join with FULL OUTER JOIN
    from contextlib import ExitStack
    import pandas as pd
    from geoetl.duckdb.engine import registered_table

    conn = get_connection()
    table_names = []
    with ExitStack() as stack:
        for col_name, agg_result in agg_results.items():
            tbl = f"_agg_{col_name}"
            df = pd.DataFrame({"h3_index": agg_result["h3_index"], "value": agg_result["value"]})
            stack.enter_context(registered_table(conn, tbl, df))
            table_names.append(tbl)

        # Build chained FULL OUTER JOIN SQL
        first_tbl = table_names[0]
        first_col = col_names[0]
        select_parts = [f'COALESCE({first_tbl}.h3_index, {", ".join(t + ".h3_index" for t in table_names[1:])}) AS h3_index']
        select_parts.append(f'COALESCE({first_tbl}.value, 0) AS "{first_col}"')

        join_parts = f"FROM {first_tbl}"
        for tbl, col_name in zip(table_names[1:], col_names[1:]):
            join_parts += f"\nFULL OUTER JOIN {tbl} ON {first_tbl}.h3_index = {tbl}.h3_index"
            select_parts.append(f'COALESCE({tbl}.value, 0) AS "{col_name}"')

        sql = f"SELECT {', '.join(select_parts)}\n{join_parts}"
        joined_df = conn.execute(sql).fetchdf()

    # Cast to float32
    for col_name in col_names:
        joined_df[col_name] = joined_df[col_name].astype("float32")

    # Reconstruct geometry from the first result that has it
    geom_source = next(r for r in agg_results.values() if "geometry" in r.columns)
    geom_lookup = geom_source[["h3_index", "geometry"]].drop_duplicates("h3_index").set_index("h3_index")
    joined_df = joined_df.set_index("h3_index").join(geom_lookup).reset_index()
    result_gdf = gpd.GeoDataFrame(joined_df, geometry="geometry", crs=h3_gdf.crs)

    return result_gdf


def derive_yield(
    gdf: gpd.GeoDataFrame,
    production_col: str,
    area_col: str,
    yield_col: str,
    scale: float = 1000.0,
) -> gpd.GeoDataFrame:
    """Derive yield (intensive variable) from production and area (extensive variables).

    Yield = production / area. This is the ONLY correct way to aggregate yield
    for gridded data. Never average yield pixels directly.

    Args:
        gdf: GeoDataFrame with summed production and area columns.
        production_col: Column name for production (metric tons).
        area_col: Column name for harvested area (hectares).
        yield_col: Name for the derived yield column.
        scale: Conversion factor. Default 1000.0 converts MT/ha to kg/ha.
            Set to 1.0 if production and area are in compatible units.

    Returns:
        GeoDataFrame with yield column added.

    Example:
        >>> gdf = derive_yield(gdf, "production_mt", "harv_area_ha", "yield_kgha")
    """
    if production_col not in gdf.columns:
        raise H3Error(f"Column '{production_col}' not found")
    if area_col not in gdf.columns:
        raise H3Error(f"Column '{area_col}' not found")

    gdf = gdf.copy()
    area = gdf[area_col].values
    production = gdf[production_col].values

    # Avoid division by zero: yield is NaN where area is 0
    with np.errstate(divide="ignore", invalid="ignore"):
        yield_values = np.where(area > 0, (production / area) * scale, np.nan)

    gdf[yield_col] = yield_values.astype("float32")

    valid = np.count_nonzero(~np.isnan(yield_values))
    logger.info(
        "Derived %s: %d hexagons with valid yield (mean=%.0f, median=%.0f)",
        yield_col,
        valid,
        np.nanmean(yield_values) if valid > 0 else 0,
        np.nanmedian(yield_values) if valid > 0 else 0,
    )
    return gdf
