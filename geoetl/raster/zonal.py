"""Zonal statistics: aggregate raster values within vector zones."""

import logging
from enum import Enum
from pathlib import Path
from typing import Optional

import geopandas as gpd
import numpy as np
import rasterio
import rasterio.mask
from pydantic import BaseModel

from geoetl.exceptions import ZonalStatsError
from geoetl.raster._utils import nodata_mask

logger = logging.getLogger(__name__)


class AggMethod(str, Enum):
    SUM = "sum"
    MEAN = "mean"
    MEDIAN = "median"
    MIN = "min"
    MAX = "max"
    STD = "std"
    COUNT = "count"


class ZonalResult(BaseModel):
    """Result of zonal statistics for a single zone."""

    zone_id: str
    value: float | None
    pixel_count: int
    nodata_count: int


_AGG_FUNCS = {
    AggMethod.SUM: np.nansum,
    AggMethod.MEAN: np.nanmean,
    AggMethod.MEDIAN: np.nanmedian,
    AggMethod.MIN: np.nanmin,
    AggMethod.MAX: np.nanmax,
    AggMethod.STD: np.nanstd,
    AggMethod.COUNT: lambda a: np.count_nonzero(~np.isnan(a)),
}


def zonal_stats(
    raster_path: Path,
    zones_gdf: gpd.GeoDataFrame,
    zone_id_col: str,
    band: int = 1,
    agg: AggMethod = AggMethod.MEAN,
    all_touched: bool = False,
    nodata: Optional[float] = None,
) -> list[ZonalResult]:
    """Compute zonal statistics for each polygon zone against a raster.

    Args:
        raster_path: Path to the raster file.
        zones_gdf: GeoDataFrame with polygon geometries.
        zone_id_col: Column name for zone identifiers.
        band: Raster band index (1-based).
        agg: Aggregation method.
        all_touched: If True, all pixels touched by geometry are included.
            If False (default), only pixels whose center is inside the polygon.
        nodata: Override raster nodata value.

    Returns:
        List of ZonalResult, one per zone.
    """
    raster_path = Path(raster_path)
    if not raster_path.exists():
        raise ZonalStatsError(f"Raster not found: {raster_path}")

    if zone_id_col not in zones_gdf.columns:
        raise ZonalStatsError(f"Column '{zone_id_col}' not found in zones GeoDataFrame")

    agg_func = _AGG_FUNCS[agg]
    results: list[ZonalResult] = []

    with rasterio.open(raster_path) as src:
        # CRS safety check
        if src.crs is not None and zones_gdf.crs is not None:
            if not zones_gdf.crs.equals(src.crs):
                raise ZonalStatsError(
                    f"CRS mismatch: zones are {zones_gdf.crs}, raster is {src.crs}. "
                    "Reproject zones to match the raster CRS before calling zonal_stats."
                )

        raster_nodata = nodata if nodata is not None else src.nodata
        raster_bounds = src.bounds

        # Bbox pre-filter: only process zones that overlap the raster extent
        filtered = zones_gdf.cx[
            raster_bounds.left:raster_bounds.right,
            raster_bounds.bottom:raster_bounds.top,
        ]

        if len(filtered) < len(zones_gdf):
            logger.info(
                "Bbox pre-filter: %d/%d zones overlap raster",
                len(filtered), len(zones_gdf),
            )

        for idx, row in filtered.iterrows():
            zone_id = str(row[zone_id_col])
            geom = row.geometry

            try:
                masked, _ = rasterio.mask.mask(
                    src,
                    [geom],
                    crop=True,
                    all_touched=all_touched,
                    nodata=raster_nodata,
                )
            except ValueError:
                # Geometry doesn't overlap raster data
                results.append(ZonalResult(
                    zone_id=zone_id, value=None, pixel_count=0, nodata_count=0,
                ))
                continue

            data = masked[band - 1].astype(float)

            if raster_nodata is not None:
                nd_mask = nodata_mask(data, raster_nodata)
                nodata_count = int(np.count_nonzero(nd_mask))
                data[nd_mask] = np.nan
            else:
                nodata_count = 0

            valid = data[~np.isnan(data)]
            pixel_count = len(valid)

            if pixel_count == 0:
                value = None
            else:
                value = float(agg_func(valid))

            results.append(ZonalResult(
                zone_id=zone_id,
                value=value,
                pixel_count=pixel_count,
                nodata_count=nodata_count,
            ))

    logger.info("Zonal stats (%s): %d zones processed", agg.value, len(results))
    return results
