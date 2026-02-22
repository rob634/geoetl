"""Raster validation: CRS checks, type detection, quality analysis, memory estimation."""

import logging
from pathlib import Path

import numpy as np
import rasterio
from pyproj import CRS

from geoetl.config import RasterType
from geoetl.exceptions import RasterValidationError
from geoetl.raster.types import BandStats, CRSInfo, RasterInfo

logger = logging.getLogger(__name__)

# Dtype sizes in bytes
_DTYPE_BYTES = {
    "uint8": 1, "int8": 1,
    "uint16": 2, "int16": 2,
    "uint32": 4, "int32": 4,
    "float32": 4, "float64": 8,
}


def validate_raster(path: Path) -> RasterInfo:
    """Validate a raster file and return comprehensive metadata.

    Raises RasterValidationError if the file cannot be opened or has fatal issues.
    Non-fatal issues are collected in RasterInfo.errors.
    """
    path = Path(path)
    if not path.exists():
        raise RasterValidationError(f"File not found: {path}")

    errors: list[str] = []

    try:
        with rasterio.open(path) as src:
            crs_info = _validate_crs(src, errors)
            _check_bounds_sanity(src, crs_info, errors)
            raster_type = _detect_raster_type(src)
            bands = _compute_band_stats(src)
            memory_mb = _estimate_memory_footprint(src)
            _check_data_quality(src, bands, errors)
            _check_bit_depth_efficiency(src, errors)

            return RasterInfo(
                path=str(path),
                raster_type=raster_type,
                crs=crs_info,
                width=src.width,
                height=src.height,
                band_count=src.count,
                dtype=str(src.dtypes[0]),
                bands=bands,
                memory_estimate_mb=memory_mb,
                is_valid=len(errors) == 0,
                errors=errors,
            )
    except rasterio.errors.RasterioIOError as e:
        raise RasterValidationError(f"Cannot open raster: {e}") from e


def _validate_crs(src, errors: list[str]) -> CRSInfo:
    """Extract and validate CRS information."""
    if src.crs is None:
        errors.append("No CRS defined")
        return CRSInfo(epsg=None, wkt="", is_geographic=False, is_projected=False)

    try:
        crs = CRS.from_user_input(src.crs)
        epsg = crs.to_epsg()
        return CRSInfo(
            epsg=epsg,
            wkt=crs.to_wkt(),
            is_geographic=crs.is_geographic,
            is_projected=crs.is_projected,
        )
    except Exception as e:
        errors.append(f"CRS parsing error: {e}")
        return CRSInfo(epsg=None, wkt=str(src.crs), is_geographic=False, is_projected=False)


def _check_bounds_sanity(src, crs_info: CRSInfo, errors: list[str]):
    """Check that raster bounds are within expected ranges for the CRS."""
    bounds = src.bounds
    if crs_info.is_geographic:
        if bounds.left < -180 or bounds.right > 180:
            errors.append(f"Longitude out of range: [{bounds.left}, {bounds.right}]")
        if bounds.bottom < -90 or bounds.top > 90:
            errors.append(f"Latitude out of range: [{bounds.bottom}, {bounds.top}]")
    elif crs_info.is_projected:
        width = bounds.right - bounds.left
        height = bounds.top - bounds.bottom
        if width > 1e8 or height > 1e8:
            errors.append(f"Suspiciously large projected extent: {width:.0f} x {height:.0f}")


def _detect_raster_type(src) -> RasterType:
    """Detect raster type from band count, dtype, and color interpretation."""
    count = src.count
    dtype = str(src.dtypes[0])
    interp = [str(ci) for ci in src.colorinterp] if src.colorinterp else []

    # Check color interpretation first
    if count >= 3 and all(c in str(interp) for c in ["red", "green", "blue"]):
        if count == 4:
            return RasterType.RGBA
        return RasterType.RGB

    # Band count heuristics
    if count == 1:
        if dtype in ("float32", "float64"):
            return RasterType.DEM
        if dtype in ("uint8", "int8") and _is_categorical(src):
            return RasterType.CATEGORICAL
        if dtype in ("uint8",):
            return RasterType.PANCHROMATIC
        return RasterType.DEM  # default for single-band float-like data

    if count == 3:
        return RasterType.RGB
    if count == 4:
        return RasterType.RGBA
    if count > 4:
        return RasterType.MULTISPECTRAL

    return RasterType.UNKNOWN


def _is_categorical(src) -> bool:
    """Heuristic: check if a single-band raster has few unique values (likely categorical)."""
    try:
        # Sample a window to avoid reading the entire raster
        window = rasterio.windows.Window(0, 0, min(1024, src.width), min(1024, src.height))
        data = src.read(1, window=window)
        if src.nodata is not None:
            data = data[data != src.nodata]
        unique_count = len(np.unique(data))
        return unique_count < 256
    except Exception:
        return False


def _compute_band_stats(src) -> list[BandStats]:
    """Compute per-band statistics."""
    stats = []
    for i in range(1, src.count + 1):
        try:
            data = src.read(i).astype(float)
            nodata = src.nodata
            if nodata is not None:
                mask = data != nodata
                null_count = int((~mask).sum())
                valid = data[mask]
            else:
                null_count = 0
                valid = data.ravel()

            total = data.size
            null_pct = (null_count / total * 100) if total > 0 else 0.0

            if valid.size > 0:
                stats.append(BandStats(
                    band=i,
                    min=float(np.nanmin(valid)),
                    max=float(np.nanmax(valid)),
                    mean=float(np.nanmean(valid)),
                    std=float(np.nanstd(valid)),
                    nodata=nodata,
                    null_percent=null_pct,
                ))
            else:
                stats.append(BandStats(
                    band=i, min=0, max=0, mean=0, std=0,
                    nodata=nodata, null_percent=100.0,
                ))
        except Exception as e:
            logger.warning("Failed to compute stats for band %d: %s", i, e)
            stats.append(BandStats(
                band=i, min=0, max=0, mean=0, std=0,
                nodata=src.nodata, null_percent=0,
            ))
    return stats


def _estimate_memory_footprint(src) -> float:
    """Estimate peak memory in MB needed to process this raster."""
    dtype_str = str(src.dtypes[0])
    bytes_per_pixel = _DTYPE_BYTES.get(dtype_str, 8)
    raw_bytes = src.width * src.height * src.count * bytes_per_pixel
    # Factor of 3: input + output + overhead
    peak_bytes = raw_bytes * 3
    return peak_bytes / (1024 * 1024)


def _check_data_quality(src, bands: list[BandStats], errors: list[str]):
    """Check for data quality issues."""
    for band in bands:
        if band.null_percent > 99.0:
            errors.append(f"Band {band.band} is >99% nodata ({band.null_percent:.1f}%)")
        if band.min == band.max and band.null_percent < 100:
            errors.append(f"Band {band.band} has constant value: {band.min}")


def _check_bit_depth_efficiency(src, errors: list[str]):
    """Flag unnecessarily wide data types."""
    dtype_str = str(src.dtypes[0])
    if dtype_str == "float64":
        errors.append("64-bit float detected; consider float32 for reduced file size")
