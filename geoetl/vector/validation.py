"""Eight-stage geometry validation pipeline.

Stages:
1. Remove null geometries
2. Repair invalid geometries (make_valid)
3. Force 2D (drop Z/M coordinates)
4. Fix antimeridian crossings
5. Normalize to multi-geometry types
6. Fix polygon winding order (CCW exterior, CW holes)
7. Validate geometry types
8. Sanitize datetime columns
"""

import logging

import geopandas as gpd
import pandas as pd
from shapely import force_2d
from shapely.geometry import MultiLineString, MultiPoint, MultiPolygon
from shapely.geometry.polygon import orient
from shapely.ops import split
from shapely.geometry import LineString
from shapely.affinity import translate
from shapely.validation import make_valid

from geoetl.vector.types import StageStats, ValidationReport

logger = logging.getLogger(__name__)

SUPPORTED_GEOM_TYPES = {
    "Point", "MultiPoint",
    "LineString", "MultiLineString",
    "Polygon", "MultiPolygon",
}

MIN_YEAR = 1
MAX_YEAR = 9999


def validate_geometries(
    gdf: gpd.GeoDataFrame,
    target_crs: str = "EPSG:4326",
) -> tuple[gpd.GeoDataFrame, ValidationReport]:
    """Run the 8-stage geometry validation pipeline.

    Args:
        gdf: Input GeoDataFrame.
        target_crs: Target CRS for reprojection (after validation).

    Returns:
        Tuple of (cleaned GeoDataFrame, ValidationReport).
    """
    report = ValidationReport()

    gdf, stats = _stage_1_remove_nulls(gdf)
    report.add(stats)

    gdf, stats = _stage_2_repair_invalid(gdf)
    report.add(stats)

    gdf, stats = _stage_3_force_2d(gdf)
    report.add(stats)

    gdf, stats = _stage_4_fix_antimeridian(gdf)
    report.add(stats)

    gdf, stats = _stage_5_normalize_multi(gdf)
    report.add(stats)

    gdf, stats = _stage_6_fix_winding_order(gdf)
    report.add(stats)

    gdf, stats = _stage_7_validate_types(gdf)
    report.add(stats)

    gdf, stats = _stage_8_sanitize_datetimes(gdf)
    report.add(stats)

    # Reproject to target CRS if needed
    if gdf.crs is not None and str(gdf.crs) != target_crs:
        gdf = gdf.to_crs(target_crs)
        logger.info("Reprojected to %s", target_crs)
    elif gdf.crs is None:
        gdf = gdf.set_crs(target_crs)
        logger.info("Set CRS to %s (was undefined)", target_crs)

    # Clean column names
    gdf.columns = [_clean_column_name(c) for c in gdf.columns]

    logger.info(
        "Validation complete: %d -> %d rows, %d total fixes",
        report.input_count, report.output_count, report.total_affected,
    )
    return gdf, report


def _clean_column_name(name: str) -> str:
    """Normalize column names to lowercase with underscores."""
    if name == "geometry":
        return name
    return (
        name.lower()
        .replace(" ", "_")
        .replace("-", "_")
        .replace(".", "_")
        .replace("(", "")
        .replace(")", "")
    )


def _stage_1_remove_nulls(gdf: gpd.GeoDataFrame) -> tuple[gpd.GeoDataFrame, StageStats]:
    """Stage 1: Remove rows with null geometries."""
    n_before = len(gdf)
    null_mask = gdf.geometry.isna()
    null_count = int(null_mask.sum())
    gdf = gdf[~null_mask].copy()

    if null_count:
        logger.info("Stage 1: Removed %d null geometries", null_count)

    return gdf, StageStats(
        stage="remove_nulls",
        input_count=n_before,
        output_count=len(gdf),
        affected=null_count,
        details=f"Removed {null_count} null geometries",
    )


def _stage_2_repair_invalid(gdf: gpd.GeoDataFrame) -> tuple[gpd.GeoDataFrame, StageStats]:
    """Stage 2: Repair invalid geometries using make_valid."""
    n_before = len(gdf)
    invalid_mask = ~gdf.geometry.is_valid
    invalid_count = int(invalid_mask.sum())

    if invalid_count > 0:
        gdf.loc[invalid_mask, "geometry"] = gdf.loc[invalid_mask, "geometry"].apply(make_valid)
        logger.info("Stage 2: Repaired %d invalid geometries", invalid_count)

    return gdf, StageStats(
        stage="repair_invalid",
        input_count=n_before,
        output_count=len(gdf),
        affected=invalid_count,
        details=f"Repaired {invalid_count} invalid geometries",
    )


def _stage_3_force_2d(gdf: gpd.GeoDataFrame) -> tuple[gpd.GeoDataFrame, StageStats]:
    """Stage 3: Force all geometries to 2D (drop Z/M coordinates)."""
    n_before = len(gdf)
    has_z = gdf.geometry.has_z
    z_count = int(has_z.sum())

    if z_count > 0:
        crs_before = gdf.crs
        geoms_2d = gdf.geometry.apply(force_2d)
        gdf = gpd.GeoDataFrame(
            gdf.drop(columns=["geometry"]),
            geometry=geoms_2d,
            crs=crs_before,
        )
        logger.info("Stage 3: Forced %d geometries to 2D", z_count)

    return gdf, StageStats(
        stage="force_2d",
        input_count=n_before,
        output_count=len(gdf),
        affected=z_count,
        details=f"Forced {z_count} geometries to 2D",
    )


def _stage_4_fix_antimeridian(gdf: gpd.GeoDataFrame) -> tuple[gpd.GeoDataFrame, StageStats]:
    """Stage 4: Fix geometries crossing the antimeridian (±180°)."""
    n_before = len(gdf)
    fixed_count = 0

    def _fix_single(geom):
        nonlocal fixed_count
        bounds = geom.bounds
        minx, _, maxx, _ = bounds
        width = maxx - minx

        needs_fix = maxx > 180 or minx < -180 or width > 180
        if not needs_fix:
            return geom

        fixed_count += 1
        try:
            antimeridian = LineString([(180, -90), (180, 90)])
            result = split(geom, antimeridian)
            fixed_parts = []
            for part in result.geoms:
                if part.bounds[0] >= 180:
                    fixed_parts.append(translate(part, xoff=-360))
                elif part.bounds[2] <= -180:
                    fixed_parts.append(translate(part, xoff=360))
                else:
                    fixed_parts.append(part)
            if len(fixed_parts) == 1:
                return fixed_parts[0]
            # Combine parts back into multi-geometry
            geom_type = geom.geom_type
            if "Polygon" in geom_type:
                return MultiPolygon(fixed_parts)
            elif "LineString" in geom_type:
                return MultiLineString(fixed_parts)
            elif "Point" in geom_type:
                return MultiPoint(fixed_parts)
            return geom
        except Exception:
            return geom

    gdf["geometry"] = gdf.geometry.apply(_fix_single)
    if fixed_count:
        logger.info("Stage 4: Fixed %d antimeridian crossings", fixed_count)

    return gdf, StageStats(
        stage="fix_antimeridian",
        input_count=n_before,
        output_count=len(gdf),
        affected=fixed_count,
        details=f"Fixed {fixed_count} antimeridian crossings",
    )


def _stage_5_normalize_multi(gdf: gpd.GeoDataFrame) -> tuple[gpd.GeoDataFrame, StageStats]:
    """Stage 5: Normalize single geometries to their multi-type equivalents."""
    n_before = len(gdf)
    converted_count = 0

    def _to_multi(geom):
        nonlocal converted_count
        geom_type = geom.geom_type
        if geom_type == "Polygon":
            converted_count += 1
            return MultiPolygon([geom])
        elif geom_type == "LineString":
            converted_count += 1
            return MultiLineString([geom])
        elif geom_type == "Point":
            converted_count += 1
            return MultiPoint([geom])
        return geom

    gdf["geometry"] = gdf.geometry.apply(_to_multi)
    if converted_count:
        logger.info("Stage 5: Normalized %d geometries to multi-type", converted_count)

    return gdf, StageStats(
        stage="normalize_multi",
        input_count=n_before,
        output_count=len(gdf),
        affected=converted_count,
        details=f"Converted {converted_count} to multi-type",
    )


def _stage_6_fix_winding_order(gdf: gpd.GeoDataFrame) -> tuple[gpd.GeoDataFrame, StageStats]:
    """Stage 6: Enforce CCW exterior, CW holes for polygons."""
    n_before = len(gdf)
    fixed_count = 0

    def _orient_polygon(geom):
        nonlocal fixed_count
        if geom.geom_type == "Polygon":
            oriented = orient(geom, sign=1.0)
            if not oriented.equals(geom):
                fixed_count += 1
            return oriented
        elif geom.geom_type == "MultiPolygon":
            oriented_polys = [orient(p, sign=1.0) for p in geom.geoms]
            result = MultiPolygon(oriented_polys)
            if not result.equals(geom):
                fixed_count += 1
            return result
        return geom

    gdf["geometry"] = gdf.geometry.apply(_orient_polygon)
    if fixed_count:
        logger.info("Stage 6: Fixed winding order on %d polygons", fixed_count)

    return gdf, StageStats(
        stage="fix_winding_order",
        input_count=n_before,
        output_count=len(gdf),
        affected=fixed_count,
        details=f"Fixed winding order on {fixed_count} polygons",
    )


def _stage_7_validate_types(gdf: gpd.GeoDataFrame) -> tuple[gpd.GeoDataFrame, StageStats]:
    """Stage 7: Validate that all geometry types are supported."""
    n_before = len(gdf)
    unique_types = set(gdf.geometry.geom_type.unique())
    unsupported = unique_types - SUPPORTED_GEOM_TYPES
    removed_count = 0

    if unsupported:
        logger.warning("Unsupported geometry types found: %s", unsupported)
        mask = gdf.geometry.geom_type.isin(SUPPORTED_GEOM_TYPES)
        removed_count = int((~mask).sum())
        gdf = gdf[mask].copy()

    return gdf, StageStats(
        stage="validate_types",
        input_count=n_before,
        output_count=len(gdf),
        affected=removed_count,
        details=f"Removed {removed_count} unsupported geometry types" if removed_count else "All types valid",
    )


def _stage_8_sanitize_datetimes(gdf: gpd.GeoDataFrame) -> tuple[gpd.GeoDataFrame, StageStats]:
    """Stage 8: Sanitize datetime columns (replace out-of-range dates with NaT)."""
    n_before = len(gdf)
    total_fixed = 0

    for col in gdf.columns:
        if col == "geometry":
            continue
        if pd.api.types.is_datetime64_any_dtype(gdf[col]):
            years = gdf[col].dt.year
            invalid_mask = (years < MIN_YEAR) | (years > MAX_YEAR)
            n_invalid = int(invalid_mask.sum())
            if n_invalid > 0:
                gdf.loc[invalid_mask, col] = pd.NaT
                total_fixed += n_invalid
                logger.info("Stage 8: Fixed %d invalid dates in column '%s'", n_invalid, col)

    return gdf, StageStats(
        stage="sanitize_datetimes",
        input_count=n_before,
        output_count=len(gdf),
        affected=total_fixed,
        details=f"Sanitized {total_fixed} datetime values",
    )
