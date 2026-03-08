"""Assign polygon attributes to H3 hexagons via DuckDB spatial join."""

import logging
import warnings

import geopandas as gpd
import pandas as pd

from geoetl.config import BatchConfig
from geoetl.duckdb.engine import get_connection, registered_table
from geoetl.exceptions import H3Error
from geoetl.h3.grid import extract_centroids

logger = logging.getLogger(__name__)


def assign_polygons(
    h3_gdf: gpd.GeoDataFrame,
    polygons_gdf: gpd.GeoDataFrame,
    columns: list[str],
    tile_degrees: tuple[int, int] = (30, 60),
    config: BatchConfig | None = None,
) -> pd.DataFrame:
    """Assign polygon attributes to H3 hexagons via point-in-polygon spatial join.

    Uses DuckDB spatial extension with R-tree indexing for fast joins.
    No manual tiling or multiprocessing required.

    Args:
        h3_gdf: GeoDataFrame with h3_index column and polygon geometry.
        polygons_gdf: GeoDataFrame with polygon geometries and attribute columns.
        columns: Column names from polygons_gdf to assign to each hexagon.
        tile_degrees: Deprecated, ignored. Kept for backward compatibility.
        config: Deprecated, ignored. Kept for backward compatibility.

    Returns:
        DataFrame with h3_index + assigned columns (one row per hexagon).
    """
    if tile_degrees != (30, 60) or config is not None:
        warnings.warn(
            "tile_degrees and config params are deprecated — DuckDB handles partitioning internally",
            DeprecationWarning,
            stacklevel=2,
        )

    if "h3_index" not in h3_gdf.columns:
        raise H3Error("h3_gdf must have 'h3_index' column")

    for col in columns:
        if col not in polygons_gdf.columns:
            raise H3Error(f"Column '{col}' not found in polygons_gdf")

    # Ensure centroids are available
    h3_gdf = extract_centroids(h3_gdf)

    conn = get_connection()

    # Prepare H3 points table
    h3_df = pd.DataFrame({
        "h3_index": h3_gdf["h3_index"].values,
        "lon": h3_gdf["lon"].values,
        "lat": h3_gdf["lat"].values,
    })

    # Prepare polygons table with WKB geometry
    poly_df = pd.DataFrame(polygons_gdf[columns])
    poly_df["geom_wkb"] = polygons_gdf.geometry.to_wkb()

    # Build column selection
    col_select = ", ".join(f'p."{c}"' for c in columns)

    sql = f"""
        SELECT h.h3_index, {col_select}
        FROM _h3_points h
        JOIN _polygons p
            ON ST_Within(ST_Point(h.lon, h.lat), ST_GeomFromWKB(p.geom_wkb))
    """

    with registered_table(conn, "_h3_points", h3_df):
        with registered_table(conn, "_polygons", poly_df):
            result_df = conn.execute(sql).fetchdf()

    # Deduplicate: keep first match per h3_index
    result_df = result_df.drop_duplicates(subset="h3_index", keep="first")

    if result_df.empty:
        logger.warning("No spatial join matches found")
        return pd.DataFrame(columns=["h3_index"] + columns)

    matched = result_df[columns[0]].notna().sum() if columns else len(result_df)
    logger.info(
        "Spatial join complete: %d hexagons, %d matched",
        len(result_df), matched,
    )

    return result_df
