"""Vector utility functions."""

import logging
import zipfile
from pathlib import Path

import geopandas as gpd
import pandas as pd
from shapely.geometry import Point

logger = logging.getLogger(__name__)

DEFAULT_CRS = "EPSG:4326"


def xy_df_to_gdf(
    df: pd.DataFrame,
    x_col: str = "longitude",
    y_col: str = "latitude",
    crs: str = DEFAULT_CRS,
) -> gpd.GeoDataFrame:
    """Convert a DataFrame with X/Y columns to a GeoDataFrame.

    Args:
        df: Input DataFrame.
        x_col: Name of the longitude/X column.
        y_col: Name of the latitude/Y column.
        crs: Coordinate reference system.

    Returns:
        GeoDataFrame with Point geometries.
    """
    geometry = [Point(xy) for xy in zip(df[x_col], df[y_col])]
    gdf = gpd.GeoDataFrame(df, geometry=geometry, crs=crs)
    return gdf


def wkt_df_to_gdf(
    df: pd.DataFrame,
    wkt_col: str = "geometry",
    crs: str = DEFAULT_CRS,
) -> gpd.GeoDataFrame:
    """Convert a DataFrame with a WKT geometry column to a GeoDataFrame.

    Args:
        df: Input DataFrame.
        wkt_col: Name of the WKT geometry column.
        crs: Coordinate reference system.

    Returns:
        GeoDataFrame with parsed geometries.
    """
    from shapely import wkt

    geometry = df[wkt_col].apply(wkt.loads)
    gdf = gpd.GeoDataFrame(df.drop(columns=[wkt_col]), geometry=geometry, crs=crs)
    return gdf


def extract_zip_file(zip_path: Path, extract_dir: Path) -> Path:
    """Extract a zip archive and return the extraction directory.

    Args:
        zip_path: Path to the zip file.
        extract_dir: Directory to extract into.

    Returns:
        Path to the extraction directory.
    """
    extract_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(extract_dir)
    logger.info("Extracted %s to %s", zip_path.name, extract_dir)
    return extract_dir
