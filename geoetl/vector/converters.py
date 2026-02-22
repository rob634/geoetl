"""Vector format converters: CSV, GeoJSON, GPKG, KML, KMZ, Shapefile."""

import io
import logging
import tempfile
import zipfile
from pathlib import Path
from typing import Optional

import geopandas as gpd
import pandas as pd

from geoetl.exceptions import UnsupportedFormatError
from geoetl.vector.helpers import DEFAULT_CRS, xy_df_to_gdf, wkt_df_to_gdf

logger = logging.getLogger(__name__)


def csv_to_gdf(
    path: Path,
    lat_col: Optional[str] = None,
    lon_col: Optional[str] = None,
    wkt_col: Optional[str] = None,
    **kwargs,
) -> gpd.GeoDataFrame:
    """Load a CSV file as a GeoDataFrame.

    Supports both lat/lon columns and WKT geometry columns.
    Auto-detects common column names if not specified.
    """
    df = pd.read_csv(path, **kwargs)
    columns_lower = {c.lower(): c for c in df.columns}

    # Try WKT column first
    if wkt_col:
        return wkt_df_to_gdf(df, wkt_col=wkt_col)
    for candidate in ("geometry", "wkt", "geom", "the_geom"):
        if candidate in columns_lower:
            return wkt_df_to_gdf(df, wkt_col=columns_lower[candidate])

    # Try lat/lon columns
    lat = lat_col
    lon = lon_col
    if lat is None:
        for candidate in ("latitude", "lat", "y"):
            if candidate in columns_lower:
                lat = columns_lower[candidate]
                break
    if lon is None:
        for candidate in ("longitude", "lon", "lng", "long", "x"):
            if candidate in columns_lower:
                lon = columns_lower[candidate]
                break

    if lat and lon:
        return xy_df_to_gdf(df, x_col=lon, y_col=lat)

    raise UnsupportedFormatError(
        f"Cannot determine geometry from CSV columns: {list(df.columns)}"
    )


def geojson_to_gdf(path: Path, **kwargs) -> gpd.GeoDataFrame:
    """Load a GeoJSON file as a GeoDataFrame."""
    return gpd.read_file(path, driver="GeoJSON", **kwargs)


def gpkg_to_gdf(
    path: Path,
    layer_name: Optional[str] = None,
    **kwargs,
) -> gpd.GeoDataFrame:
    """Load a GeoPackage file as a GeoDataFrame."""
    if layer_name:
        return gpd.read_file(path, layer=layer_name, **kwargs)
    return gpd.read_file(path, **kwargs)


def kml_to_gdf(path: Path, **kwargs) -> gpd.GeoDataFrame:
    """Load a KML file as a GeoDataFrame."""
    import fiona
    fiona.drvsupport.supported_drivers["KML"] = "r"
    return gpd.read_file(path, driver="KML", **kwargs)


def kmz_to_gdf(
    path: Path,
    kml_name: Optional[str] = None,
    **kwargs,
) -> gpd.GeoDataFrame:
    """Load a KMZ file (zipped KML) as a GeoDataFrame."""
    import fiona
    fiona.drvsupport.supported_drivers["KML"] = "r"

    with zipfile.ZipFile(path, "r") as zf:
        kml_files = [n for n in zf.namelist() if n.lower().endswith(".kml")]
        if not kml_files:
            raise UnsupportedFormatError("No KML file found inside KMZ")
        target = kml_name if kml_name and kml_name in kml_files else kml_files[0]

        with tempfile.TemporaryDirectory() as tmpdir:
            kml_path = Path(tmpdir) / target
            kml_path.parent.mkdir(parents=True, exist_ok=True)
            kml_path.write_bytes(zf.read(target))
            return gpd.read_file(kml_path, driver="KML", **kwargs)


def shp_to_gdf(path: Path, **kwargs) -> gpd.GeoDataFrame:
    """Load a Shapefile or zipped Shapefile as a GeoDataFrame."""
    if path.suffix.lower() == ".zip":
        return gpd.read_file(f"zip://{path}", **kwargs)
    return gpd.read_file(path, **kwargs)


def load_vector(path: Path, **kwargs) -> gpd.GeoDataFrame:
    """Auto-detect format and load a vector file as a GeoDataFrame.

    Supported formats: CSV, GeoJSON, GeoPackage, KML, KMZ, Shapefile (+ zipped).
    """
    suffix = path.suffix.lower()
    loaders = {
        ".csv": csv_to_gdf,
        ".geojson": geojson_to_gdf,
        ".json": geojson_to_gdf,
        ".gpkg": gpkg_to_gdf,
        ".kml": kml_to_gdf,
        ".kmz": kmz_to_gdf,
        ".shp": shp_to_gdf,
        ".zip": shp_to_gdf,
    }
    loader = loaders.get(suffix)
    if loader is None:
        raise UnsupportedFormatError(f"No loader for format: {suffix}")
    logger.info("Loading vector: %s (format: %s)", path.name, suffix)
    return loader(path, **kwargs)
