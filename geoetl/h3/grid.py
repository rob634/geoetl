"""H3 grid generation, loading, and centroid extraction."""

import json
import logging
from pathlib import Path

import geopandas as gpd
import h3
import numpy as np
import pandas as pd
from shapely.geometry import Polygon, shape

from geoetl.duckdb.engine import get_connection, query_to_gdf, registered_table
from geoetl.exceptions import H3Error

logger = logging.getLogger(__name__)


def h3_to_polygon(h3_index: str) -> Polygon:
    """Convert an H3 index to a shapely Polygon."""
    boundary = h3.cell_to_boundary(h3_index)
    # h3 returns (lat, lng); shapely needs (lng, lat)
    coords = [(lng, lat) for lat, lng in boundary]
    coords.append(coords[0])
    return Polygon(coords)


def h3_to_geodataframe(h3_indices: list[str]) -> gpd.GeoDataFrame:
    """Convert a list of H3 indices to a GeoDataFrame with polygon geometries.

    Uses DuckDB h3 extension for vectorized conversion.

    Args:
        h3_indices: List of H3 cell index strings.

    Returns:
        GeoDataFrame with h3_index, lat, lon, resolution columns and polygon geometry.
    """
    if not h3_indices:
        return gpd.GeoDataFrame(columns=["h3_index", "lat", "lon", "resolution", "geometry"], crs="EPSG:4326")

    conn = get_connection()
    df = pd.DataFrame({"h3_index": h3_indices})

    with registered_table(conn, "_h3_input", df):
        gdf = query_to_gdf(
            conn,
            """
            SELECT h3_index,
                   h3_cell_to_lat(h3_index) AS lat,
                   h3_cell_to_lng(h3_index) AS lon,
                   h3_get_resolution(h3_index) AS resolution,
                   ST_AsWKB(ST_GeomFromText(h3_cell_to_boundary_wkt(h3_index))) AS geom_wkb
            FROM _h3_input
            """,
        )

    return gdf


def load_grid(path: Path) -> gpd.GeoDataFrame:
    """Load a pre-built H3 grid from GeoParquet or GeoJSON.

    For GeoJSON, uses json.load + shapely to avoid pyproj CRS issues
    that some environments have with gpd.read_file().

    Args:
        path: Path to grid file (.parquet, .geojson, .json).

    Returns:
        GeoDataFrame with h3_index column and polygon geometry.
    """
    path = Path(path)
    suffix = path.suffix.lower()

    if suffix == ".parquet":
        gdf = gpd.read_parquet(path)
    elif suffix in (".geojson", ".json"):
        gdf = _load_geojson_safe(path)
    else:
        raise H3Error(f"Unsupported grid format: {suffix}")

    # Normalize column names
    if "cell_id" in gdf.columns and "h3_index" not in gdf.columns:
        gdf = gdf.rename(columns={"cell_id": "h3_index"})

    if "h3_index" not in gdf.columns:
        raise H3Error("Grid must have 'h3_index' or 'cell_id' column")

    logger.info("Loaded H3 grid: %d hexagons from %s", len(gdf), path.name)
    return gdf


def _load_geojson_safe(path: Path) -> gpd.GeoDataFrame:
    """Load GeoJSON via json.load to avoid pyproj database issues."""
    with open(path) as f:
        data = json.load(f)

    features = data["features"]
    records = []
    geometries = []

    for feature in features:
        records.append(feature["properties"])
        geometries.append(shape(feature["geometry"]))

    return gpd.GeoDataFrame(records, geometry=geometries, crs="EPSG:4326")


def extract_centroids(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Add lat/lon centroid columns to an H3 GeoDataFrame.

    If 'lat' and 'lon' columns already exist, returns as-is.
    Uses DuckDB h3 extension for vectorized centroid extraction.

    Args:
        gdf: GeoDataFrame with h3_index column.

    Returns:
        GeoDataFrame with lat and lon columns added.
    """
    if "lat" in gdf.columns and "lon" in gdf.columns:
        return gdf

    if "h3_index" not in gdf.columns:
        raise H3Error("GeoDataFrame must have 'h3_index' column")

    conn = get_connection()
    h3_series = gdf["h3_index"].reset_index(drop=True)
    df_input = pd.DataFrame({"h3_index": h3_series})

    with registered_table(conn, "_h3_centroids", df_input):
        centroids = conn.execute(
            "SELECT h3_index, h3_cell_to_lat(h3_index) AS lat, h3_cell_to_lng(h3_index) AS lon FROM _h3_centroids"
        ).fetchdf()

    centroid_map = centroids.set_index("h3_index")
    gdf = gdf.copy()
    gdf["lat"] = gdf["h3_index"].map(centroid_map["lat"])
    gdf["lon"] = gdf["h3_index"].map(centroid_map["lon"])
    return gdf


def generate_grid(resolution: int, bounds: tuple[float, float, float, float] | None = None) -> gpd.GeoDataFrame:
    """Generate H3 hexagons covering a bounding box.

    Tiles the globe in 60-degree longitude strips to avoid antimeridian issues
    with h3.geo_to_cells() on wide polygons.

    Args:
        resolution: H3 resolution (0-15). L5 (~252 km2) is typical for global work.
        bounds: (west, south, east, north) in WGS84. Defaults to global land bounds.

    Returns:
        GeoDataFrame with h3_index and polygon geometry.
    """
    if bounds is None:
        bounds = (-180.0, -60.0, 180.0, 75.0)  # Exclude Antarctica

    west, south, east, north = bounds

    # Tile in 60° longitude strips to avoid antimeridian/wide-polygon issues
    cells: set[str] = set()
    strip_width = 60.0
    lon_start = west
    while lon_start < east:
        lon_end = min(lon_start + strip_width, east)
        polygon = {
            "type": "Polygon",
            "coordinates": [
                [[lon_start, south], [lon_end, south], [lon_end, north], [lon_start, north], [lon_start, south]]
            ],
        }
        strip_cells = h3.geo_to_cells(polygon, res=resolution)
        cells.update(strip_cells)
        logger.info("Strip [%.0f, %.0f]: %d cells", lon_start, lon_end, len(strip_cells))
        lon_start = lon_end

    logger.info("Generated %d H3 cells at resolution %d", len(cells), resolution)
    return h3_to_geodataframe(list(cells))


def filter_grid_to_raster(
    grid: gpd.GeoDataFrame, raster_path: Path, exclude_zero: bool = True
) -> gpd.GeoDataFrame:
    """Filter H3 grid to only cells whose centroid falls on a finite raster pixel.

    Useful for creating a 'land-only' grid from raster data coverage.

    Args:
        grid: H3 GeoDataFrame with h3_index column.
        raster_path: Path to a reference raster file.
        exclude_zero: If True (default), also exclude cells where the pixel value is exactly zero.
            Set to False to keep cells with legitimate zero values.

    Returns:
        Filtered GeoDataFrame containing only cells with valid raster data.
    """
    import rasterio
    from rasterio.transform import rowcol

    grid = extract_centroids(grid)
    lats = grid["lat"].values
    lons = grid["lon"].values

    with rasterio.open(raster_path) as src:
        rows, cols = rowcol(src.transform, lons, lats)
        rows = np.array(rows)
        cols = np.array(cols)

        # Mask out-of-bounds centroids
        valid_bounds = (
            (rows >= 0) & (rows < src.height) &
            (cols >= 0) & (cols < src.width)
        )

        data = src.read(1)
        nodata = src.nodata

    # Check pixel values at centroid locations
    has_data = np.zeros(len(grid), dtype=bool)
    in_bounds = np.where(valid_bounds)[0]
    pixel_vals = data[rows[in_bounds], cols[in_bounds]].astype(float)

    finite_mask = np.isfinite(pixel_vals)
    if nodata is not None:
        finite_mask &= pixel_vals != nodata
    if exclude_zero:
        finite_mask &= pixel_vals != 0

    has_data[in_bounds[finite_mask]] = True

    filtered = grid[has_data].copy()
    logger.info("Filtered grid: %d/%d cells have raster data", len(filtered), len(grid))
    return filtered


# Default land polygon path (Natural Earth 10m)
_LAND_POLYGON_PATH = Path(__file__).parent.parent.parent / "data" / "reference" / "ne_10m_land.zip"


def filter_grid_to_land(
    grid: gpd.GeoDataFrame,
    land_path: Path | None = None,
    raster_resolution: float = 0.1,
    all_touched: bool = True,
) -> gpd.GeoDataFrame:
    """Filter H3 grid to land-only cells using a rasterized land polygon lookup.

    Rasterizes the land polygon to a coarse grid (default 0.1° ~ 11km), then
    checks each H3 centroid against the raster. This is orders of magnitude
    faster than point-in-polygon tests on complex coastline geometry.

    Args:
        grid: H3 GeoDataFrame with h3_index column.
        land_path: Path to land polygon file (shapefile/zip).
            Defaults to data/reference/ne_10m_land.zip (Natural Earth 10m).
        raster_resolution: Resolution of the intermediate raster in degrees.
            Default 0.1° (~11km) matches H3 L5 well.
        all_touched: If True, rasterize marks all pixels touched by land polygons
            (better coastal coverage). Default True.

    Returns:
        Filtered GeoDataFrame containing only land cells.
    """
    from rasterio.features import rasterize
    from rasterio.transform import from_bounds

    land_path = Path(land_path) if land_path is not None else _LAND_POLYGON_PATH
    if not land_path.exists():
        raise H3Error(f"Land polygon file not found: {land_path}")

    # Load and rasterize land polygons
    land = gpd.read_file(land_path)
    res = raster_resolution
    width = int(360 / res)
    height = int(180 / res)
    transform = from_bounds(-180, -90, 180, 90, width, height)

    land_raster = rasterize(
        [(geom, 1) for geom in land.geometry],
        out_shape=(height, width),
        transform=transform,
        fill=0,
        dtype="uint8",
        all_touched=all_touched,
    )
    logger.info("Rasterized land: %dx%d, %d land pixels", width, height, land_raster.sum())

    # Centroid lookup against raster
    grid = extract_centroids(grid)
    lons = grid["lon"].values
    lats = grid["lat"].values

    cols = ((lons + 180) / res).astype(int).clip(0, width - 1)
    rows = ((90 - lats) / res).astype(int).clip(0, height - 1)
    is_land = land_raster[rows, cols] == 1

    filtered = grid[is_land].copy()
    logger.info(
        "Land filter: %d/%d cells are land (%.1f%%)",
        len(filtered), len(grid), len(filtered) / len(grid) * 100,
    )
    return filtered
