"""DuckDB connection management with spatial and H3 extensions."""

import logging
import threading
from contextlib import contextmanager

import duckdb
import geopandas as gpd
import pandas as pd
from shapely import wkb

logger = logging.getLogger(__name__)

_lock = threading.Lock()
_conn: duckdb.DuckDBPyConnection | None = None


def get_connection() -> duckdb.DuckDBPyConnection:
    """Return a thread-safe lazy singleton DuckDB connection.

    Installs and loads ``spatial`` and ``h3`` extensions on first call.
    """
    global _conn
    if _conn is not None:
        return _conn

    with _lock:
        if _conn is not None:
            return _conn

        conn = duckdb.connect()
        # spatial is a core extension; h3 is a community extension
        conn.execute("INSTALL spatial")
        conn.execute("LOAD spatial")
        logger.info("DuckDB: loaded spatial extension")

        conn.execute("INSTALL h3 FROM community")
        conn.execute("LOAD h3")
        logger.info("DuckDB: loaded h3 extension (community)")

        _conn = conn
        return _conn


def close_connection() -> None:
    """Close the singleton connection (useful for tests)."""
    global _conn
    with _lock:
        if _conn is not None:
            _conn.close()
            _conn = None


@contextmanager
def registered_table(conn: duckdb.DuckDBPyConnection, name: str, df: pd.DataFrame):
    """Register a DataFrame as a DuckDB table, unregister on exit."""
    conn.register(name, df)
    try:
        yield name
    finally:
        conn.unregister(name)


def gdf_to_table(conn: duckdb.DuckDBPyConnection, gdf: gpd.GeoDataFrame, name: str) -> None:
    """Register a GeoDataFrame as a DuckDB table, converting geometry to WKB bytes."""
    df = pd.DataFrame(gdf.drop(columns="geometry"))
    if "geometry" in gdf.columns or gdf.geometry is not None:
        df["geom_wkb"] = gdf.geometry.to_wkb()
    conn.register(name, df)


def query_to_gdf(
    conn: duckdb.DuckDBPyConnection,
    sql: str,
    geometry_col: str = "geom_wkb",
    crs: str = "EPSG:4326",
) -> gpd.GeoDataFrame:
    """Execute SQL and convert a WKB geometry column back to a GeoDataFrame."""
    df = conn.execute(sql).fetchdf()
    if geometry_col not in df.columns:
        return gpd.GeoDataFrame(df, crs=crs)

    geometries = df[geometry_col].apply(lambda b: wkb.loads(bytes(b)) if isinstance(b, (bytes, memoryview, bytearray)) else None)
    return gpd.GeoDataFrame(
        df.drop(columns=geometry_col),
        geometry=geometries.values,
        crs=crs,
    )
