"""DuckDB connection setup for Overture S3 queries and tile execution."""

import logging
import time

import duckdb
import pandas as pd

from geoetl.duckdb.engine import get_connection
from geoetl.overture.config import OvertureConfig

logger = logging.getLogger(__name__)


def get_overture_connection(config: OvertureConfig | None = None) -> duckdb.DuckDBPyConnection:
    """Get a DuckDB connection configured for Overture S3 access.

    Loads httpfs extension and sets S3 region, object cache, and memory limit
    on top of the base connection (which already has spatial + h3).
    """
    config = config or OvertureConfig()
    conn = get_connection()

    conn.execute("INSTALL httpfs")
    conn.execute("LOAD httpfs")
    conn.execute(f"SET s3_region = '{config.s3_region}'")
    conn.execute(f"SET enable_object_cache = {str(config.object_cache).lower()}")
    conn.execute(f"SET memory_limit = '{config.memory_limit}'")
    logger.info(
        "DuckDB configured for Overture: region=%s, memory=%s, cache=%s",
        config.s3_region, config.memory_limit, config.object_cache,
    )
    return conn


def s3_path(config: OvertureConfig, theme: str, type_name: str) -> str:
    """Build S3 glob path for an Overture theme/type."""
    return f"{config.s3_base}/theme={theme}/type={type_name}/*"


def query_tile(
    conn: duckdb.DuckDBPyConnection,
    sql: str,
    tile_key: str,
) -> pd.DataFrame:
    """Execute a tile query and return results as a DataFrame.

    Args:
        conn: DuckDB connection with httpfs loaded.
        sql: Complete SQL query with bbox predicates baked in.
        tile_key: Tile identifier for logging.

    Returns:
        DataFrame with h3_index as first column plus metric columns.
        Empty DataFrame if the query returns no rows.
    """
    t0 = time.monotonic()
    try:
        result = conn.execute(sql).fetchdf()
        elapsed = time.monotonic() - t0
        logger.debug("Tile %s: %d rows in %.1fs", tile_key, len(result), elapsed)
        return result
    except Exception:
        logger.exception("Tile %s query failed", tile_key)
        raise
