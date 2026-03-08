"""SQL template builders for Overture → H3 aggregation by theme."""

from geoetl.overture.config import BUILDING_SUBTYPES, ROAD_CLASSES, OvertureConfig

# Column names produced by each theme (for merge/rollup reference)
TRANSPORT_COLUMNS = [f"{cls}_length_m" for cls in ROAD_CLASSES] + [
    "rail_length_m",
    "road_segment_count",
    "total_road_length_m",
]

BUILDING_COLUMNS = [
    "building_count",
    "building_area_m2",
    *[f"{st}_building_count" for st in BUILDING_SUBTYPES],
    "buildings_with_height_count",
    "total_building_height_m",
    "avg_building_height_m",
]

PLACES_COLUMNS = ["poi_count"]

ALL_COLUMNS = TRANSPORT_COLUMNS + BUILDING_COLUMNS + PLACES_COLUMNS


def _h3_centroid_expr(resolution: int) -> str:
    """SQL expression to assign a feature's centroid to an H3 cell (hex string)."""
    return (
        f"h3_h3_to_string(h3_latlng_to_cell("
        f"ST_Y(ST_Centroid(geometry)), "
        f"ST_X(ST_Centroid(geometry)), "
        f"{resolution}))"
    )


def _bbox_predicate(west: float, south: float, east: float, north: float) -> str:
    """SQL WHERE clause for Overture bbox column predicate pushdown."""
    return (
        f"bbox.xmin <= {east} AND bbox.xmax >= {west} "
        f"AND bbox.ymin <= {north} AND bbox.ymax >= {south}"
    )


def _centroid_in_tile(west: float, south: float, east: float, north: float) -> str:
    """SQL WHERE clause ensuring a feature's centroid falls within the tile.

    Uses >= on west/south and < on east/north so each centroid belongs to
    exactly one tile. The bbox predicate (above) is kept for Parquet row-group
    pruning; this filter deduplicates within the pruned result.
    """
    return (
        f"ST_X(ST_Centroid(geometry)) >= {west} AND ST_X(ST_Centroid(geometry)) < {east} "
        f"AND ST_Y(ST_Centroid(geometry)) >= {south} AND ST_Y(ST_Centroid(geometry)) < {north}"
    )


def transport_sql(
    west: float, south: float, east: float, north: float,
    config: OvertureConfig | None = None,
) -> str:
    """Build SQL for transportation segment aggregation within a bbox tile.

    Computes road length per class, rail length, segment count, and total road length,
    all grouped by H3 cell.
    """
    config = config or OvertureConfig()
    parquet = f"{config.s3_base}/theme=transportation/type=segment/*"
    h3_expr = _h3_centroid_expr(config.h3_resolution)
    bbox = _bbox_predicate(west, south, east, north)
    centroid = _centroid_in_tile(west, south, east, north)

    # Per-class road length columns
    class_cases = ",\n    ".join(
        f"COALESCE(SUM(CASE WHEN class = '{cls}' "
        f"THEN ST_Length_Spheroid(geometry) ELSE 0 END), 0) AS {cls}_length_m"
        for cls in ROAD_CLASSES
    )

    return f"""
SELECT
    {h3_expr} AS h3_index,
    {class_cases},
    COALESCE(SUM(CASE WHEN subtype = 'rail'
        THEN ST_Length_Spheroid(geometry) ELSE 0 END), 0) AS rail_length_m,
    COUNT(*) FILTER (WHERE subtype = 'road') AS road_segment_count,
    COALESCE(SUM(ST_Length_Spheroid(geometry))
        FILTER (WHERE subtype = 'road'), 0) AS total_road_length_m
FROM read_parquet('{parquet}')
WHERE {bbox}
  AND {centroid}
  AND subtype IN ('road', 'rail')
GROUP BY 1
HAVING h3_index IS NOT NULL
"""


def buildings_sql(
    west: float, south: float, east: float, north: float,
    config: OvertureConfig | None = None,
) -> str:
    """Build SQL for building aggregation within a bbox tile.

    Computes building count, total area, per-subtype counts, total height sum,
    and average height, all grouped by H3 cell.
    """
    config = config or OvertureConfig()
    parquet = f"{config.s3_base}/theme=buildings/type=building/*"
    h3_expr = _h3_centroid_expr(config.h3_resolution)
    bbox = _bbox_predicate(west, south, east, north)
    centroid = _centroid_in_tile(west, south, east, north)

    subtype_counts = ",\n    ".join(
        f"COUNT(*) FILTER (WHERE subtype = '{st}') AS {st}_building_count"
        for st in BUILDING_SUBTYPES
    )

    return f"""
SELECT
    {h3_expr} AS h3_index,
    COUNT(*) AS building_count,
    COALESCE(SUM(ST_Area_Spheroid(geometry)), 0) AS building_area_m2,
    {subtype_counts},
    COUNT(*) FILTER (WHERE height IS NOT NULL) AS buildings_with_height_count,
    SUM(height) AS total_building_height_m,
    CASE WHEN COUNT(*) FILTER (WHERE height IS NOT NULL) > 0
        THEN SUM(height) / COUNT(*) FILTER (WHERE height IS NOT NULL)
        ELSE NULL END AS avg_building_height_m
FROM read_parquet('{parquet}')
WHERE {bbox}
  AND {centroid}
GROUP BY 1
HAVING h3_index IS NOT NULL
"""


def places_sql(
    west: float, south: float, east: float, north: float,
    config: OvertureConfig | None = None,
) -> str:
    """Build SQL for places (POI) aggregation within a bbox tile.

    Counts points of interest per H3 cell.
    """
    config = config or OvertureConfig()
    parquet = f"{config.s3_base}/theme=places/type=place/*"
    h3_expr = _h3_centroid_expr(config.h3_resolution)
    bbox = _bbox_predicate(west, south, east, north)
    centroid = _centroid_in_tile(west, south, east, north)

    return f"""
SELECT
    {h3_expr} AS h3_index,
    COUNT(*) AS poi_count
FROM read_parquet('{parquet}')
WHERE {bbox}
  AND {centroid}
GROUP BY 1
HAVING h3_index IS NOT NULL
"""


# Map theme name to (sql_builder, column_list) for pipeline dispatch
THEME_REGISTRY: dict[str, tuple] = {
    "transportation": (transport_sql, TRANSPORT_COLUMNS),
    "buildings": (buildings_sql, BUILDING_COLUMNS),
    "places": (places_sql, PLACES_COLUMNS),
}
