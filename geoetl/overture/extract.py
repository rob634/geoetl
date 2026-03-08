"""SQL templates for extracting raw Overture road features and computing statistics.

Three SQL builders for the road pipeline:
  - road_extract_sql: raw feature extraction from S3 (Mode A download)
  - road_local_stats_sql: stats from local GeoParquet (Mode A stats)
  - road_remote_stats_sql: stats directly from S3 (Mode B)

Local and remote stats produce identical schemas for comparison.
"""

from geoetl.overture.aggregation import _bbox_predicate, _centroid_in_tile, _h3_centroid_expr
from geoetl.overture.config import ROAD_CLASSES, OvertureConfig


def road_extract_sql(
    west: float,
    south: float,
    east: float,
    north: float,
    config: OvertureConfig | None = None,
    classes: list[str] | None = None,
    all_classes: bool = False,
) -> str:
    """SQL to extract raw road segments within a tile bbox.

    Returns individual features with H3 cell assignment, pre-computed
    geodesic length, and connector IDs for network analysis.

    Args:
        all_classes: If True, extract all road segments (subtype='road')
            regardless of class. Equivalent to OSM highway != NULL.
    """
    config = config or OvertureConfig()
    parquet = f"{config.s3_base}/theme=transportation/type=segment/*"
    h3_expr = _h3_centroid_expr(config.h3_resolution)
    bbox = _bbox_predicate(west, south, east, north)
    centroid = _centroid_in_tile(west, south, east, north)

    if all_classes:
        class_filter = ""
    else:
        classes = classes or ROAD_CLASSES
        class_in = ", ".join(f"'{c}'" for c in classes)
        class_filter = f"\n  AND class IN ({class_in})"

    return f"""
SELECT
    id,
    sources[1].dataset AS source_dataset,
    sources[1].record_id AS osm_id,
    class,
    connectors,
    ST_Length_Spheroid(geometry) AS length_m,
    {h3_expr} AS h3_l5,
    ST_AsWKB(geometry) AS geom_wkb
FROM read_parquet('{parquet}')
WHERE {bbox}
  AND {centroid}
  AND subtype = 'road'{class_filter}
"""


def road_local_stats_sql(
    parquet_path: str,
    classes: list[str] | None = None,
) -> str:
    """SQL to compute road statistics from a local extracted parquet.

    Uses pre-computed length_m column. Output schema matches
    road_remote_stats_sql for direct comparison.
    """
    classes = classes or ROAD_CLASSES

    class_cases = ",\n    ".join(
        f"COALESCE(SUM(CASE WHEN class = '{cls}' "
        f"THEN length_m ELSE 0 END), 0) AS {cls}_length_m"
        for cls in classes
    )

    return f"""
SELECT
    h3_l5 AS h3_index,
    {class_cases},
    COUNT(*) AS road_segment_count,
    COALESCE(SUM(length_m), 0) AS total_road_length_m
FROM read_parquet('{parquet_path}')
GROUP BY h3_l5
HAVING h3_l5 IS NOT NULL
ORDER BY h3_l5
"""


def road_remote_stats_sql(
    west: float,
    south: float,
    east: float,
    north: float,
    config: OvertureConfig | None = None,
    classes: list[str] | None = None,
) -> str:
    """SQL to compute road statistics directly from Overture S3.

    Same output schema as road_local_stats_sql for comparison.
    Computes ST_Length_Spheroid on the fly (no pre-computed length).
    """
    config = config or OvertureConfig()
    classes = classes or ROAD_CLASSES
    parquet = f"{config.s3_base}/theme=transportation/type=segment/*"
    h3_expr = _h3_centroid_expr(config.h3_resolution)
    bbox = _bbox_predicate(west, south, east, north)
    centroid = _centroid_in_tile(west, south, east, north)
    class_in = ", ".join(f"'{c}'" for c in classes)

    class_cases = ",\n    ".join(
        f"COALESCE(SUM(CASE WHEN class = '{cls}' "
        f"THEN ST_Length_Spheroid(geometry) ELSE 0 END), 0) AS {cls}_length_m"
        for cls in classes
    )

    return f"""
SELECT
    {h3_expr} AS h3_index,
    {class_cases},
    COUNT(*) AS road_segment_count,
    COALESCE(SUM(ST_Length_Spheroid(geometry)), 0) AS total_road_length_m
FROM read_parquet('{parquet}')
WHERE {bbox}
  AND {centroid}
  AND subtype = 'road'
  AND class IN ({class_in})
GROUP BY 1
HAVING h3_index IS NOT NULL
"""


def merge_extracts_sql(tiles_glob: str, output_path: str) -> str:
    """SQL to merge per-tile extract parquets, add H3 parent columns, sort.

    Uses DuckDB COPY TO for efficient out-of-core merge+sort.
    """
    return f"""
COPY (
    SELECT *,
        h3_h3_to_string(h3_cell_to_parent(h3_string_to_h3(h3_l5), 4)) AS h3_l4,
        h3_h3_to_string(h3_cell_to_parent(h3_string_to_h3(h3_l5), 3)) AS h3_l3
    FROM read_parquet('{tiles_glob}')
    ORDER BY h3_l5
) TO '{output_path}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000)
"""
