"""H3 level rollup: aggregate from fine resolution to coarser parent cells.

For intensive variables like yield, use derive_spec to recompute from
rolled-up extensive variables rather than averaging child values directly.
See docs/mapspam-h3-aggregation-methodology.md.
"""

import logging

import geopandas as gpd
import h3
import pandas as pd

from geoetl.duckdb.engine import get_connection, query_to_gdf, registered_table
from geoetl.exceptions import H3Error

logger = logging.getLogger(__name__)

_AGG_MAP = {
    "sum": "SUM",
    "mean": "AVG",
    "median": "MEDIAN",
    "min": "MIN",
    "max": "MAX",
    "first": "FIRST",
    "last": "LAST",
    "count": "COUNT",
    "mode": "MODE",
}


def rollup(
    gdf: gpd.GeoDataFrame,
    target_resolution: int,
    agg_spec: dict[str, str],
    derive_spec: dict[str, tuple[str, str, float]] | None = None,
    h3_col: str = "h3_index",
) -> gpd.GeoDataFrame:
    """Aggregate H3 data from fine resolution to coarser parent cells.

    Uses DuckDB for vectorized parent mapping, aggregation, derived columns,
    and geometry generation in a single SQL query.

    Args:
        gdf: GeoDataFrame with H3 index column and data columns.
        target_resolution: Target H3 resolution (must be coarser than source).
        agg_spec: Mapping of column name to aggregation type.
            Supported: "sum", "mean", "median", "min", "max", "first",
            "last", "count", "mode".
        derive_spec: Mapping of derived column name to (numerator_col, denominator_col, scale).
            These columns are computed AFTER aggregation as numerator/denominator * scale.
            Use this for intensive variables like yield that must be derived from
            rolled-up extensive variables, never averaged directly.
            e.g. {"yield_kgha": ("production_mt", "harv_area_ha", 1000.0)}
        h3_col: Name of the H3 index column.

    Returns:
        GeoDataFrame at target resolution with aggregated values and
        new polygon geometries.

    Example:
        >>> rollup(gdf, 4,
        ...     agg_spec={
        ...         "production_mt": "sum",
        ...         "harv_area_ha": "sum",
        ...         "ISO_A3": "mode",
        ...     },
        ...     derive_spec={
        ...         "yield_kgha": ("production_mt", "harv_area_ha", 1000.0),
        ...     },
        ... )
    """
    if h3_col not in gdf.columns:
        raise H3Error(f"Column '{h3_col}' not found in GeoDataFrame")

    # Validate target resolution is coarser
    sample_idx = gdf[h3_col].iloc[0]
    source_res = h3.get_resolution(sample_idx)
    if target_resolution >= source_res:
        raise H3Error(
            f"Target resolution {target_resolution} must be coarser (lower) "
            f"than source resolution {source_res}"
        )

    # Validate agg_spec columns and methods
    agg_columns = []
    for col, method in agg_spec.items():
        if col not in gdf.columns:
            logger.warning("Column '%s' not in GeoDataFrame, skipping", col)
            continue
        if method not in _AGG_MAP:
            raise H3Error(f"Unknown aggregation method: {method}")
        agg_columns.append((col, method))

    # Build SQL aggregation expressions
    parent_expr = f"h3_cell_to_parent({h3_col}, {target_resolution})"
    select_parts = [f"{parent_expr} AS h3_index"]

    for col, method in agg_columns:
        sql_agg = _AGG_MAP[method]
        select_parts.append(f'{sql_agg}("{col}") AS "{col}"')

    # Derived columns (e.g. yield = production / area * scale)
    if derive_spec:
        agg_dict = dict(agg_columns)
        for col_name, (num_col, denom_col, scale) in derive_spec.items():
            if num_col not in agg_dict:
                logger.warning("Numerator '%s' not in agg_spec, skipping %s", num_col, col_name)
                continue
            if denom_col not in agg_dict:
                logger.warning("Denominator '%s' not in agg_spec, skipping %s", denom_col, col_name)
                continue
            if agg_dict[num_col] != "sum":
                raise H3Error(
                    f"derive_spec requires 'sum' aggregation for numerator '{num_col}', "
                    f"got '{agg_dict[num_col]}'"
                )
            if agg_dict[denom_col] != "sum":
                raise H3Error(
                    f"derive_spec requires 'sum' aggregation for denominator '{denom_col}', "
                    f"got '{agg_dict[denom_col]}'"
                )
            select_parts.append(
                f'CASE WHEN SUM("{denom_col}") > 0 '
                f'THEN ROUND((SUM("{num_col}") / SUM("{denom_col}")) * {scale}, 2) '
                f'ELSE NULL END AS "{col_name}"'
            )

    # Geometry from parent cell boundary
    select_parts.append(
        f"ST_AsWKB(ST_GeomFromText(h3_cell_to_boundary_wkt({parent_expr}))) AS geom_wkb"
    )

    sql = f"""
        SELECT {', '.join(select_parts)}
        FROM _rollup_input
        GROUP BY {parent_expr}
    """

    conn = get_connection()
    # Register input — drop geometry column, keep data columns
    input_df = pd.DataFrame(gdf.drop(columns="geometry"))

    with registered_table(conn, "_rollup_input", input_df):
        result = query_to_gdf(conn, sql)

    # Round float columns
    float_cols = result.select_dtypes(include=["float64", "float32"]).columns
    for col in float_cols:
        result[col] = result[col].round(2)

    n_parents = len(result)
    avg_children = len(gdf) / n_parents if n_parents > 0 else 0
    logger.info(
        "Rollup L%d -> L%d: %d cells -> %d parents (avg %.1f children)",
        source_res, target_resolution, len(gdf), n_parents, avg_children,
    )
    logger.info("Rollup complete: %d parent cells", n_parents)

    return result
