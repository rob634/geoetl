"""Aggregate Overture Maps GeoParquet onto H3 grids at levels 3, 4, and 5.

Queries Overture S3 GeoParquet via DuckDB, computing geometry metrics
(road lengths, building areas, POI counts) per H3 cell. Outputs GeoParquet
at L5/L4/L3 for cross-dataset joins with MapSPAM.

Usage:
    python scripts/aggregate_overture.py                     # Full run (all themes)
    python scripts/aggregate_overture.py --test              # Single test tile
    python scripts/aggregate_overture.py --theme transport    # One theme only
    python scripts/aggregate_overture.py --skip-fetch         # Merge+rollup only
"""

import argparse
import logging
import time
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd

from geoetl.h3.grid import h3_to_geodataframe
from geoetl.h3.rollup import rollup
from geoetl.overture.aggregation import (
    ALL_COLUMNS,
    BUILDING_COLUMNS,
    PLACES_COLUMNS,
    THEME_REGISTRY,
    TRANSPORT_COLUMNS,
)
from geoetl.overture.client import get_overture_connection, query_tile
from geoetl.overture.config import OvertureConfig
from geoetl.pipeline.checkpoint import CheckpointManager

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

OUTPUT_DIR = Path("outputs/overture")
CHECKPOINT_DIR = OUTPUT_DIR / "checkpoints"

# Test tile: covers London area
TEST_BBOX = (-1.0, 51.0, 1.0, 53.0)

THEME_ALIASES = {
    "transport": "transportation",
    "transportation": "transportation",
    "building": "buildings",
    "buildings": "buildings",
    "place": "places",
    "places": "places",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("overture")


# ---------------------------------------------------------------------------
# Phase 1: Tile generation
# ---------------------------------------------------------------------------

def generate_tiles(
    tile_degrees: float = 5.0,
    bounds: tuple[float, float, float, float] = (-180.0, -60.0, 180.0, 75.0),
) -> list[dict]:
    """Partition the globe into rectangular bbox tiles.

    Returns list of dicts with keys: key, west, south, east, north.
    Default bounds exclude Antarctica and Arctic.
    """
    west_min, south_min, east_max, north_max = bounds
    tiles = []
    lon = west_min
    while lon < east_max:
        lat = south_min
        lon_end = min(lon + tile_degrees, east_max)
        while lat < north_max:
            lat_end = min(lat + tile_degrees, north_max)
            key = f"{lon:+07.1f}_{lat:+06.1f}"
            tiles.append({
                "key": key,
                "west": lon,
                "south": lat,
                "east": lon_end,
                "north": lat_end,
            })
            lat = lat_end
        lon = lon_end

    logger.info("Generated %d tiles (%.0f° grid)", len(tiles), tile_degrees)
    return tiles


# ---------------------------------------------------------------------------
# Phase 3: Fetch + Aggregate
# ---------------------------------------------------------------------------

def fetch_theme_tiles(
    theme: str,
    tiles: list[dict],
    config: OvertureConfig,
) -> None:
    """Query Overture S3 for one theme across all tiles, checkpointing each."""
    sql_builder, columns = THEME_REGISTRY[theme]
    theme_cp_dir = CHECKPOINT_DIR / theme
    theme_cp_dir.mkdir(parents=True, exist_ok=True)

    checkpoint = CheckpointManager(theme_cp_dir / "checkpoint.json")
    checkpoint.load()

    conn = get_overture_connection(config)

    total = len(tiles)
    done = 0
    skipped = 0

    for tile in tiles:
        tile_key = tile["key"]
        done += 1

        if checkpoint.is_done(tile_key):
            skipped += 1
            continue

        logger.info(
            "[%d/%d] %s tile %s (%.1f,%.1f)-(%.1f,%.1f)",
            done, total, theme, tile_key,
            tile["west"], tile["south"], tile["east"], tile["north"],
        )

        sql = sql_builder(
            west=tile["west"], south=tile["south"],
            east=tile["east"], north=tile["north"],
            config=config,
        )

        t0 = time.time()
        try:
            df = query_tile(conn, sql, tile_key)
        except Exception:
            logger.exception("Failed tile %s/%s, skipping", theme, tile_key)
            continue

        elapsed = time.time() - t0

        if len(df) > 0:
            out_path = theme_cp_dir / f"{tile_key}.parquet"
            df.to_parquet(out_path, index=False)
            logger.info("  -> %d cells, %.1fs, saved %s", len(df), elapsed, out_path.name)
        else:
            logger.debug("  -> 0 cells, %.1fs (empty tile)", elapsed)

        checkpoint.mark_done(tile_key)

    logger.info(
        "%s fetch complete: %d/%d tiles (%d from checkpoint)",
        theme, done, total, skipped,
    )


# ---------------------------------------------------------------------------
# Phase 4: Merge
# ---------------------------------------------------------------------------

def merge_theme(theme: str) -> pd.DataFrame:
    """Merge all tile parquet fragments for a theme into one DataFrame."""
    theme_cp_dir = CHECKPOINT_DIR / theme
    parts = sorted(theme_cp_dir.glob("*.parquet"))

    if not parts:
        logger.warning("No checkpoint files for %s", theme)
        return pd.DataFrame()

    dfs = [pd.read_parquet(p) for p in parts]
    merged = pd.concat(dfs, ignore_index=True)

    # Drop avg_building_height_m before summing — it will be re-derived from totals.
    if "avg_building_height_m" in merged.columns:
        merged = merged.drop(columns=["avg_building_height_m"])

    # Sum all numeric columns per h3_index to consolidate overlapping tiles.
    numeric_cols = [c for c in merged.columns if c != "h3_index"]
    merged = merged.groupby("h3_index", as_index=False)[numeric_cols].sum()

    logger.info("Merged %s: %d files -> %d cells, %d columns", theme, len(parts), len(merged), len(numeric_cols))
    return merged


def merge_all_themes(themes: list[str]) -> gpd.GeoDataFrame:
    """Merge all themes into a single wide GeoDataFrame on h3_index.

    Builds geometry from the H3 indices found in the data (no pre-existing grid needed).
    """
    theme_dfs: list[pd.DataFrame] = []
    for theme in themes:
        df = merge_theme(theme)
        if not df.empty:
            theme_dfs.append(df)

    if not theme_dfs:
        logger.warning("No data from any theme")
        return gpd.GeoDataFrame(columns=["h3_index", "geometry"])

    # Full outer join all themes on h3_index
    base = theme_dfs[0]
    for df in theme_dfs[1:]:
        base = base.merge(df, on="h3_index", how="outer")

    # Fill NaN with 0 for count/length columns, leave height columns as NaN where appropriate
    height_cols = {"total_building_height_m", "avg_building_height_m"}
    metric_cols = [c for c in base.columns if c != "h3_index"]
    for c in metric_cols:
        if c not in height_cols:
            base[c] = base[c].fillna(0).astype("float32")
        else:
            base[c] = base[c].astype("float32")

    # Re-derive avg_building_height from totals using correct denominator
    if "total_building_height_m" in base.columns and "buildings_with_height_count" in base.columns:
        mask = base["buildings_with_height_count"] > 0
        base["avg_building_height_m"] = np.where(
            mask,
            base["total_building_height_m"] / base["buildings_with_height_count"],
            np.nan,
        ).astype("float32")

    logger.info("Merged %d themes -> %d cells, %d columns", len(theme_dfs), len(base), len(metric_cols))

    # Build geometry from H3 indices
    h3_indices = base["h3_index"].tolist()
    geo_gdf = h3_to_geodataframe(h3_indices)
    base = base.merge(geo_gdf[["h3_index", "geometry"]], on="h3_index", how="left")
    base = gpd.GeoDataFrame(base, geometry="geometry", crs="EPSG:4326")
    return base


# ---------------------------------------------------------------------------
# Phase 5: Rollup
# ---------------------------------------------------------------------------

def rollup_levels(l5: gpd.GeoDataFrame) -> None:
    """Roll up L5 to L4 and L3 with proper derived columns."""
    # All columns are extensive (summable) except avg_building_height
    metric_cols = [c for c in l5.columns if c not in ("h3_index", "geometry")]
    agg_spec = {c: "sum" for c in metric_cols if c != "avg_building_height_m"}

    derive_spec: dict[str, tuple[str, str, float]] | None = None
    if "total_building_height_m" in l5.columns and "buildings_with_height_count" in l5.columns:
        derive_spec = {
            "avg_building_height_m": ("total_building_height_m", "buildings_with_height_count", 1.0),
        }

    for target, label in [(4, "L4"), (3, "L3")]:
        logger.info("Rolling up to %s...", label)
        t0 = time.time()
        rolled = rollup(l5, target_resolution=target, agg_spec=agg_spec, derive_spec=derive_spec)
        logger.info("%s: %d rows (%.1fs)", label, len(rolled), time.time() - t0)

        out_path = OUTPUT_DIR / f"overture_h3l{target}.parquet"
        rolled.to_parquet(out_path)
        logger.info("Saved: %s", out_path)

        # Feed L4 into L3 rollup
        if target == 4:
            l5 = rolled


# ---------------------------------------------------------------------------
# Phase 6: Validate
# ---------------------------------------------------------------------------

def validate() -> None:
    """Run sanity checks on outputs."""
    logger.info("=" * 60)
    logger.info("VALIDATION")
    logger.info("=" * 60)

    levels = {}
    for level in [5, 4, 3]:
        path = OUTPUT_DIR / f"overture_h3l{level}.parquet"
        if not path.exists():
            logger.warning("Missing: %s", path)
            continue
        gdf = gpd.read_parquet(path)
        levels[level] = gdf
        size_mb = path.stat().st_size / 1024 / 1024
        logger.info("L%d: %d rows, %d cols, %.1f MB", level, len(gdf), len(gdf.columns), size_mb)

    # Conservation check on total_road_length_m
    if 5 in levels and "total_road_length_m" in levels[5].columns:
        road_l5 = float(levels[5]["total_road_length_m"].sum())
        logger.info("L5 total road length: %s m (%.0f km)", f"{road_l5:,.0f}", road_l5 / 1000)

        for level in [4, 3]:
            if level in levels and "total_road_length_m" in levels[level].columns:
                road = float(levels[level]["total_road_length_m"].sum())
                ratio = road / road_l5 if road_l5 > 0 else 0
                logger.info("L%d total road length: %s m (ratio L%d/L5 = %.6f)", level, f"{road:,.0f}", level, ratio)

    # Conservation check on building_count
    if 5 in levels and "building_count" in levels[5].columns:
        bldg_l5 = float(levels[5]["building_count"].sum())
        logger.info("L5 building count: %s", f"{bldg_l5:,.0f}")

        for level in [4, 3]:
            if level in levels and "building_count" in levels[level].columns:
                bldg = float(levels[level]["building_count"].sum())
                ratio = bldg / bldg_l5 if bldg_l5 > 0 else 0
                logger.info("L%d building count: %s (ratio = %.6f)", level, f"{bldg:,.0f}", ratio)

    # Top cells by road length
    if 5 in levels and "total_road_length_m" in levels[5].columns:
        l5 = levels[5]
        top_cols = ["h3_index", "total_road_length_m", "road_segment_count"]
        top_cols = [c for c in top_cols if c in l5.columns]
        top = l5.nlargest(5, "total_road_length_m")[top_cols]
        logger.info("Top 5 hexes by road length:\n%s", top.to_string(index=False))

    # POI total
    if 5 in levels and "poi_count" in levels[5].columns:
        poi_total = float(levels[5]["poi_count"].sum())
        logger.info("L5 total POI count: %s", f"{poi_total:,.0f}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Aggregate Overture Maps to H3 grids")
    parser.add_argument("--test", action="store_true", help="Process a single test tile only")
    parser.add_argument("--theme", type=str, default=None, help="Process one theme only (transportation, buildings, places)")
    parser.add_argument("--skip-fetch", action="store_true", help="Skip S3 fetch, only merge+rollup")
    parser.add_argument("--release", type=str, default=None, help="Overture release tag (default: 2026-02-18.0)")
    parser.add_argument("--tile-degrees", type=float, default=5.0, help="Tile size in degrees (default: 5)")
    args = parser.parse_args()

    config = OvertureConfig(
        release=args.release or OvertureConfig.model_fields["release"].default,
        tile_degrees=args.tile_degrees,
    )

    # Resolve themes
    if args.theme:
        theme_key = THEME_ALIASES.get(args.theme.lower())
        if theme_key is None:
            logger.error("Unknown theme: %s (choose: transportation, buildings, places)", args.theme)
            return
        themes = [theme_key]
    else:
        themes = list(THEME_REGISTRY.keys())

    logger.info("Overture H3 Aggregation Pipeline")
    logger.info("Release: %s | Themes: %s | Tile: %.0f°", config.release, themes, config.tile_degrees)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    t_start = time.time()

    # Phase 1: Tiles
    logger.info("--- Phase 1: Tiles ---")
    if args.test:
        tiles = [{"key": "test", "west": TEST_BBOX[0], "south": TEST_BBOX[1], "east": TEST_BBOX[2], "north": TEST_BBOX[3]}]
        logger.info("TEST MODE: single tile %s", TEST_BBOX)
    else:
        tiles = generate_tiles(tile_degrees=config.tile_degrees)

    # Phase 2: Fetch + Aggregate
    if not args.skip_fetch:
        logger.info("--- Phase 2: Fetch + Aggregate ---")
        for theme in themes:
            logger.info("=== Theme: %s ===", theme)
            fetch_theme_tiles(theme, tiles, config)
    else:
        logger.info("--- Phase 2: SKIPPED ---")

    # Phase 3: Merge
    logger.info("--- Phase 3: Merge ---")
    l5 = merge_all_themes(themes)

    l5_path = OUTPUT_DIR / "overture_h3l5.parquet"
    l5.to_parquet(l5_path)
    logger.info("Saved L5: %s (%d rows, %d cols)", l5_path, len(l5), len(l5.columns))

    # Phase 4: Rollup
    logger.info("--- Phase 4: Rollup ---")
    if len(l5) == 0:
        logger.warning("No data to roll up — skipping")
    else:
        rollup_levels(l5)

    # Phase 5: Validate
    logger.info("--- Phase 5: Validate ---")
    validate()

    elapsed = time.time() - t_start
    logger.info("Pipeline complete in %.0f min %.0f sec", elapsed // 60, elapsed % 60)


if __name__ == "__main__":
    main()
