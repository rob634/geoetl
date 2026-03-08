"""Overture road pipeline: extract raw features and compute H3 statistics.

Two pipeline modes for comparison:
  Mode A (local):  extract → merge → stats from local GeoParquet
  Mode B (remote): stats computed directly from Overture S3

Usage:
    python scripts/road_pipeline.py extract [--test] [--workers N]
    python scripts/road_pipeline.py merge
    python scripts/road_pipeline.py stats-local
    python scripts/road_pipeline.py stats-remote [--test] [--workers N]
    python scripts/road_pipeline.py compare
    python scripts/road_pipeline.py all [--test] [--workers N]
"""

import argparse
import json
import logging
import multiprocessing as mp
import time
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd

from geoetl.duckdb.engine import get_connection
from geoetl.h3.grid import h3_to_geodataframe
from geoetl.h3.rollup import rollup
from geoetl.overture.client import get_overture_connection, query_tile
from geoetl.overture.config import ROAD_CLASSES, OvertureConfig
from geoetl.overture.extract import (
    merge_extracts_sql,
    road_extract_sql,
    road_local_stats_sql,
    road_remote_stats_sql,
)
from geoetl.pipeline.checkpoint import CheckpointManager

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BASE_DIR = Path("outputs/overture/roads")
EXTRACT_DIR = BASE_DIR / "extract"
EXTRACT_TILES_DIR = EXTRACT_DIR / "tiles"
LOCAL_STATS_DIR = BASE_DIR / "stats_local"
REMOTE_STATS_DIR = BASE_DIR / "stats_remote"
REMOTE_TILES_DIR = REMOTE_STATS_DIR / "tiles"
COMPARE_DIR = BASE_DIR / "comparison"

TEST_BBOX = (-1.0, 51.0, 1.0, 53.0)  # London area

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("road_pipeline")


# ---------------------------------------------------------------------------
# Tile generation (shared between extract and remote-stats)
# ---------------------------------------------------------------------------

def generate_tiles(
    tile_degrees: float = 5.0,
    bounds: tuple[float, float, float, float] = (-180.0, -60.0, 180.0, 75.0),
    land_only: bool = False,
) -> list[dict]:
    """Partition the globe into rectangular bbox tiles.

    Args:
        land_only: If True, skip tiles that have no land coverage using
            a rasterized Natural Earth 10m land polygon check.
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
                "west": lon, "south": lat,
                "east": lon_end, "north": lat_end,
            })
            lat = lat_end
        lon = lon_end

    logger.info("Generated %d tiles (%.0f° grid)", len(tiles), tile_degrees)

    if land_only:
        tiles = _filter_tiles_to_land(tiles)

    return tiles


def _filter_tiles_to_land(tiles: list[dict]) -> list[dict]:
    """Filter tiles to those overlapping land using rasterized Natural Earth."""
    from rasterio.features import rasterize
    from rasterio.transform import from_bounds

    land_path = Path(__file__).parent.parent / "data" / "reference" / "ne_10m_land.zip"
    if not land_path.exists():
        logger.warning("Land polygon not found: %s — skipping land filter", land_path)
        return tiles

    import geopandas as _gpd
    land = _gpd.read_file(land_path)

    # Rasterize at 1° — coarse but fast, good enough for tile filtering
    res = 1.0
    width, height = int(360 / res), int(180 / res)
    transform = from_bounds(-180, -90, 180, 90, width, height)
    land_raster = rasterize(
        [(geom, 1) for geom in land.geometry],
        out_shape=(height, width),
        transform=transform,
        fill=0, dtype="uint8", all_touched=True,
    )

    filtered = []
    for tile in tiles:
        # Check if any land pixel falls within the tile bbox
        c0 = max(0, int((tile["west"] + 180) / res))
        c1 = min(width, int((tile["east"] + 180) / res) + 1)
        r0 = max(0, int((90 - tile["north"]) / res))
        r1 = min(height, int((90 - tile["south"]) / res) + 1)
        if land_raster[r0:r1, c0:c1].any():
            filtered.append(tile)

    logger.info("Land filter: %d/%d tiles have land coverage", len(filtered), len(tiles))
    return filtered


# ---------------------------------------------------------------------------
# Phase: Extract (Mode A — download raw road features)
# ---------------------------------------------------------------------------

def _extract_tile_worker(args: tuple) -> dict:
    """Worker: extract road features for one tile. Runs in spawned process.

    Returns an error result instead of raising — a single tile failure must
    not kill the pool and lose completed-but-unconsumed results from other workers.
    """
    tile, config_dict, output_dir, all_classes = args
    pid = mp.current_process().pid

    try:
        config = OvertureConfig(**config_dict)
        conn = get_overture_connection(config)
        sql = road_extract_sql(
            west=tile["west"], south=tile["south"],
            east=tile["east"], north=tile["north"],
            config=config,
            all_classes=all_classes,
        )

        t0 = time.monotonic()
        df = conn.execute(sql).fetchdf()
        query_time = time.monotonic() - t0
        rows = len(df)

        write_time = 0.0
        bytes_written = 0
        if rows > 0:
            out_path = Path(output_dir) / f"{tile['key']}.parquet"
            tw0 = time.monotonic()
            df.to_parquet(out_path, index=False)
            write_time = time.monotonic() - tw0
            bytes_written = out_path.stat().st_size

        return {
            "key": tile["key"],
            "rows": rows,
            "query_time": query_time,
            "write_time": write_time,
            "total_time": query_time + write_time,
            "bytes_written": bytes_written,
            "worker_pid": pid,
            "timestamp": time.time(),
        }
    except Exception as e:
        return {
            "key": tile["key"],
            "rows": 0,
            "query_time": 0.0,
            "write_time": 0.0,
            "total_time": 0.0,
            "bytes_written": 0,
            "worker_pid": pid,
            "timestamp": time.time(),
            "error": f"{type(e).__name__}: {e}",
        }


def do_extract(tiles: list[dict], config: OvertureConfig, workers: int = 1, all_classes: bool = False) -> None:
    """Download raw road features from Overture S3, one parquet per tile."""
    EXTRACT_TILES_DIR.mkdir(parents=True, exist_ok=True)
    cp = CheckpointManager(EXTRACT_DIR / "checkpoint.json")
    cp.load()

    pending = [t for t in tiles if not cp.is_done(t["key"])]
    logger.info(
        "Extract: %d tiles total, %d pending, %d already done | all_classes=%s | workers=%d",
        len(tiles), len(pending), len(tiles) - len(pending), all_classes, workers,
    )

    if not pending:
        logger.info("Extract: nothing to do")
        return

    config_dict = config.model_dump()
    work_items = [(t, config_dict, str(EXTRACT_TILES_DIR), all_classes) for t in pending]
    metrics: list[dict] = []

    # Open metrics log for incremental writes (monitor during long runs)
    metrics_path = EXTRACT_DIR / "metrics.jsonl"
    metrics_file = open(metrics_path, "a")

    t_start = time.time()
    total_rows = 0

    def _record_metric(m: dict) -> None:
        metrics.append(m)
        metrics_file.write(json.dumps(m) + "\n")
        metrics_file.flush()

    failed_tiles: list[dict] = []

    if workers <= 1:
        # Sequential — single connection, reused across tiles
        conn = get_overture_connection(config)
        pid = mp.current_process().pid
        for i, tile in enumerate(pending, 1):
            logger.info(
                "[%d/%d] extract tile %s (%.1f,%.1f)-(%.1f,%.1f)",
                i, len(pending), tile["key"],
                tile["west"], tile["south"], tile["east"], tile["north"],
            )
            try:
                sql = road_extract_sql(
                    west=tile["west"], south=tile["south"],
                    east=tile["east"], north=tile["north"],
                    config=config,
                    all_classes=all_classes,
                )
                t0 = time.monotonic()
                df = query_tile(conn, sql, tile["key"])
                query_time = time.monotonic() - t0
                rows = len(df)

                write_time = 0.0
                bytes_written = 0
                if rows > 0:
                    out_path = EXTRACT_TILES_DIR / f"{tile['key']}.parquet"
                    tw0 = time.monotonic()
                    df.to_parquet(out_path, index=False)
                    write_time = time.monotonic() - tw0
                    bytes_written = out_path.stat().st_size
                    logger.info("  -> %d features, %.1fs query + %.1fs write", rows, query_time, write_time)
                else:
                    logger.debug("  -> 0 features, %.1fs", query_time)

                total_rows += rows
                cp.mark_done(tile["key"])
                _record_metric({
                    "key": tile["key"], "rows": rows,
                    "query_time": round(query_time, 2),
                    "write_time": round(write_time, 2),
                    "total_time": round(query_time + write_time, 2),
                    "bytes_written": bytes_written,
                    "worker_pid": pid,
                    "timestamp": time.time(),
                })
            except Exception as e:
                logger.error("FAILED tile %s: %s: %s", tile["key"], type(e).__name__, e)
                failed_tiles.append({"key": tile["key"], "error": f"{type(e).__name__}: {e}"})
                _record_metric({
                    "key": tile["key"], "rows": 0,
                    "query_time": 0.0, "write_time": 0.0, "total_time": 0.0,
                    "bytes_written": 0, "worker_pid": pid,
                    "timestamp": time.time(), "error": f"{type(e).__name__}: {e}",
                })
                # Re-establish connection in case the error left it broken
                try:
                    conn = get_overture_connection(config)
                except Exception:
                    logger.error("Failed to reconnect after error — aborting")
                    break
    else:
        # Parallel — spawn context, each process gets its own connection
        logger.info("Using %d workers (spawn context)", workers)
        ctx = mp.get_context("spawn")
        with ctx.Pool(processes=workers) as pool:
            for result in pool.imap_unordered(_extract_tile_worker, work_items):
                if "error" in result:
                    logger.error(
                        "FAILED tile %s [pid %d]: %s",
                        result["key"], result["worker_pid"], result["error"],
                    )
                    failed_tiles.append(result)
                    _record_metric(result)
                    continue
                total_rows += result["rows"]
                cp.mark_done(result["key"])
                _record_metric(result)
                if result["rows"] > 0:
                    logger.info(
                        "  tile %s: %d features, %.1fs query + %.1fs write [pid %d]",
                        result["key"], result["rows"],
                        result["query_time"], result["write_time"],
                        result["worker_pid"],
                    )

    metrics_file.close()
    elapsed_total = time.time() - t_start

    # Throughput summary
    _log_extract_throughput(metrics, elapsed_total, workers)

    if failed_tiles:
        logger.warning(
            "Extract finished with %d FAILED tiles (will be retried on next run):",
            len(failed_tiles),
        )
        for ft in failed_tiles:
            logger.warning("  %s: %s", ft["key"], ft.get("error", "unknown"))

    succeeded = len(pending) - len(failed_tiles)
    logger.info(
        "Extract complete: %d features across %d/%d tiles (%.0fs, %d failed) | metrics: %s",
        total_rows, succeeded, len(pending), elapsed_total, len(failed_tiles), metrics_path,
    )


def _log_extract_throughput(metrics: list[dict], wall_time: float, workers: int) -> None:
    """Analyze and log per-worker and aggregate throughput from extract metrics."""
    if not metrics:
        return

    total_rows = sum(m["rows"] for m in metrics)
    total_bytes = sum(m["bytes_written"] for m in metrics)
    total_query = sum(m["query_time"] for m in metrics)
    total_write = sum(m["write_time"] for m in metrics)
    total_task = sum(m["total_time"] for m in metrics)
    non_empty = [m for m in metrics if m["rows"] > 0]
    empty = len(metrics) - len(non_empty)

    logger.info("--- Extract Throughput ---")
    logger.info("  Wall time:        %.1fs", wall_time)
    logger.info("  Workers:          %d", workers)
    logger.info("  Tiles:            %d (%d non-empty, %d empty)", len(metrics), len(non_empty), empty)
    logger.info("  Total features:   %s", f"{total_rows:,}")
    logger.info("  Total data:       %.1f MB", total_bytes / 1024 / 1024)
    logger.info("  Agg query time:   %.1fs (sum across workers)", total_query)
    logger.info("  Agg write time:   %.1fs (sum across workers)", total_write)
    logger.info("  Parallelism eff:  %.1fx (agg_task_time / wall_time)",
                total_task / wall_time if wall_time > 0 else 0)

    if non_empty:
        query_times = [m["query_time"] for m in non_empty]
        rows_per_sec = [m["rows"] / m["query_time"] if m["query_time"] > 0 else 0 for m in non_empty]
        mb_per_sec = [m["bytes_written"] / m["query_time"] / 1024 / 1024 if m["query_time"] > 0 else 0 for m in non_empty]

        logger.info("  Query time (non-empty tiles):")
        logger.info("    min=%.1fs  median=%.1fs  max=%.1fs  mean=%.1fs",
                     min(query_times), sorted(query_times)[len(query_times) // 2],
                     max(query_times), sum(query_times) / len(query_times))
        logger.info("  Throughput per query:")
        logger.info("    features/s: min=%.0f  median=%.0f  max=%.0f",
                     min(rows_per_sec), sorted(rows_per_sec)[len(rows_per_sec) // 2], max(rows_per_sec))
        logger.info("    MB/s:       min=%.1f  median=%.1f  max=%.1f",
                     min(mb_per_sec), sorted(mb_per_sec)[len(mb_per_sec) // 2], max(mb_per_sec))

    # Per-worker breakdown
    worker_pids = set(m["worker_pid"] for m in metrics)
    if len(worker_pids) > 1:
        logger.info("  Per-worker breakdown:")
        for pid in sorted(worker_pids):
            wm = [m for m in metrics if m["worker_pid"] == pid]
            w_rows = sum(m["rows"] for m in wm)
            w_time = sum(m["total_time"] for m in wm)
            w_bytes = sum(m["bytes_written"] for m in wm)
            logger.info(
                "    pid %-6d: %4d tiles, %8s features, %6.1f MB, %6.1fs task time",
                pid, len(wm), f"{w_rows:,}", w_bytes / 1024 / 1024, w_time,
            )


# ---------------------------------------------------------------------------
# Phase: Merge (combine tile extracts into sorted GeoParquet)
# ---------------------------------------------------------------------------

def do_merge() -> Path:
    """Merge per-tile extract parquets, add H3 parent columns, sort by h3_l5."""
    tiles_glob = str(EXTRACT_TILES_DIR / "*.parquet")
    output_path = EXTRACT_DIR / "roads.parquet"

    parts = sorted(EXTRACT_TILES_DIR.glob("*.parquet"))
    if not parts:
        logger.error("No tile parquets found in %s", EXTRACT_TILES_DIR)
        raise FileNotFoundError(f"No parquets in {EXTRACT_TILES_DIR}")

    logger.info("Merging %d tile parquets...", len(parts))
    t0 = time.time()

    conn = get_connection()
    sql = merge_extracts_sql(tiles_glob, str(output_path))
    conn.execute(sql)

    elapsed = time.time() - t0
    size_mb = output_path.stat().st_size / 1024 / 1024

    # Count rows
    row_count = conn.execute(
        f"SELECT COUNT(*) FROM read_parquet('{output_path}')"
    ).fetchone()[0]

    logger.info(
        "Merged: %s (%d features, %.1f MB, %.1fs)",
        output_path, row_count, size_mb, elapsed,
    )
    return output_path


# ---------------------------------------------------------------------------
# Phase: Stats Local (Mode A — compute from local extract)
# ---------------------------------------------------------------------------

def do_stats_local() -> gpd.GeoDataFrame:
    """Compute road statistics from the local merged extract."""
    extract_path = EXTRACT_DIR / "roads.parquet"
    if not extract_path.exists():
        logger.error("Local extract not found: %s (run 'extract' and 'merge' first)", extract_path)
        raise FileNotFoundError(str(extract_path))

    LOCAL_STATS_DIR.mkdir(parents=True, exist_ok=True)

    logger.info("Computing local stats from %s...", extract_path)
    t0 = time.time()

    conn = get_connection()
    sql = road_local_stats_sql(str(extract_path))
    df = conn.execute(sql).fetchdf()

    elapsed = time.time() - t0
    logger.info("Local stats: %d cells, %.1fs", len(df), elapsed)

    # Build geometry from H3 indices
    l5 = _stats_to_geodataframe(df)

    l5_path = LOCAL_STATS_DIR / "roads_h3l5.parquet"
    l5.to_parquet(l5_path)
    logger.info("Saved: %s", l5_path)

    # Rollup
    _do_rollup(l5, LOCAL_STATS_DIR)

    return l5


# ---------------------------------------------------------------------------
# Phase: Stats Remote (Mode B — compute directly from S3)
# ---------------------------------------------------------------------------

def do_stats_remote(tiles: list[dict], config: OvertureConfig, workers: int = 1) -> gpd.GeoDataFrame:
    """Compute road statistics directly from Overture S3, tile by tile."""
    REMOTE_TILES_DIR.mkdir(parents=True, exist_ok=True)
    REMOTE_STATS_DIR.mkdir(parents=True, exist_ok=True)
    cp = CheckpointManager(REMOTE_STATS_DIR / "checkpoint.json")
    cp.load()

    pending = [t for t in tiles if not cp.is_done(t["key"])]
    logger.info(
        "Remote stats: %d tiles total, %d pending",
        len(tiles), len(pending),
    )

    if pending:
        conn = get_overture_connection(config)
        t_start = time.time()

        for i, tile in enumerate(pending, 1):
            logger.info(
                "[%d/%d] remote-stats tile %s",
                i, len(pending), tile["key"],
            )
            sql = road_remote_stats_sql(
                west=tile["west"], south=tile["south"],
                east=tile["east"], north=tile["north"],
                config=config,
            )
            t0 = time.monotonic()
            df = query_tile(conn, sql, tile["key"])
            elapsed = time.monotonic() - t0

            if len(df) > 0:
                out_path = REMOTE_TILES_DIR / f"{tile['key']}.parquet"
                df.to_parquet(out_path, index=False)
                logger.info("  -> %d cells, %.1fs", len(df), elapsed)
            else:
                logger.debug("  -> 0 cells, %.1fs", elapsed)

            cp.mark_done(tile["key"])

        logger.info("Remote fetch: %.0fs", time.time() - t_start)

    # Merge tile stats
    parts = sorted(REMOTE_TILES_DIR.glob("*.parquet"))
    if not parts:
        logger.warning("No remote stats tiles found")
        return gpd.GeoDataFrame()

    logger.info("Merging %d remote stats tiles...", len(parts))
    dfs = [pd.read_parquet(p) for p in parts]
    merged = pd.concat(dfs, ignore_index=True)

    # Groupby sum to consolidate any overlapping cells (shouldn't happen with centroid filter)
    numeric_cols = [c for c in merged.columns if c != "h3_index"]
    merged = merged.groupby("h3_index", as_index=False)[numeric_cols].sum()

    l5 = _stats_to_geodataframe(merged)

    l5_path = REMOTE_STATS_DIR / "roads_h3l5.parquet"
    l5.to_parquet(l5_path)
    logger.info("Saved: %s (%d cells)", l5_path, len(l5))

    # Rollup
    _do_rollup(l5, REMOTE_STATS_DIR)

    return l5


# ---------------------------------------------------------------------------
# Phase: Compare
# ---------------------------------------------------------------------------

def do_compare() -> dict:
    """Compare local vs remote road statistics."""
    local_path = LOCAL_STATS_DIR / "roads_h3l5.parquet"
    remote_path = REMOTE_STATS_DIR / "roads_h3l5.parquet"

    for p, label in [(local_path, "local"), (remote_path, "remote")]:
        if not p.exists():
            logger.error("%s stats not found: %s", label, p)
            raise FileNotFoundError(str(p))

    local = gpd.read_parquet(local_path)
    remote = gpd.read_parquet(remote_path)

    logger.info("Local:  %d cells", len(local))
    logger.info("Remote: %d cells", len(remote))

    # Join on h3_index
    merged = local.drop(columns="geometry").merge(
        remote.drop(columns="geometry"),
        on="h3_index",
        how="outer",
        suffixes=("_local", "_remote"),
    )

    # Cells in one but not the other
    local_only = merged["total_road_length_m_local"].notna() & merged["total_road_length_m_remote"].isna()
    remote_only = merged["total_road_length_m_remote"].notna() & merged["total_road_length_m_local"].isna()
    both = merged["total_road_length_m_local"].notna() & merged["total_road_length_m_remote"].notna()

    logger.info("Cells in both: %d", both.sum())
    logger.info("Local-only:    %d", local_only.sum())
    logger.info("Remote-only:   %d", remote_only.sum())

    # Compare metric columns
    metric_cols = [c.replace("_length_m", "") for c in ROAD_CLASSES]
    stat_cols = [f"{cls}_length_m" for cls in ROAD_CLASSES] + [
        "road_segment_count",
        "total_road_length_m",
    ]

    report = {
        "local_cells": int(len(local)),
        "remote_cells": int(len(remote)),
        "cells_in_both": int(both.sum()),
        "local_only": int(local_only.sum()),
        "remote_only": int(remote_only.sum()),
        "columns": {},
    }

    shared = merged[both]
    for col in stat_cols:
        lcol = f"{col}_local"
        rcol = f"{col}_remote"
        if lcol not in shared.columns or rcol not in shared.columns:
            continue

        local_sum = float(shared[lcol].sum())
        remote_sum = float(shared[rcol].sum())
        diff = shared[lcol] - shared[rcol]
        abs_diff = diff.abs()

        ratio = local_sum / remote_sum if remote_sum > 0 else float("nan")
        max_abs = float(abs_diff.max()) if len(abs_diff) > 0 else 0.0
        mean_abs = float(abs_diff.mean()) if len(abs_diff) > 0 else 0.0

        report["columns"][col] = {
            "local_total": round(local_sum, 2),
            "remote_total": round(remote_sum, 2),
            "ratio": round(ratio, 8),
            "max_abs_diff": round(max_abs, 4),
            "mean_abs_diff": round(mean_abs, 4),
        }

        logger.info(
            "  %-30s local=%14.1f  remote=%14.1f  ratio=%.8f  max_diff=%.4f",
            col, local_sum, remote_sum, ratio, max_abs,
        )

    # Save report
    COMPARE_DIR.mkdir(parents=True, exist_ok=True)
    report_path = COMPARE_DIR / "comparison.json"
    report_path.write_text(json.dumps(report, indent=2))
    logger.info("Comparison report: %s", report_path)

    # Timing comparison (from checkpoint metadata if available)
    _log_timing()

    return report


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _stats_to_geodataframe(df: pd.DataFrame) -> gpd.GeoDataFrame:
    """Convert stats DataFrame to GeoDataFrame with H3 polygon geometry."""
    # Cast metric columns to float32
    for c in df.columns:
        if c != "h3_index":
            df[c] = df[c].astype("float32")

    # Build geometry
    h3_indices = df["h3_index"].tolist()
    geo_gdf = h3_to_geodataframe(h3_indices)
    df = df.merge(geo_gdf[["h3_index", "geometry"]], on="h3_index", how="left")
    return gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326")


def _do_rollup(l5: gpd.GeoDataFrame, output_dir: Path) -> None:
    """Roll up L5 stats to L4 and L3. All columns are extensive (summable)."""
    metric_cols = [c for c in l5.columns if c not in ("h3_index", "geometry")]
    agg_spec = {c: "sum" for c in metric_cols}

    for target, label in [(4, "L4"), (3, "L3")]:
        logger.info("Rolling up to %s...", label)
        t0 = time.time()
        rolled = rollup(l5, target_resolution=target, agg_spec=agg_spec)
        logger.info("%s: %d rows (%.1fs)", label, len(rolled), time.time() - t0)

        out_path = output_dir / f"roads_h3l{target}.parquet"
        rolled.to_parquet(out_path)
        logger.info("Saved: %s", out_path)

        if target == 4:
            l5 = rolled


def _log_timing() -> None:
    """Log timing info from extract and remote-stats checkpoint files."""
    for label, cp_path in [
        ("extract", EXTRACT_DIR / "checkpoint.json"),
        ("remote-stats", REMOTE_STATS_DIR / "checkpoint.json"),
    ]:
        if cp_path.exists():
            data = json.loads(cp_path.read_text())
            logger.info("%s: %d tiles checkpointed", label, len(data))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Overture road pipeline: extract features + compute H3 statistics",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Commands:
  extract       Download raw road features from Overture S3
  merge         Merge tile parquets into sorted GeoParquet
  stats-local   Compute H3 stats from local extract
  stats-remote  Compute H3 stats directly from Overture S3
  compare       Compare local vs remote stats
  all           Run full pipeline (extract → merge → stats-local → stats-remote → compare)
""",
    )
    parser.add_argument(
        "command",
        choices=["extract", "merge", "stats-local", "stats-remote", "compare", "all"],
    )
    parser.add_argument("--test", action="store_true", help="Single test tile (London area)")
    parser.add_argument("--workers", type=int, default=1, help="Parallel workers for S3 queries")
    parser.add_argument("--tile-degrees", type=float, default=5.0, help="Tile size in degrees")
    parser.add_argument("--release", type=str, default=None, help="Overture release tag")
    parser.add_argument("--all-classes", action="store_true", help="Extract all road classes (highway != NULL)")
    parser.add_argument("--land-only", action="store_true", help="Skip ocean-only tiles (Natural Earth land filter)")
    args = parser.parse_args()

    # Divide DuckDB memory budget across workers, leaving headroom for Python/OS.
    # Default: 48GB usable for DuckDB out of 64GB system RAM.
    DUCKDB_TOTAL_GB = 48
    per_worker_gb = max(4, DUCKDB_TOTAL_GB // max(args.workers, 1))
    config = OvertureConfig(
        release=args.release or OvertureConfig.model_fields["release"].default,
        tile_degrees=args.tile_degrees,
        memory_limit=f"{per_worker_gb}GB",
    )

    # Generate tiles
    if args.test:
        tiles = [{
            "key": "test",
            "west": TEST_BBOX[0], "south": TEST_BBOX[1],
            "east": TEST_BBOX[2], "north": TEST_BBOX[3],
        }]
        logger.info("TEST MODE: single tile %s", TEST_BBOX)
    else:
        tiles = generate_tiles(tile_degrees=config.tile_degrees, land_only=args.land_only)

    logger.info(
        "Road Pipeline | Release: %s | Tiles: %d | Workers: %d | DuckDB memory: %s/worker",
        config.release, len(tiles), args.workers, config.memory_limit,
    )
    logger.info("Output: %s", BASE_DIR)

    t_start = time.time()
    timing = {}

    cmd = args.command

    # --- Extract ---
    if cmd in ("extract", "all"):
        logger.info("=" * 60)
        logger.info("PHASE: Extract (download raw road features)")
        logger.info("=" * 60)
        t0 = time.time()
        do_extract(tiles, config, workers=args.workers, all_classes=args.all_classes)
        timing["extract"] = time.time() - t0

    # --- Merge ---
    if cmd in ("merge", "all"):
        logger.info("=" * 60)
        logger.info("PHASE: Merge (combine tiles → sorted GeoParquet)")
        logger.info("=" * 60)
        t0 = time.time()
        do_merge()
        timing["merge"] = time.time() - t0

    # --- Stats Local ---
    if cmd in ("stats-local", "all"):
        logger.info("=" * 60)
        logger.info("PHASE: Stats Local (from local extract)")
        logger.info("=" * 60)
        t0 = time.time()
        do_stats_local()
        timing["stats_local"] = time.time() - t0

    # --- Stats Remote ---
    if cmd in ("stats-remote", "all"):
        logger.info("=" * 60)
        logger.info("PHASE: Stats Remote (from Overture S3)")
        logger.info("=" * 60)
        t0 = time.time()
        do_stats_remote(tiles, config, workers=args.workers)
        timing["stats_remote"] = time.time() - t0

    # --- Compare ---
    if cmd in ("compare", "all"):
        logger.info("=" * 60)
        logger.info("PHASE: Compare (local vs remote)")
        logger.info("=" * 60)
        t0 = time.time()
        do_compare()
        timing["compare"] = time.time() - t0

    # --- Summary ---
    total = time.time() - t_start
    timing["total"] = total
    logger.info("=" * 60)
    for phase, secs in timing.items():
        logger.info("%-15s %.1fs", phase, secs)
    logger.info("=" * 60)

    # Save timing
    COMPARE_DIR.mkdir(parents=True, exist_ok=True)
    timing_path = COMPARE_DIR / "timing.json"
    timing_path.write_text(json.dumps(timing, indent=2))
    logger.info("Timing: %s", timing_path)


if __name__ == "__main__":
    main()
