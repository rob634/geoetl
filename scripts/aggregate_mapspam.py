"""Aggregate MapSPAM 2020 rasters onto H3 grids at levels 3, 4, and 5.

Processes 414 rasters (46 crops x 3 techs x 3 extensive vars: Production,
Harvested Area, Physical Area), derives yield as P/H, and outputs GeoParquet.
Yield rasters (Y) are skipped — yield is always derived from summed extensives.

Usage:
    python scripts/aggregate_mapspam.py                  # Full run
    python scripts/aggregate_mapspam.py --test            # 3 crops only
    python scripts/aggregate_mapspam.py --skip-aggregate  # Skip phase 3, merge+rollup only
    python scripts/aggregate_mapspam.py --workers 8       # Override worker count
"""

import argparse
import json
import logging
import subprocess
import sys
import time
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MAPSPAM_ZIPS = {
    "P": Path("~/Downloads/spam2020V2r0_global_production.geotiff.zip").expanduser(),
    "H": Path("~/Downloads/spam2020V2r0_global_harvested_area.geotiff.zip").expanduser(),
    "A": Path("~/Downloads/spam2020V2r0_global_physical_area.geotiff.zip").expanduser(),
}

# Subdirectory names inside the zips
ZIP_DIRS = {
    "P": "spam2020V2r0_global_production",
    "H": "spam2020V2r0_global_harvested_area",
    "A": "spam2020V2r0_global_physical_area",
}

DATA_DIR = Path("data/mapspam")
OUTPUT_DIR = Path("outputs/mapspam")
GRID_CACHE = OUTPUT_DIR / "h3_l5_grid.parquet"
CHECKPOINT_DIR = OUTPUT_DIR / "checkpoints"

CROPS = [
    "BANA", "BARL", "BEAN", "CASS", "CHIC", "CITR", "CNUT", "COCO",
    "COFF", "COTT", "COWP", "GROU", "LENT", "MAIZ", "MILL", "OCER",
    "OFIB", "OILP", "ONIO", "OOIL", "OPUL", "ORTS", "PIGE", "PLNT",
    "PMIL", "POTA", "RAPE", "RCOF", "REST", "RICE", "RUBB", "SESA",
    "SORG", "SOYB", "SUGB", "SUGC", "SUNF", "SWPO", "TEAS", "TEMF",
    "TOBA", "TOMA", "TROF", "VEGE", "WHEA", "YAMS",
]
TEST_CROPS = ["WHEA", "MAIZ", "RICE"]

TECHS = ["A", "I", "R"]
EXTENSIVE_VARS = {"P": "production_mt", "H": "harv_area_ha", "A": "phys_area_ha"}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("mapspam")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def raster_path(var: str, crop: str, tech: str) -> Path:
    """Build raster file path for a given variable/crop/tech combo."""
    return DATA_DIR / ZIP_DIRS[var] / f"spam2020_V2r0_global_{var}_{crop}_{tech}.tif"


def col_name(crop: str, tech: str, var: str) -> str:
    """Build column name like whea_a_production_mt."""
    return f"{crop.lower()}_{tech.lower()}_{EXTENSIVE_VARS[var]}"


def yield_col_name(crop: str, tech: str) -> str:
    return f"{crop.lower()}_{tech.lower()}_yield_kgha"


def checkpoint_path(crop: str, tech: str, var: str) -> Path:
    return CHECKPOINT_DIR / f"{crop.lower()}_{tech.lower()}_{var}.parquet"


def checkpoint_done(crop: str, tech: str, var: str) -> bool:
    cp = checkpoint_path(crop, tech, var)
    return cp.exists() and cp.stat().st_size > 0


# ---------------------------------------------------------------------------
# Phase 1: Unzip
# ---------------------------------------------------------------------------

def unzip_sources() -> None:
    """Unzip P, H, A zip files to data/mapspam/ if not already extracted."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    for var, zip_path in MAPSPAM_ZIPS.items():
        target_dir = DATA_DIR / ZIP_DIRS[var]
        if target_dir.exists() and any(target_dir.glob("*.tif")):
            n_tifs = len(list(target_dir.glob("*.tif")))
            logger.info("Already extracted %s: %d tifs in %s", var, n_tifs, target_dir)
            continue

        if not zip_path.exists():
            logger.error("Zip not found: %s", zip_path)
            sys.exit(1)

        logger.info("Unzipping %s -> %s", zip_path.name, DATA_DIR)
        subprocess.run(
            ["unzip", "-o", "-q", str(zip_path), "-d", str(DATA_DIR)],
            check=True,
        )
        n_tifs = len(list(target_dir.glob("*.tif")))
        logger.info("Extracted %d tifs for %s", n_tifs, var)

    # Validate counts
    for var in EXTENSIVE_VARS:
        target_dir = DATA_DIR / ZIP_DIRS[var]
        n_tifs = len(list(target_dir.glob("*.tif")))
        expected = len(CROPS) * len(TECHS)  # 138
        if n_tifs != expected:
            logger.warning("Expected %d tifs for %s, found %d", expected, var, n_tifs)


# ---------------------------------------------------------------------------
# Phase 2: Generate Grid
# ---------------------------------------------------------------------------

def build_grid() -> gpd.GeoDataFrame:
    """Generate or load cached H3 L5 land-only grid.

    Filters to cells where ANY crop has data by checking all production rasters
    (tech=A) and keeping cells with data in at least one.
    """
    if GRID_CACHE.exists():
        logger.info("Loading cached grid from %s", GRID_CACHE)
        return gpd.read_parquet(GRID_CACHE)

    from geoetl.h3.grid import extract_centroids, generate_grid

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    logger.info("Generating global H3 L5 grid...")
    t0 = time.time()
    grid = generate_grid(resolution=5)
    logger.info("Raw grid: %d cells (%.1fs)", len(grid), time.time() - t0)

    # Filter to land: union of all production rasters (tech=A) coverage
    import rasterio
    from rasterio.transform import rowcol

    grid = extract_centroids(grid)
    has_any_data = np.zeros(len(grid), dtype=bool)

    for crop in CROPS:
        rpath = raster_path("P", crop, "A")
        if not rpath.exists():
            logger.warning("Reference raster not found: %s", rpath)
            continue

        lats = grid["lat"].values
        lons = grid["lon"].values

        with rasterio.open(rpath) as src:
            rows, cols = rowcol(src.transform, lons, lats)
            rows = np.array(rows)
            cols = np.array(cols)
            valid_bounds = (rows >= 0) & (rows < src.height) & (cols >= 0) & (cols < src.width)
            data = src.read(1)
            nodata = src.nodata

        in_bounds = np.where(valid_bounds)[0]
        pixel_vals = data[rows[in_bounds], cols[in_bounds]].astype(float)
        finite = np.isfinite(pixel_vals) & (pixel_vals != 0)
        if nodata is not None:
            finite &= pixel_vals != nodata
        has_any_data[in_bounds[finite]] = True

        n_new = has_any_data.sum()
        logger.info("After %s: %d cells with data", crop, n_new)

    grid = grid[has_any_data].copy()
    grid = gpd.GeoDataFrame(grid, geometry="geometry", crs="EPSG:4326")
    logger.info("Land grid: %d cells total", len(grid))

    grid.to_parquet(GRID_CACHE)
    logger.info("Saved grid cache: %s", GRID_CACHE)
    return grid


# ---------------------------------------------------------------------------
# Phase 3: Aggregate Extensive Variables
# ---------------------------------------------------------------------------

def aggregate_all(grid: gpd.GeoDataFrame, crops: list[str], workers: int) -> None:
    """Aggregate all crop/tech/var combos with per-raster checkpointing."""
    from geoetl.config import BatchConfig
    from geoetl.h3.aggregation import zonal_aggregate
    from geoetl.raster.zonal import AggMethod

    CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
    config = BatchConfig(max_workers=workers)

    total = len(crops) * len(TECHS) * len(EXTENSIVE_VARS)
    done = 0
    skipped = 0

    for crop in crops:
        for tech in TECHS:
            for var in EXTENSIVE_VARS:
                done += 1
                if checkpoint_done(crop, tech, var):
                    skipped += 1
                    continue

                rpath = raster_path(var, crop, tech)
                cname = col_name(crop, tech, var)
                logger.info(
                    "[%d/%d] Aggregating %s (%s)", done, total, cname, rpath.name,
                )

                if not rpath.exists():
                    logger.warning("Raster not found, skipping: %s", rpath)
                    continue

                t0 = time.time()
                result = zonal_aggregate(
                    raster_path=rpath,
                    h3_gdf=grid,
                    agg=AggMethod.SUM,
                    config=config,
                )
                elapsed = time.time() - t0

                # Save checkpoint: just h3_index and value
                cp = checkpoint_path(crop, tech, var)
                result[["h3_index", "value"]].rename(
                    columns={"value": cname}
                ).to_parquet(cp, index=False)

                logger.info(
                    "  -> %d hexes with data, %.1fs, saved %s",
                    len(result), elapsed, cp.name,
                )

    logger.info(
        "Aggregation complete: %d/%d done (%d skipped from checkpoint)",
        done, total, skipped,
    )


# ---------------------------------------------------------------------------
# Phase 4: Merge + Derive Yield
# ---------------------------------------------------------------------------

def merge_and_derive(crops: list[str]) -> gpd.GeoDataFrame:
    """Merge all checkpoint parquets and derive yield columns."""
    logger.info("Merging checkpoint results...")

    # Start with grid for geometry
    grid = gpd.read_parquet(GRID_CACHE)
    merged = grid[["h3_index", "geometry"]].copy()

    loaded = 0
    missing = 0
    for crop in crops:
        for tech in TECHS:
            for var in EXTENSIVE_VARS:
                cp = checkpoint_path(crop, tech, var)
                if not cp.exists():
                    missing += 1
                    continue
                chunk = pd.read_parquet(cp)
                cname = col_name(crop, tech, var)
                # Ensure column is named correctly
                if cname not in chunk.columns:
                    # Rename 'value' if present
                    if "value" in chunk.columns:
                        chunk = chunk.rename(columns={"value": cname})
                merged = merged.merge(chunk[["h3_index", cname]], on="h3_index", how="left")
                loaded += 1

    logger.info("Merged %d checkpoint files (%d missing)", loaded, missing)

    # Fill NaN with 0 for extensive vars
    ext_cols = [
        col_name(crop, tech, var)
        for crop in crops for tech in TECHS for var in EXTENSIVE_VARS
        if col_name(crop, tech, var) in merged.columns
    ]
    for c in ext_cols:
        merged[c] = merged[c].fillna(0).astype("float32")

    # Derive yield for each crop/tech
    derived = 0
    for crop in crops:
        for tech in TECHS:
            prod_c = col_name(crop, tech, "P")
            area_c = col_name(crop, tech, "H")
            yld_c = yield_col_name(crop, tech)

            if prod_c in merged.columns and area_c in merged.columns:
                area = merged[area_c].values
                prod = merged[prod_c].values
                with np.errstate(divide="ignore", invalid="ignore"):
                    merged[yld_c] = np.where(
                        area > 0, (prod / area) * 1000.0, np.nan
                    ).astype("float32")
                derived += 1

    logger.info("Derived %d yield columns", derived)

    # Summary columns (tech=A only)
    a_prod_cols = [col_name(crop, "A", "P") for crop in crops if col_name(crop, "A", "P") in merged.columns]
    a_area_cols = [col_name(crop, "A", "H") for crop in crops if col_name(crop, "A", "H") in merged.columns]

    merged["total_production_mt"] = merged[a_prod_cols].sum(axis=1).astype("float32")
    merged["total_harv_area_ha"] = merged[a_area_cols].sum(axis=1).astype("float32")

    # Drop rows with zero data everywhere
    data_cols = ext_cols
    has_any = merged[data_cols].sum(axis=1) > 0
    before = len(merged)
    merged = merged[has_any].copy()
    logger.info("Dropped %d empty rows, keeping %d", before - len(merged), len(merged))

    # Re-create GeoDataFrame
    merged = gpd.GeoDataFrame(merged, geometry="geometry", crs="EPSG:4326")

    out_path = OUTPUT_DIR / "mapspam2020_h3l5.parquet"
    merged.to_parquet(out_path)
    logger.info("Saved L5: %s (%d rows, %d cols)", out_path, len(merged), len(merged.columns))

    return merged


# ---------------------------------------------------------------------------
# Phase 5: Rollup L5 -> L4 -> L3
# ---------------------------------------------------------------------------

def rollup_levels(l5: gpd.GeoDataFrame, crops: list[str]) -> None:
    """Roll up L5 to L4 and L3 with proper yield derivation."""
    from geoetl.h3.rollup import rollup

    # Build agg_spec and derive_spec
    agg_spec: dict[str, str] = {}
    derive_spec: dict[str, tuple[str, str, float]] = {}

    for crop in crops:
        for tech in TECHS:
            prefix = f"{crop.lower()}_{tech.lower()}"
            for var in EXTENSIVE_VARS:
                c = f"{prefix}_{EXTENSIVE_VARS[var]}"
                if c in l5.columns:
                    agg_spec[c] = "sum"

            prod_c = f"{prefix}_production_mt"
            area_c = f"{prefix}_harv_area_ha"
            yld_c = f"{prefix}_yield_kgha"
            if prod_c in l5.columns and area_c in l5.columns:
                derive_spec[yld_c] = (prod_c, area_c, 1000.0)

    # Summary columns
    if "total_production_mt" in l5.columns:
        agg_spec["total_production_mt"] = "sum"
    if "total_harv_area_ha" in l5.columns:
        agg_spec["total_harv_area_ha"] = "sum"

    logger.info("Rollup specs: %d agg columns, %d derived columns", len(agg_spec), len(derive_spec))

    # L5 -> L4
    logger.info("Rolling up L5 -> L4...")
    t0 = time.time()
    l4 = rollup(l5, target_resolution=4, agg_spec=agg_spec, derive_spec=derive_spec)
    logger.info("L4: %d rows (%.1fs)", len(l4), time.time() - t0)

    l4_path = OUTPUT_DIR / "mapspam2020_h3l4.parquet"
    l4.to_parquet(l4_path)
    logger.info("Saved: %s", l4_path)

    # L4 -> L3
    logger.info("Rolling up L4 -> L3...")
    t0 = time.time()
    l3 = rollup(l4, target_resolution=3, agg_spec=agg_spec, derive_spec=derive_spec)
    logger.info("L3: %d rows (%.1fs)", len(l3), time.time() - t0)

    l3_path = OUTPUT_DIR / "mapspam2020_h3l3.parquet"
    l3.to_parquet(l3_path)
    logger.info("Saved: %s", l3_path)


# ---------------------------------------------------------------------------
# Phase 6: Validation
# ---------------------------------------------------------------------------

def validate(crops: list[str]) -> None:
    """Run conservation and sanity checks on outputs."""
    logger.info("=" * 60)
    logger.info("VALIDATION")
    logger.info("=" * 60)

    levels = {}
    for level in [5, 4, 3]:
        path = OUTPUT_DIR / f"mapspam2020_h3l{level}.parquet"
        if not path.exists():
            logger.warning("Missing: %s", path)
            continue
        gdf = gpd.read_parquet(path)
        levels[level] = gdf
        size_mb = path.stat().st_size / 1024 / 1024
        logger.info(
            "L%d: %d rows, %d cols, %.1f MB",
            level, len(gdf), len(gdf.columns), size_mb,
        )

    # Conservation check
    if 5 in levels and "total_production_mt" in levels[5].columns:
        prod_l5 = float(levels[5]["total_production_mt"].sum())
        logger.info("L5 total production: %s MT", f"{prod_l5:,.0f}")

        for level in [4, 3]:
            if level in levels and "total_production_mt" in levels[level].columns:
                prod = float(levels[level]["total_production_mt"].sum())
                ratio = prod / prod_l5 if prod_l5 > 0 else 0
                logger.info(
                    "L%d total production: %s MT (ratio L%d/L5 = %.6f)",
                    level, f"{prod:,.0f}", level, ratio,
                )

    # Yield sanity check on wheat (tech=A)
    if 5 in levels:
        l5 = levels[5]
        yld_col = "whea_a_yield_kgha"
        if yld_col in l5.columns:
            valid_yield = l5[yld_col].dropna()
            if len(valid_yield) > 0:
                logger.info(
                    "Wheat yield (L5): mean=%.0f, median=%.0f, min=%.0f, max=%.0f kg/ha (%d hexes)",
                    valid_yield.mean(), valid_yield.median(),
                    valid_yield.min(), valid_yield.max(), len(valid_yield),
                )

    # Sample hex check
    if 5 in levels:
        l5 = levels[5]
        sample_cols = ["h3_index", "total_production_mt", "total_harv_area_ha"]
        sample_cols = [c for c in sample_cols if c in l5.columns]
        top = l5.nlargest(5, "total_production_mt")[sample_cols] if "total_production_mt" in l5.columns else None
        if top is not None:
            logger.info("Top 5 hexes by production:\n%s", top.to_string(index=False))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Aggregate MapSPAM 2020 to H3 grids")
    parser.add_argument("--test", action="store_true", help="Process only 3 test crops (WHEA, MAIZ, RICE)")
    parser.add_argument("--skip-aggregate", action="store_true", help="Skip aggregation, only merge+rollup")
    parser.add_argument("--workers", type=int, default=None, help="Override worker count")
    args = parser.parse_args()

    crops = TEST_CROPS if args.test else CROPS
    workers = args.workers or max(1, __import__("os").cpu_count() - 2)

    logger.info("MapSPAM H3 Aggregation Pipeline")
    logger.info("Crops: %d, Techs: %d, Vars: %d -> %d rasters", len(crops), len(TECHS), len(EXTENSIVE_VARS),
                len(crops) * len(TECHS) * len(EXTENSIVE_VARS))
    logger.info("Workers: %d", workers)

    t_start = time.time()

    # Phase 1: Unzip
    logger.info("--- Phase 1: Unzip ---")
    unzip_sources()

    # Phase 2: Grid
    logger.info("--- Phase 2: Grid ---")
    grid = build_grid()

    # Phase 3: Aggregate
    if not args.skip_aggregate:
        logger.info("--- Phase 3: Aggregate ---")
        aggregate_all(grid, crops, workers)
    else:
        logger.info("--- Phase 3: SKIPPED ---")

    # Phase 4: Merge + Derive
    logger.info("--- Phase 4: Merge + Derive ---")
    l5 = merge_and_derive(crops)

    # Phase 5: Rollup
    logger.info("--- Phase 5: Rollup ---")
    rollup_levels(l5, crops)

    # Phase 6: Validate
    logger.info("--- Phase 6: Validate ---")
    validate(crops)

    elapsed = time.time() - t_start
    logger.info("Pipeline complete in %.0f min %.0f sec", elapsed // 60, elapsed % 60)


if __name__ == "__main__":
    main()
