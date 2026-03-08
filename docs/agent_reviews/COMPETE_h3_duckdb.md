# COMPETE Adversarial Review: H3 + DuckDB Subsystem

**Date**: 2026-03-06
**Pipeline**: COMPETE (Adversarial Review)
**Scope**: H3 hexagonal grid module + DuckDB engine layer
**Scope Split**: A — Design vs Runtime
**Split Rationale**: Module was just rewritten from pandas+multiprocessing to DuckDB SQL. Split A creates maximum friction between architecture elegance (Alpha) and runtime correctness of generated SQL and resource management (Beta).
**Files Reviewed**: 10 files (7 primary + 3 dependency context)

---

## Token Usage

| Agent | Role | Tokens | Tool Uses | Duration |
|-------|------|--------|-----------|----------|
| Omega | Scope split (inline) | — | — | inline |
| Alpha | Architecture & Design | 42,704 | 15 | 2m 19s |
| Beta | Correctness & Reliability | 42,547 | 14 | 1m 57s |
| Gamma | Contradictions & Blind Spots | 53,059 | 16 | 3m 14s |
| Delta | Final Report | 38,874 | 10 | 1m 31s |
| **Total** | | **177,184** | **55** | **~9m 01s** |

---

## EXECUTIVE SUMMARY

The H3/DuckDB subsystem is functionally complete and demonstrates strong architectural thinking — DuckDB for vectorized SQL operations, spawn-safe multiprocessing, and clean separation of extensive vs. intensive variable aggregation. However, the zonal aggregation worker has two compounding data-loss bugs (silent exception swallowing and unconditional zero-exclusion) that will silently drop hexagons from output with no diagnostic trace. DuckDB resource management is the second systemic concern: every module uses `register()`/`unregister()` pairs outside `try/finally`, meaning any SQL error leaks tables into the singleton connection. The singleton itself has no cleanup lifecycle. These five fixes address confirmed bugs that either lose data silently or leak resources under failure conditions.

---

## TOP 5 FIXES

### Fix 1: Silent Exception Swallowing Drops Hexagons

**WHAT**: The inner `except Exception: continue` in `_zonal_chunk_worker` silently discards hexagons that fail rasterio masking, with zero logging or error propagation.

**WHY**: Any masking failure (corrupt geometry, edge-case rasterio error, CRS issue) causes that hexagon to vanish from results. The outer `except` at line 117 also swallows full-chunk failures. This is undetectable data loss in production. Combined with Fix 2, the result set can be dramatically smaller than expected with no warning.

**WHERE**: `geoetl/h3/aggregation.py`, function `_zonal_chunk_worker`, lines 115-118.

**HOW**: Replace `except Exception: continue` with:
```python
except Exception as e:
    logger.debug("Hex %s failed: %s", row.get("h3_index", "?"), e)
    results.append({"h3_index": row["h3_index"], "value": float("nan"), "pixel_count": 0})
```
This preserves the hexagon in the result set with NaN, making it visible in downstream analysis. The outer except (line 117) should re-raise after logging, or at minimum append a structured error marker to results.

**EFFORT**: Small (< 1 hour).

**RISK OF FIX**: Low. Only changes error handling; does not alter the happy path.

---

### Fix 2: Zero-Valid-Pixel Hexagons Silently Dropped + Hardcoded Zero Exclusion

**WHAT**: Two compounding issues: (a) `valid = valid[valid != 0]` on line 105 unconditionally strips zero-valued pixels for all agg methods except COUNT, and (b) `if len(valid) == 0: continue` on lines 107-108 drops hexagons where all remaining pixels are zero or NaN.

**WHY**: Zero is a legitimate raster value in most datasets (elevation, temperature, spectral bands). This filter was written for MapSPAM crop data (where 0 = no-crop) but is hardcoded for all callers. Hexagons over ocean at 0m elevation, or any area with legitimate zero values, are silently excluded.

**WHERE**: `geoetl/h3/aggregation.py`, function `_zonal_chunk_worker`, lines 105-108. Also `geoetl/h3/grid.py`, function `filter_grid_to_raster`, lines 226-227.

**HOW**: Add an `exclude_zero: bool = True` parameter to `_ChunkTask` and `zonal_aggregate`. In the worker, gate line 105 behind `if task.exclude_zero:`. Change the default to `False` (safe for general use) and document that MapSPAM callers should pass `exclude_zero=True`. Same pattern for `filter_grid_to_raster`. For the `continue` on line 108, emit a NaN-valued result dict instead of skipping.

**EFFORT**: Small (< 1 hour).

**RISK OF FIX**: Low. Adds a parameter with backward-compatible default; existing MapSPAM scripts just need `exclude_zero=True`.

---

### Fix 3: DuckDB Tables Leak on Exception (No try/finally)

**WHAT**: Every DuckDB `register()`/`unregister()` pair in the codebase is unprotected — if the SQL between them raises, the registered table persists in the singleton connection indefinitely.

**WHY**: Leaked tables pollute the DuckDB namespace. On retry or concurrent use, name collisions cause `CatalogException`. Because the connection is a process-lifetime singleton, leaked tables accumulate. Affects `aggregate_extensive` (lines 380-405), `rollup` (lines 129-135), `assign_polygons` (lines 64-84), `h3_to_geodataframe` (lines 44-58), and `extract_centroids` (lines 132-137).

**WHERE**: All five functions across `geoetl/h3/aggregation.py`, `geoetl/h3/rollup.py`, `geoetl/h3/spatial_join.py`, `geoetl/h3/grid.py`.

**HOW**: Create a context manager in `engine.py`:
```python
@contextmanager
def registered_table(conn, name, df):
    conn.register(name, df)
    try:
        yield
    finally:
        conn.unregister(name)
```
Refactor all call sites to use `with registered_table(conn, "_rollup_input", input_df):`.

**EFFORT**: Medium (1-4 hours). Five call sites to refactor, but the pattern is mechanical.

**RISK OF FIX**: Low. Pure structural refactor; SQL logic unchanged.

---

### Fix 4: Nodata Handling Inconsistency (Exact Equality vs isclose)

**WHAT**: `_zonal_chunk_worker` uses exact equality (`data[data == nodata] = np.nan` at line 102) while `_utils.nodata_mask` uses `np.isclose(data, nodata_value)`. The same raster processed through `zonal_aggregate` vs `zonal_stats` will produce different results for floating-point nodata values.

**WHY**: Floating-point nodata values (e.g., -3.4028235e+38) commonly suffer from precision drift after `astype(float)` at line 99. Exact equality silently fails to mask these pixels, including them as valid data. This inflates sums and distorts means. The `raster/zonal.py` path already uses `nodata_mask` correctly (line 130).

**WHERE**: `geoetl/h3/aggregation.py`, function `_zonal_chunk_worker`, lines 101-102.

**HOW**: Import `nodata_mask` from `geoetl.raster._utils` inside the worker function (already an internal module, safe in spawn context). Replace lines 101-102 with:
```python
from geoetl.raster._utils import nodata_mask as _nodata_mask
nd = _nodata_mask(data, nodata)
data[nd] = _np.nan
```

**EFFORT**: Small (< 1 hour).

**RISK OF FIX**: Low. Strictly more correct; only changes masking for edge-case float nodata values.

---

### Fix 5: derive_spec in Rollup Hardcodes SUM() Independent of agg_spec

**WHAT**: In `rollup()`, the `derive_spec` SQL generation (lines 112-116) always uses `SUM()` regardless of what aggregation was specified in `agg_spec` for those columns.

**WHY**: If a user specifies `agg_spec={"production_mt": "mean", ...}` but also has a `derive_spec` referencing `production_mt`, the derived column uses SUM while the aggregated column uses AVG. The two columns in the same row are computed from different aggregation logic, producing silently inconsistent results.

**WHERE**: `geoetl/h3/rollup.py`, function `rollup`, lines 112-116.

**HOW**: Assert that both numerator and denominator columns use `"sum"` in `agg_spec`, raising `H3Error` if not:
```python
for col_name, (num_col, denom_col, scale) in derive_spec.items():
    if dict(agg_columns).get(num_col) != "sum" or dict(agg_columns).get(denom_col) != "sum":
        raise H3Error(f"derive_spec requires SUM aggregation for {num_col} and {denom_col}")
```

**EFFORT**: Small (< 1 hour).

**RISK OF FIX**: Low. Adds a validation gate; existing correct callers already use "sum".

---

## ACCEPTED RISKS

1. **Singleton DuckDB connection with no DI path** (`engine.py:13-50`). The module-level singleton with double-checked locking works for the current single-process, single-user prototype workflow. A proper DI/factory pattern would be needed for containerized deployment or testing with isolated connections. **Revisit when**: writing integration tests needing connection isolation, or moving to multi-process serving.

2. **Hardcoded table names cause theoretical race conditions** (`_rollup_input`, `_h3_points`, `_polygons`, etc.). All DuckDB operations run in a single-threaded context today. **Revisit when**: introducing async/threaded DuckDB access or running multiple pipelines concurrently.

3. **NetCDF dimension names hardcoded as "lat"/"lon"** (`aggregation.py:293`). Correct for SPEI, the only current NetCDF consumer. **Revisit when**: adding a new NetCDF source with different coordinate conventions.

4. **`_sample_geotiff` clamps out-of-bounds to edge pixels** (`aggregation.py:308-309`). Acceptable for coarse-resolution sources (SPEI 0.25 deg) where edge values are still meaningful. **Revisit when**: using point_sample with high-resolution rasters.

5. **`generate_grid()` memory at high resolutions** (`grid.py:146`). At L7+ with global bounds this could produce hundreds of millions of cells. Current use case is L5 (832K cells). **Revisit when**: supporting resolutions finer than L6.

6. **`warnings.filterwarnings("ignore")` in workers** (`aggregation.py:60-61`). Suppresses GDAL/rasterio warnings. Acceptable during development. **Revisit when**: debugging data quality issues or moving to production.

7. **SQL identifiers unquoted in some positions** (`rollup.py:96` for `h3_col`; table names `_agg_{col_name}` in `aggregation.py:383`). Current callers use well-known column names. **Revisit when**: accepting user-supplied column names that could contain special characters.

8. **No DuckDBError in exception hierarchy** (`exceptions.py`). DuckDB errors bubble as raw `duckdb.Error`. Adding a wrapper would improve catch specificity. **Revisit when**: adding error recovery logic that distinguishes DuckDB failures from other errors.

---

## ARCHITECTURE WINS

1. **Extensive/intensive variable separation** (`aggregation.py:325-469`). The `aggregate_extensive()` + `derive_yield()` pattern correctly enforces that intensive variables (yield) must be derived from rolled-up extensives (production/area), never averaged directly. Getting this right at the API level prevents a common and costly geospatial aggregation mistake.

2. **DuckDB for vectorized H3 operations** (`grid.py:42-59`, `rollup.py:96-127`). Using DuckDB's H3 extension for `h3_cell_to_parent`, `h3_cell_to_boundary_wkt`, and centroid extraction replaces Python loops. The SQL-first approach in `rollup()` handles parent mapping, aggregation, derived columns, and geometry in a single query.

3. **Spawn-context multiprocessing** (`aggregation.py:206`). `mp.get_context("spawn")` for rasterio/GDAL workers avoids fork-safety issues. `_ChunkTask` with serializable fields (strings, ints, no file handles) is the correct spawn-context pattern.

4. **Bbox pre-filtering** (`aggregation.py:164-168`, `zonal.py:97-100`). `.cx[]` spatial filtering before expensive per-hexagon masking eliminates the majority of hexagons for regional rasters against global grids.

5. **Clean module boundaries** (`h3/__init__.py`). Explicit `__all__` with 11 public functions. Internal helpers properly prefixed with `_`. Public API stable across the DuckDB migration — same function names and signatures.

---

## FINDING SUMMARY

| Severity | Count | Key Issues |
|----------|-------|------------|
| HIGH | 4 | Silent hex dropping (#1, #2), table leaks (#3), nodata inconsistency (#4) |
| MEDIUM | 7 | derive_spec SUM hardcode, edge-pixel clamping, empty GDF crash, NetCDF dims, dead code, blanket warning suppression, no DuckDB exception |
| LOW | 7 | Unused method param, GeoJSON validation, EPSG:4326 default, empty __init__, dead gdf_to_table, fiona bypass, inconsistent worker imports |
| INFORMATIONAL | 2 | Network-required extension install, spatial join result size |
