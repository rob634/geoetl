# COMPETE Adversarial Review: Overture Maps Pipeline

**Date**: 2026-03-06
**Pipeline**: COMPETE (Adversarial Review)
**Scope**: Overture module + aggregation script — DuckDB/httpfs S3 queries → H3 aggregation → L5/L4/L3 rollup
**Scope Split**: B — Internal vs External
**Split Rationale**: Module sits at a clear boundary between internal SQL/aggregation logic and external S3 Overture data. Split B creates maximum productive tension between SQL correctness (Alpha) and S3/DuckDB resilience (Beta).
**Files Reviewed**: 8 files (4 primary + 4 dependency context)

---

## Token Usage

| Agent | Role | Tokens | Tool Uses | Duration |
|-------|------|--------|-----------|----------|
| Omega | Scope split (inline) | — | — | inline |
| Alpha | Internal Logic & Invariants | 34,712 | 17 | 1m 57s |
| Beta | External Interfaces & Boundaries | 33,824 | 24 | 2m 02s |
| Gamma | Contradictions & Blind Spots | 42,772 | 35 | 3m 00s |
| Delta | Final Report | 31,251 | 13 | 1m 54s |
| **Total** | | **142,559** | **89** | **~8m 53s** |

---

## EXECUTIVE SUMMARY

The Overture subsystem is architecturally sound: it partitions global queries into tiles, checkpoints progress, and uses DuckDB with predicate pushdown against S3 GeoParquet — a well-chosen stack for this scale. However, the tile merge logic contains a confirmed data-correctness bug that inflates all extensive metrics for features near tile boundaries (double-counting), and the building height average is computed against the wrong denominator at both the merge and rollup stages. These three bugs (double-counting, merge denominator, rollup denominator) are the highest priority because they silently produce incorrect output numbers that would propagate into downstream analyses. The remaining issues are defensive-coding gaps that do not currently cause failures but would under edge cases or dependency upgrades.

---

## TOP 5 FIXES

### Fix 1: Tile-boundary double-counting in merge

**WHAT:** Features whose bounding box spans two tiles are returned by both tile queries, assigned to the same H3 cell via centroid, then summed again at merge — inflating all extensive metrics.

**WHY:** Every metric (road lengths, building counts, building areas, POI counts) is overstated for H3 cells near tile boundaries. At 5-degree tiles globally, this affects a significant fraction of cells.

**WHERE:** `scripts/aggregate_overture.py`, `merge_theme()`, lines 182-185. Root cause: `geoetl/overture/aggregation.py`, `_bbox_predicate()`, lines 35-40.

**HOW:** In each SQL template (`transport_sql`, `buildings_sql`, `places_sql`), add a WHERE clause requiring the centroid to fall within the tile bounds:
```sql
AND ST_X(ST_Centroid(geometry)) >= {west} AND ST_X(ST_Centroid(geometry)) < {east}
AND ST_Y(ST_Centroid(geometry)) >= {south} AND ST_Y(ST_Centroid(geometry)) < {north}
```
Use `<` on east/north to avoid double-counting on boundaries. The bbox predicate stays as-is (it enables Parquet row-group pruning), and the centroid filter deduplicates within the pruned result.

**EFFORT:** Medium (2-3 hours — modify 3 SQL builders + test with boundary tile).

**RISK OF FIX:** Low. Adding a tighter WHERE clause only removes duplicates; it cannot introduce new rows.

---

### Fix 2: avg_building_height uses wrong denominator at merge

**WHAT:** `avg_building_height_m` is re-derived as `total_building_height_m / building_count`, but `building_count` includes buildings with no height data.

**WHY:** Systematically underestimates average building height. In areas where height coverage is sparse (common in Overture), the error can be 2-10x.

**WHERE:** `scripts/aggregate_overture.py`, `merge_all_themes()`, lines 217-222. Root cause: `geoetl/overture/aggregation.py`, `buildings_sql()`, lines 100-114 — computes the correct in-tile average but never emits the denominator as a column.

**HOW:**
1. Add to `buildings_sql()`: `COUNT(*) FILTER (WHERE height IS NOT NULL) AS buildings_with_height_count`
2. Add to `BUILDING_COLUMNS` in `aggregation.py`
3. In `merge_all_themes()`, change denominator from `building_count` to `buildings_with_height_count`
4. When `buildings_with_height_count == 0`, set avg to NaN (not 0.0)

**EFFORT:** Small (< 1 hour).

**RISK OF FIX:** Low. Adds one column and fixes arithmetic.

---

### Fix 3: Same wrong denominator propagated to rollup derive_spec

**WHAT:** `derive_spec` for avg_building_height uses `("total_building_height_m", "building_count", 1.0)` — same wrong denominator as Fix 2.

**WHY:** L4 and L3 rollup outputs carry the same underestimated average.

**WHERE:** `scripts/aggregate_overture.py`, `rollup_levels()`, lines 246-249.

**HOW:** After Fix 2 adds `buildings_with_height_count`, change derive_spec to `{"avg_building_height_m": ("total_building_height_m", "buildings_with_height_count", 1.0)}` and add `"buildings_with_height_count": "sum"` to `agg_spec`.

**EFFORT:** Small (< 30 min, couples with Fix 2).

**RISK OF FIX:** Low.

---

### Fix 4: COALESCE(SUM(height), 0) conflates NULL with zero

**WHAT:** `total_building_height_m` is set to 0 for cells where buildings exist but none have height data, making these cells indistinguishable from cells with genuinely zero-height features.

**WHY:** Downstream analysis cannot distinguish "no height data available" from "total height is zero." This matters for the avg_building_height derivation and for any data-quality filtering.

**WHERE:** `geoetl/overture/aggregation.py`, `buildings_sql()`, line 106.

**HOW:** Change `COALESCE(SUM(height), 0)` to just `SUM(height)` — let it be NULL when no buildings have height. In `merge_all_themes()`, do not `fillna(0)` for `total_building_height_m` and `avg_building_height_m` — leave them as NaN. Apply `fillna(0)` only to count and length columns.

**EFFORT:** Small (< 1 hour).

**RISK OF FIX:** Medium. Downstream consumers may assume no NULLs in numeric columns. Audit any `.sum()` or arithmetic that would break on NaN.

---

### Fix 5: pd.isna() on WKB bytes may raise ValueError in pandas >= 2.0

**WHAT:** `pd.isna(b)` is called on a value that may be a `bytes` object from WKB geometry columns. In pandas 2.x, `pd.isna()` on non-scalar bytes can raise `ValueError`.

**WHY:** Would crash `query_to_gdf()` when processing any query that returns geometry, breaking rollup, h3_to_geodataframe, and all DuckDB-backed geometry pipelines.

**WHERE:** `geoetl/duckdb/engine.py`, `query_to_gdf()`, line 83.

**HOW:** Replace the null check with a type-safe guard:
```python
lambda b: wkb.loads(bytes(b)) if b is not None and not isinstance(b, float) else None
```
Or explicitly check `b is pd.NA`. The goal is to guard against both `None` and pandas NA sentinels without calling `pd.isna()` on bytes.

**EFFORT:** Small (< 30 min).

**RISK OF FIX:** Low. Pure defensive check.

---

## ACCEPTED RISKS

1. **SQL via f-string** (`aggregation.py:38-39`, `client.py:26-28`). All interpolated values are developer-controlled floats or Pydantic-validated strings. No injection vector exists today. **Revisit when**: any of these values come from user input.

2. **No explicit anonymous S3 configuration** (`client.py:24-28`). DuckDB httpfs defaults to unsigned requests. Works unless the machine has AWS credentials that interfere. **Revisit when**: users report 403 errors — add `SET s3_access_key_id = ''`.

3. **Singleton connection never closed** (`engine.py:45-51`). Process exit handles cleanup for CLI scripts. **Revisit when**: used in a long-running service or test suite.

4. **`--skip-fetch` with failed tiles produces incomplete data** (`aggregate_overture.py:145-147`). Failed tiles are logged but absent from merge output with no summary warning. **Revisit when**: pipeline runs in production where completeness must be verified.

5. **Rollup SQL missing HAVING NULL filter** (`rollup.py:134-138`). Input data is already filtered by aggregation SQL. Risk only if rollup is called with arbitrary input. **Revisit when**: rollup becomes a general-purpose library function.

6. **`get_overture_connection` mutates singleton** (`client.py:24-28`). Called once per theme, runs redundant INSTALL/LOAD/SET. Idempotent today but wastes DuckDB extension checks. **Revisit when**: performance matters or config varies between calls.

7. **`places_sql` has no subtype filter** (`aggregation.py:130-138`). Counts all records in the place type. May include non-POI records. **Revisit when**: POI categorization matters for downstream analysis.

---

## ARCHITECTURE WINS

1. **Centroid-based H3 assignment in SQL** (`aggregation.py:25-32`). Pushes H3 cell assignment into DuckDB via `h3_h3_to_string(h3_latlng_to_cell(...))`, avoiding Python-side iteration over millions of features.

2. **Checkpoint-and-resume tile processing** (`aggregate_overture.py:103-163`). `CheckpointManager` per-tile with atomic JSON writes. Correct granularity for a multi-hour global pipeline.

3. **DuckDB + httpfs for serverless S3 queries** with `bbox` predicate pushdown. Avoids downloading terabytes. `OvertureConfig` cleanly encapsulates release version, memory limits, and S3 settings.

4. **Separation of SQL generation from execution** (`aggregation.py` builds SQL, `client.py` executes). Enables testing SQL templates without S3 access.

5. **Rollup derive_spec pattern** (`h3/rollup.py:32-160`). Correctly handles intensive vs. extensive variable aggregation, computing derived ratios after summing extensives. Avoids "average of averages" error.

---

## FINDING SUMMARY

| Severity | Count | Key Issues |
|----------|-------|------------|
| HIGH | 3 | Tile double-counting (#1), height denominator merge (#2), height denominator rollup (#3) |
| MEDIUM | 2 | COALESCE NULL conflation (#4), pd.isna bytes fragility (#5) |
| LOW | 12 | SQL f-strings, anon S3, singleton lifecycle, skip-fetch gaps, rollup HAVING, singleton mutation, places filter, merge avg sum, float drift, untyped registry, incomplete exports, dead code |
