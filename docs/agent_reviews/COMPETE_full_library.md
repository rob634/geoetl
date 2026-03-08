# COMPETE Adversarial Review: GeoETL Full Library (excl. h3/)

**Date**: 2026-03-05
**Pipeline**: COMPETE (Adversarial Review)
**Scope**: Full library — config, exceptions, storage, raster, vector, batch, pipeline, vantor
**Scope Split**: C — Data vs Control Flow
**Complexity**: large

---

## Token Usage

| Agent | Role | Tokens | Tool Uses | Duration |
|-------|------|--------|-----------|----------|
| Omega | Scope split (inline) | — | — | inline |
| Alpha | Data Integrity | 55,200 | 19 | 2m 12s |
| Beta | Orchestration | 41,657 | 17 | 2m 21s |
| Gamma | Contradictions | 82,485 | 32 | 3m 38s |
| Delta | Final Report | 50,984 | 22 | 2m 1s |
| **Total** | | **230,326** | **90** | **~10m 12s** |

---

## EXECUTIVE SUMMARY

GeoETL is an early-stage local ETL library with sound architectural foundations — spawn-context multiprocessing, typed Pydantic models, proper raster type-specific compression profiles — but it carries several data-correctness bugs that will silently produce wrong results when processing float32 satellite imagery, which is the project's primary use case. The most dangerous defects are in the zonal statistics path where NaN nodata values, zero-value ambiguity, and missing CRS reprojection can all independently corrupt outputs without raising any errors. The `parallel_map` function's ordering bug will eventually cause a hard-to-diagnose mapping corruption when used with more than one worker. These top issues are all fixable without architectural changes.

---

## TOP 5 FIXES

### Fix 1: NaN nodata comparison with `==` silently skips masking on float32 rasters

**WHAT:** Replace `data == nodata` with NaN-aware comparison across all three locations.

**WHY:** IEEE 754 mandates `NaN != NaN`. When a float32 GeoTIFF has `nodata=nan` (extremely common for elevation, NDVI, etc.), the line `data == raster_nodata` evaluates to all-False. No pixels get masked. The aggregation function then includes nodata pixels as real values, producing silently wrong statistics.

**WHERE:**
- `geoetl/raster/zonal.py`, `zonal_stats`, line 121: `nodata_mask = data == raster_nodata`
- `geoetl/batch/raster_ops.py`, `_zonal_chunk_worker`, line 226: `data[data == nodata] = np.nan`
- `geoetl/raster/validation.py`, `_compute_band_stats`, line 153: `mask = data != nodata`

**HOW:** Create a helper function and use it in all three locations:
```python
def _nodata_mask(data: np.ndarray, nodata_value: float) -> np.ndarray:
    """Build a boolean mask that is True where data equals nodata, handling NaN."""
    if nodata_value is None:
        return np.zeros(data.shape, dtype=bool)
    if np.isnan(nodata_value):
        return np.isnan(data)
    return np.isclose(data, nodata_value)
```
Place in `geoetl/raster/_utils.py` and import in all three files.

**EFFORT:** Small (< 1 hour).

**RISK OF FIX:** Low. Pure function; only changes behavior for NaN-nodata rasters which are currently broken.

---

### Fix 2: `parallel_map` uses `imap_unordered` but docstring promises input-order results

**WHAT:** Replace `pool.imap_unordered` with `pool.imap` in `parallel_map`.

**WHY:** The docstring at line 45 says "List of results in input order." The implementation at line 73 uses `imap_unordered`, which returns results in completion order. Any caller relying on positional correspondence (e.g., `batch_create_cogs` mapping `input_paths[i]` to `results[i]`) gets wrong file path associations. Dormant until workloads have varying per-item latencies.

**WHERE:** `geoetl/batch/pool.py`, `parallel_map`, line 73.

**HOW:** Change:
```python
pool.imap_unordered(func, items, chunksize=chunksize),
```
to:
```python
pool.imap(func, items, chunksize=chunksize),
```

**EFFORT:** Small (< 1 hour).

**RISK OF FIX:** Low. Drop-in replacement, only adds ordering guarantees.

---

### Fix 3: Zonal stats returns `value=0.0` for zones with no valid pixels

**WHAT:** Return `value=None` (change `ZonalResult.value` to `Optional[float]`) when a zone has zero valid pixels.

**WHY:** At line 114 (geometry doesn't overlap) and line 131 (all pixels are nodata), the function returns `value=0.0`. Downstream consumers cannot distinguish "this zone had zero valid pixels" from "the real aggregate is zero." For agricultural or elevation rasters, zero is a valid physical value.

**WHERE:**
- `geoetl/raster/zonal.py`, `ZonalResult` model (line 33) and lines 114, 131.
- `geoetl/batch/raster_ops.py`, `_zonal_chunk_worker`, lines 229, 232.

**HOW:**
1. Change `ZonalResult.value` from `float` to `Optional[float]`.
2. Change sentinel assignments from `value=0.0` to `value=None`.
3. Document that `value=None` means "no valid data in this zone."

**EFFORT:** Small (< 1 hour).

**RISK OF FIX:** Medium. Callers doing arithmetic on `.value` without checking for None will need updating.

---

### Fix 4: No CRS check between zones and raster in `zonal_stats`

**WHAT:** Add a CRS comparison and raise `ZonalStatsError` when zones CRS does not match raster CRS.

**WHY:** If zones are EPSG:4326 and the raster is UTM (or vice versa), the bbox pre-filter matches wrong extents and `rasterio.mask.mask` clips to wrong geographic areas. Silently wrong statistics with no error.

**WHERE:** `geoetl/raster/zonal.py`, `zonal_stats`, between lines 83 and 87.

**HOW:**
```python
if src.crs is not None and zones_gdf.crs is not None:
    if not zones_gdf.crs.equals(src.crs):
        raise ZonalStatsError(
            f"CRS mismatch: zones are {zones_gdf.crs}, raster is {src.crs}. "
            "Reproject zones to match the raster CRS before calling zonal_stats."
        )
```

**EFFORT:** Small (< 1 hour).

**RISK OF FIX:** Low. Only adds a guard on an already-broken condition.

---

### Fix 5: `_fetch_browse_cog` assumes `content-range` header exists

**WHAT:** Add fallback when the server does not return a `content-range` header.

**WHY:** Line 40 does `int(r.headers["content-range"].split("/")[-1])` with no error handling. If the CDN returns 200 instead of 206, this crashes with `KeyError`. Also adds missing timeout on requests calls.

**WHERE:** `geoetl/vantor/cloud_check.py`, `_fetch_browse_cog`, lines 38-48.

**HOW:**
```python
def _fetch_browse_cog(browse_url: str, timeout: int = 30) -> str:
    headers = maxar_headers()
    r = requests.get(browse_url, headers={**headers, "Range": "bytes=0-0"}, timeout=timeout)
    r.raise_for_status()

    content_range = r.headers.get("content-range")
    if content_range:
        total = int(content_range.split("/")[-1])
        r = requests.get(browse_url, headers={**headers, "Range": f"bytes=0-{total - 1}"}, timeout=timeout)
    else:
        r = requests.get(browse_url, headers=headers, timeout=timeout)
    r.raise_for_status()

    tmp = tempfile.NamedTemporaryFile(suffix=".tif", delete=False)
    tmp.write(r.content)
    tmp.close()
    return tmp.name
```

**EFFORT:** Small (< 1 hour).

**RISK OF FIX:** Low. Fallback path is strictly more robust.

---

## ACCEPTED RISKS

1. **`print()` instead of `logging` in `cloud_check.py`** — Interactive-discovery functions ported from notebook code. Not called in batch pipelines. Revisit when automating.

2. **Checkpoint writes O(N^2) total I/O** — Acceptable because checkpoint files are small string keys and raster I/O dwarfs checkpoint time. Revisit at >100K items.

3. **`PipelineConfig` defined in two places** (`config.py:59-65` and `pipeline/types.py:60-68`) — Duplication smell but not a runtime bug. Revisit during config consolidation.

4. **`summarize_results` meaningless timestamps** — By design for post-hoc summary of parallel results. `total_time_seconds` is still useful.

5. **`promote()` allows backward tier demotion** — Storage manager is a thin filesystem wrapper. Revisit if tier semantics gain business rules.

6. **H3 module findings** (edge-pixel clamping, zero-value filtering, non-deterministic dedup, hardcoded dimension names) — Out of scope per review instructions. Should get a dedicated review pass.

7. **API key read at import time** — Works for single-session usage. Revisit if used in long-lived processes.

8. **`batch_zonal_stats` silent drop on raster open failure** — Deliberate resilience pattern for batch processing. Revisit to add failure count/threshold.

---

## ARCHITECTURE WINS

1. **Spawn-context multiprocessing throughout.** `pool.py` uses `mp.get_context("spawn")` consistently with `_worker_init` forcing GDAL re-initialization. This is the correct pattern for rasterio/GDAL fork-safety. Preserve this.

2. **Raster type-specific compression profiles.** The `_COMPRESSION_PROFILES` table in `cog.py` (lines 21-82) correctly maps `(RasterType, COGQuality)` to GDAL settings — JPEG for RGB, DEFLATE+predictor for analysis, LERC for DEM, nearest resampling for categorical. Real domain knowledge encoded.

3. **Chunked zonal statistics with parquet serialization.** `batch_zonal_stats` parallelizes across rasters AND within each raster by chunking zones. Parquet serialization for cross-process zone sharing avoids per-zone IPC overhead.

4. **Atomic checkpoint writes.** `mkstemp` + `Path.replace` for POSIX-atomic rename, with `BaseException` cleanup guard. Correctly handles crash-during-write.

5. **Pydantic v2 models everywhere.** All structured data uses BaseModel with proper types. `GeoETLConfig` uses pydantic-settings with `env_prefix`. `model_post_init` for derived paths is idiomatic.

6. **VRT-based COG merging via subprocess.** `create_cog_from_vrt` uses `gdalbuildvrt` → `gdal_translate -of COG` rather than in-Python merging. Leverages GDAL's optimized C++ codepath.

---

## FINDING SUMMARY

| Severity | Count | Key Issues |
|----------|-------|------------|
| CRITICAL | 2 (in scope) | NaN nodata, imap_unordered ordering |
| HIGH | 10 (in scope) | Zero-value ambiguity, CRS mismatch, content-range crash, exception swallowing, no timeouts |
| MEDIUM | 9 (in scope) | Checkpoint O(N^2), duplicate PipelineConfig, STAC links, type detection |
| LOW | 7 | Zip-slip, FD leak, memory accumulation |
| FALSE POSITIVE | 1 | Pydantic v2 mutable default (safe) |
