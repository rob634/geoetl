# Pipeline Run History

## Run 001 — Global Road Extract

**Date:** 2026-03-07
**Pipeline:** `scripts/road_pipeline.py extract --workers 4`
**Source:** Overture Maps S3 (`release/2026-02-18.0`, `theme=transportation/type=segment`)
**Output:** `outputs/overture/roads/extract/tiles/`

### Result

| Metric | Value |
|--------|-------|
| Total tiles | 1,944 (5° grid, -180°–180° lon, -60°–75° lat) |
| Non-empty tiles | 1,020 |
| Empty tiles (ocean/uninhabited) | 924 |
| Total features | 239,618,756 |
| Total disk | 76.7 GB |
| Parquet files | 1,020 |

### Execution

Completed across 3 runs due to a crash on the first attempt.

| Run | Tiles | Wall time | Status | Notes |
|-----|-------|-----------|--------|-------|
| 0 (prior session) | 377 | unknown | OK | No metrics retained |
| 1 (disrupted) | 110 | 12 min | CRASHED | DuckDB OOM — 4 workers × 16GB = 64GB exceeded system RAM |
| 2 (after fix) | 1,457 | 89 min | OK | 4 workers × 12GB = 48GB, 16GB headroom |

**Full rerun estimate:** ~2 hours from scratch at 4 workers.

### Run 2 Detail (successful run)

**Workers:** 4 (spawn context, each with independent DuckDB connection)
**DuckDB memory:** 12GB per worker (48GB total, 16GB OS/Python headroom on 64GB system)
**Parallelism efficiency:** 4.0x (near-perfect)

Per-worker breakdown:

| Worker PID | Tiles | Non-empty | Features | Data | Task time |
|------------|-------|-----------|----------|------|-----------|
| 2056936 | 348 | 135 | 48,691,312 | 11.0 GB | 88.7 min |
| 2056937 | 291 | 126 | 60,250,264 | 14.5 GB | 88.6 min |
| 2056938 | 469 | 203 | 46,292,742 | 10.7 GB | 88.6 min |
| 2056939 | 349 | 146 | 61,006,907 | 13.8 GB | 88.4 min |

Query time distribution (non-empty tiles):

| Percentile | Time |
|------------|------|
| min | 0.8s |
| p25 | 5.4s |
| median | 10.2s |
| p75 | 48.2s |
| p95 | 109.6s |
| max | 203.4s |

### Output Schema

Each parquet file contains individual road segments with 8 columns:

| Column | Type | Description |
|--------|------|-------------|
| `id` | str | Overture UUID |
| `source_dataset` | str | Always "OpenStreetMap" |
| `osm_id` | str | OSM way ID + version |
| `class` | str | Road class (14 types) |
| `connectors` | struct | Junction topology `{at: float, connector_id: UUID}` |
| `length_m` | float64 | Geodesic length in meters |
| `h3_l5` | str | H3 level-5 cell assignment (centroid-based) |
| `geom_wkb` | bytes | Road geometry as WKB |

### File Naming

Parquet files are named `{longitude}_{latitude}.parquet` — the southwest corner of each 5° tile:

```
+0005.0_+045.0.parquet  →  5°E–10°E, 45°N–50°N  (France)
-0075.0_+040.0.parquet  →  75°W–70°W, 40°N–45°N  (NYC/Philadelphia)
```

Sign convention: `+` = east/north, `-` = west/south. Zero-padded `%+07.1f_%+06.1f`.

### Top 5 Largest Tiles

| Tile | Size | Region |
|------|------|--------|
| `+0005.0_+045.0` | 1,975 MB | France |
| `+0005.0_+050.0` | 1,538 MB | Belgium / Netherlands / Germany |
| `+0010.0_+045.0` | 1,522 MB | Germany / Switzerland / Austria |
| `-0005.0_+050.0` | 1,238 MB | London / SE England |
| `-0075.0_+040.0` | 1,080 MB | NYC / Philadelphia |

File size distribution: min 0.006 MB, median 9.5 MB, mean 75.2 MB, p95 372 MB, max 1,975 MB.

### Incident: DuckDB OOM Crash (Run 1)

**Root cause:** `OvertureConfig.memory_limit` was set to 16GB per connection. Each spawned worker creates an independent DuckDB instance, so 4 × 16GB = 64GB — the entire system RAM. When dense European tiles (France, Belgium/Netherlands) pushed multiple workers toward their limits simultaneously, one hit DuckDB's internal `OutOfMemoryException`.

**Evidence:**
- No kernel OOM killer entries in dmesg (DuckDB enforces its own limit)
- Workers processing France/Benelux went silent ~5 min before crash
- One tile (`+0000.0_+035.0`) was written to disk but never checkpointed — worker completed but `pool.imap_unordered` lost the result when another worker crashed
- `_extract_tile_worker` had no error handling — single worker failure killed the entire pool

**Fixes applied:**
1. Memory budget division: `per_worker_gb = 48GB // num_workers` (12GB each at 4 workers)
2. Worker error handling: try/except returns error dict instead of crashing the pool
3. Sequential mode: reconnects after errors to handle broken DuckDB connections
4. Failed tiles are not checkpointed, so they retry automatically on next run
