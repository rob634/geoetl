# GeoETL — Project Instructions

Your physical platform is an early 21st century so-called "Desktop" computer respectable compute hardware and an RTX-4080. This is an experimental development environment of which you are the custodian and builder with me, the human. The goal is to build prototypes that can be containerized or otherwise deployed in Azure to scale horizontally. Spark clusters in Databricks are a potential as well but not the priority until we get deeper into ML pipelines.

## What This Project Is

GeoETL is a local geospatial ETL library for processing satellite imagery and vector data, with the primary goal of **running SAM2 models on Maxar/Vantor COGs** for segmentation model development.

Origins:
- ETL processing code extracted from [rmhgeoapi](https://github.com/rob634/rmhgeoapi) (Azure Functions geospatial platform, Job→Stage→Task orchestration)
- Vantor/Maxar API client ported from [rmhmlops](https://github.com/rob634/rmhmlops) (satellite imagery ML pipeline)

## Architecture

```
geoetl/
├── config.py              # Pydantic config: StorageTier, RasterType, COG/Tiling/Batch/Pipeline settings
├── exceptions.py          # GeoETLError hierarchy
├── storage/manager.py     # Bronze→Silver→Gold tier management
├── raster/                # Validation, COG, tiling, mosaic, STAC, zonal stats
│   ├── cog.py             # COG creation (rio-cogeo) + VRT→COG merge (GDAL CLI)
│   └── zonal.py           # Zonal statistics (rasterio.mask per polygon, bbox pre-filter)
├── vector/                # Validation (8-stage), format converters, helpers
├── h3/                    # H3 hexagonal grid operations
│   ├── grid.py            # Grid gen/load, h3→polygon, centroids
│   ├── aggregation.py     # Raster→H3 zonal stats, point sampling, extensive/intensive handling
│   ├── rollup.py          # H3 level rollup (L5→L4→L3) with derive_spec for intensive vars
│   └── spatial_join.py    # Polygon→H3 assignment via tiled parallel sjoin
├── batch/                 # Spawn-context multiprocessing pool + parallel ops
│   └── raster_ops.py      # batch COGs, tiles, merge, zonal stats (chunked inner parallelism)
├── pipeline/              # Pipeline orchestration (progress, checkpoint, runner)
│   ├── types.py           # ItemResult, PipelineSummary, PipelineConfig
│   ├── progress.py        # ProgressMonitor (tqdm + periodic status logging)
│   ├── checkpoint.py      # CheckpointManager (atomic JSON, crash-safe resume)
│   └── runner.py          # run_pipeline() orchestrator, summarize_results()
├── overture/              # Overture Maps S3→H3 aggregation via DuckDB
│   ├── config.py          # OvertureConfig, release URL, road classes, building subtypes
│   ├── client.py          # DuckDB httpfs connection, S3 config, tile query executor
│   └── aggregation.py     # SQL template builders per theme (transport/buildings/places)
└── vantor/                # Maxar/Vantor API client (discovery, cloud checking)
    ├── config.py          # API key, base URL, auth headers
    ├── discovery.py       # STAC search, collections, queryables
    └── cloud_check.py     # AOI-level cloud cover estimation via browse COGs
```

**Storage tiers**: Bronze (raw ingest) → Silver (validated/cleaned) → Gold (production-ready COGs, STAC catalogs)

## Vantor/Maxar API

### Authentication
- **Header**: `MAXAR-API-KEY: <key>` — NOT Bearer token, NOT x-api-key
- API key from `.env` file (`MAXAR_API_KEY`), expires April 2026, rotate quarterly
- Base URL: `https://api.maxar.com`

### Key Endpoints
- `POST /discovery/v1/search` — STAC search with CQL2-JSON filters
- `GET /discovery/v1/collections` — list all 46 collections
- `GET /discovery/v1/collections/{id}/queryables` — filterable fields
- Streaming: WMS at `/streaming/v1/ogc/wms`, WMTS at `/streaming/v1/ogc/wmts`

### Important Gotchas
- `bbox` and `intersects` are **mutually exclusive** in search (400 if both sent)
- `eo:cloud_cover` is **strip-level** (50+ km strip), not per-AOI — use `cloud_check.py` for AOI-level filtering
- Browse COG URLs require `Range` header (400 without it, 500 with `Range: bytes=0-`)
- GDAL `vsicurl` **cannot** open browse URLs (encoded query params break it) — must download to temp file
- WMS version matters: 1.1.1 uses lon,lat; 1.3.0 uses lat,lon

### Collections (best for SAM training)
- `cloud-optimized-archive` — unified search across all satellites (2001+)
- Legion (LG01-06): 30cm pan, 1.2m MS, 9 bands (2024+)
- WorldView-3: 31cm pan, 1.24m MS, 8 VNIR + 8 SWIR (2014+)
- WorldView-2: 46cm pan, 1.85m MS, 8 bands (2009+)
- GeoEye-1: 41cm pan, 1.65m MS, 4 bands (2008+)

### Cloud Check Two-Stage Filter
1. Discovery pre-filter: strip cloud <= 30%, off-nadir <= 25° (generous to catch scenes where clouds are outside AOI)
2. Browse COG pixel analysis: brightness > 180 AND saturation < 0.2 = cloud

## Tech Stack & Conventions

- **Python >=3.10**, built with setuptools
- **Raster**: rasterio, rio-cogeo, rio-stac, cogeo-mosaic
- **Vector**: geopandas, shapely, fiona
- **API**: requests (Vantor/Maxar client)
- **Config**: pydantic / pydantic-settings (Pydantic v2 patterns), python-dotenv
- **ML**: torch, torchvision, SAM-2 (optional `[sam2]` extra)
- **Linting**: ruff, line-length=120, target py310
- **Tests**: pytest, tests/ directory
- **Conda env**: `geo`

## Coding Standards

- Use type hints on all public functions
- Use Pydantic models for configuration and data structures (v2 style)
- Multiprocessing must use spawn context (not fork) for GDAL/rasterio safety
- Raster I/O goes through rasterio; vector I/O goes through geopandas/fiona
- Keep processing functions pure where possible — accept paths/data in, return results out
- Compression profiles are raster-type-specific (JPEG for RGB, DEFLATE for DEM, etc.)
- Secrets in `.env`, never hardcode API keys in source

## Upstream References

- **rmhgeoapi** (ETL patterns): https://github.com/rob634/rmhgeoapi
  - Orchestration: `core/machine.py`, Jobs: `jobs/`, Services: `services/`
- **rmhmlops** (Vantor API exploration): https://github.com/rob634/rmhmlops
  - API docs: `docs/discovery-api.md`, session notes: `docs/working-memory.md`

## Environment

- Development machine: Ubuntu desktop, VS Code
- GitHub account: rob634 (SSH auth)
- Git remote: git@github.com:rob634/geoetl.git
