# rmhtitiler API Test Report

**Service:** `https://rmhtitiler-ghcyd7g0bxdvc2hc.eastus-01.azurewebsites.net`
**Version:** geotiler v0.9.2.6
**Date:** 2026-03-07
**Stack:** TiTiler + TiPG + stac-fastapi on Azure App Service (Premium0V3, eastus, 2 CPUs, 4.68GB RAM)

---

## Table of Contents

1. [Executive Summary](#executive-summary)
2. [Infrastructure Status](#infrastructure-status)
3. [Endpoint Inventory](#endpoint-inventory)
4. [Part 1: API Integrator Testing](#part-1-api-integrator-testing)
   - [TiPG Vector](#tipg-vector-18-endpoints)
   - [TiTiler COG](#titiler-cog-21-endpoints)
   - [TiTiler xarray](#titiler-xarray-18-endpoints)
   - [STAC Catalog](#stac-catalog-12-endpoints)
   - [pgSTAC Searches](#pgstac-searches-16-endpoints)
   - [OpenAPI Compliance](#openapi-compliance-audit)
5. [Part 2: QGIS Integration Testing](#part-2-qgis-integration-testing)
6. [Issues](#issues)
7. [QGIS Workarounds](#qgis-workarounds-current-state)

---

## Executive Summary

The geotiler API surface consists of 90 endpoints across 5 service groups. Core tile serving (TiPG vector tiles, TiTiler COG tiles) is functional and performant. However, **two critical bugs** block the primary integration paths that QGIS and OGC-compliant clients depend on:

1. **Landing pages return 500** — both `/vector/` and `/stac/` fail with a missing `swagger_ui_html` route, breaking all OGC API client discovery
2. **bbox filtering returns 500** — a PostgreSQL `st_transform` ambiguity error on every collection, breaking spatial queries

These two bugs alone account for all 3 QGIS test failures (out of 12 tests).

---

## Infrastructure Status

From `GET /health`:

| Metric | Value |
|--------|-------|
| Uptime | 66,292s |
| Response time | 19.3ms |
| RAM utilization | 92.6% (356.9MB free of 4.68GB) |
| CPU utilization | 40.0% |
| DB ping | 18.87ms (Azure PostgreSQL) |
| OAuth tokens | Valid ~17.5h remaining |
| Services healthy | cog, xarray, pgstac, tipg, h3, stac_api |

**Note:** RAM at 92.6% utilization. Several endpoints timeout under load (vector diagnostics, COG preview, COG point queries). Consider whether the Premium0V3 SKU is sufficient for production workloads.

---

## Endpoint Inventory

| Group | Path Prefix | Endpoints | Backing Service | Status |
|-------|-------------|-----------|----------------|--------|
| Vector | `/vector/*` | 18 | TiPG (PostGIS) | Functional, 2 critical bugs |
| COG | `/cog/*` | 21 | TiTiler | Functional |
| xarray | `/xarray/*` | 18 | TiTiler (xarray) | Untestable (no data) |
| STAC | `/stac/*` | 12 | stac-fastapi (pgSTAC) | Empty catalog, landing page broken |
| Searches | `/searches/*` | 16 | pgSTAC mosaic | Empty (0 registered) |
| Admin | `/admin/*` | 1 | Internal | Not tested |
| Health | `/health`, `/livez`, `/readyz` | 3 | Internal | Working |
| API | `/api` | 1 | FastAPI | Partial |

**Total: 90 endpoints**

---

## Part 1: API Integrator Testing

### TiPG Vector (18 endpoints)

**Data available:** 27 collections in the `geo` schema — points (ACLED conflict events, CSV imports), polygons (cutlines, huge polygons, GeoPackage), lines (KML roads, shapefiles), and mixed geometry types.

#### Passing Tests

| Test | Endpoint | Result |
|------|----------|--------|
| List collections | `GET /vector/collections` | 27 collections with bbox, links, tiles |
| Collection metadata | `GET /vector/collections/{id}` | Proper links (self, items, queryables, tiles) |
| Get items | `GET /vector/collections/{id}/items?limit=2` | FeatureCollection with numberMatched/numberReturned |
| Single item | `GET /vector/collections/{id}/items/{itemId}` | Feature with self/collection links |
| Queryables | `GET /vector/collections/{id}/queryables` | JSON Schema with field types |
| CQL2 text filter | `?filter=country='Mexico'&filter-lang=cql2-text` | 352 matches, correct filtering |
| Property selection | `?properties=country,event_type,fatalities` | Only requested fields returned |
| Pagination | `?limit=2&offset=2` | next/prev links, correct offset |
| CSV export | `?f=csv` | Full CSV with WKT geometry |
| HTML negotiation | `Accept: text/html` | HTTP 200 |
| OGC Conformance | `GET /vector/conformance` | Features, Tiles, Filter, MVT declared |
| TileJSON | `GET /vector/collections/{id}/tiles/WebMercatorQuad/tilejson.json` | TileJSON 3.0.0, vector_layers, bounds |
| Vector tile | `GET /vector/collections/{id}/tiles/WebMercatorQuad/{z}/{x}/{y}` | MVT (application/vnd.mapbox-vector-tile), 7KB+ tiles |
| Style JSON | `GET /vector/collections/{id}/tiles/WebMercatorQuad/style.json` | Mapbox GL compatible |
| TileMatrixSets | `GET /vector/tileMatrixSets` | 13 sets (WebMercatorQuad, WGS1984Quad, etc.) |
| CORS preflight | `OPTIONS /vector/collections` | `Access-Control-Allow-Origin: *` |
| Content-Type | Items endpoint | `application/geo+json` |

```bash
# Reproduce: CQL filtering works
curl -s "https://rmhtitiler-ghcyd7g0bxdvc2hc.eastus-01.azurewebsites.net/vector/collections/geo.sg7_csv_test_acled_small_ord1/items?filter=country%3D'Mexico'&filter-lang=cql2-text&limit=2" | python3 -m json.tool | head -5
```

#### Failing Tests

See [Issues](#issues) — BUG-001 (landing page), BUG-002 (bbox), BUG-003 (item not found status), BUG-004 (error status codes).

---

### TiTiler COG (21 endpoints)

Tested with a public Sentinel-2 COG: `S2A_36NYF_20230101_0_L2A/TCI.tif` (10980x10980, 3 bands, uint8, EPSG:32636).

#### Passing Tests

| Test | Endpoint | Result |
|------|----------|--------|
| Info | `GET /cog/info?url=...` | bounds, CRS, bands, dtype, overviews, nodata |
| TileJSON | `GET /cog/WebMercatorQuad/tilejson.json?url=...` | Tile URL template, zoom 8-14, WGS84 bounds |
| Tile z=8 | `GET /cog/tiles/WebMercatorQuad/8/153/127@1x.png?url=...` | 200, 54KB valid PNG (256x256 RGBA) |
| Tile z=11 | `GET /cog/tiles/WebMercatorQuad/11/1228/1021@1x.png?url=...` | 200, 136KB valid PNG |
| Missing URL param | `GET /cog/info` | 400 with clear validation error |

```bash
# Reproduce: COG info
curl -s "https://rmhtitiler-ghcyd7g0bxdvc2hc.eastus-01.azurewebsites.net/cog/info?url=https://sentinel-cogs.s3.us-west-2.amazonaws.com/sentinel-s2-l2a-cogs/36/N/YF/2023/1/S2A_36NYF_20230101_0_L2A/TCI.tif" | python3 -m json.tool

# Reproduce: COG tile
curl -s -o tile.png "https://rmhtitiler-ghcyd7g0bxdvc2hc.eastus-01.azurewebsites.net/cog/tiles/WebMercatorQuad/8/153/127@1x.png?url=https://sentinel-cogs.s3.us-west-2.amazonaws.com/sentinel-s2-l2a-cogs/36/N/YF/2023/1/S2A_36NYF_20230101_0_L2A/TCI.tif"
file tile.png  # PNG image data, 256 x 256, 8-bit/color RGBA
```

#### Timeouts / Intermittent Failures

| Test | Endpoint | Result |
|------|----------|--------|
| Tile z=9 | `GET /cog/tiles/.../9/306/255@1x.png` | Timeout (HTTP 000) |
| Tile z=10 | `GET /cog/tiles/.../10/612/510@1x.png` | Timeout (HTTP 000) |
| Point query | `GET /cog/point/34.8,0.5?url=...` | Timeout |
| Preview | `GET /cog/preview.png?url=...` | Timeout |
| Map viewer | `GET /cog/WebMercatorQuad/map.html?url=...` | Timeout |
| Bbox image | `GET /cog/bbox/34.9,0.1,35.1,0.3/256x256.png?url=...` | Timeout |

Likely cause: the App Service's 230s request timeout (or lower Azure front-end timeout) combined with remote S3 COG read latency. Tiles at z=8 and z=11 work because they hit overview levels that read fast; z=9/z=10 may hit intermediate overviews with larger reads.

---

### TiTiler xarray (18 endpoints)

| Test | Endpoint | Result |
|------|----------|--------|
| Variables (no URL) | `GET /xarray/variables` | Empty response (no error) |
| Info (no URL) | `GET /xarray/info` | Empty response (no error) |
| Variables (public Zarr) | `GET /xarray/variables?url=...` | Empty response |
| Info (public Zarr) | `GET /xarray/info?url=...` | Empty response |

**Status:** All xarray endpoints return empty responses — no error message, no validation, just nothing. Tested with HadISST Zarr store from Pangeo. Either the xarray reader can't reach the remote store, or it requires specific configuration not documented in the API.

**OpenAPI concern:** Missing `url` parameter should return 400 (like COG does), not an empty 200.

---

### STAC Catalog (12 endpoints)

| Test | Endpoint | Result |
|------|----------|--------|
| Landing page | `GET /stac/` | **500** — swagger_ui_html route error |
| Collections | `GET /stac/collections` | 200, 0 collections |
| Conformance | `GET /stac/conformance` | 200, 15 conformance classes |
| Queryables | `GET /stac/queryables` | 200, empty properties |
| Search GET | `GET /stac/search?limit=1` | 200, 0 features |
| Search POST | `POST /stac/search` | 200, 0 features |
| Ping | `GET /stac/_mgmt/ping` | 200, `{"message":"PONG"}` |
| Health | `GET /stac/_mgmt/health` | 200, `{"status":"UP"}` |

**Status:** The STAC service is healthy and structurally correct (proper conformance, search shape, pagination links), but the catalog is empty — no collections or items ingested. The landing page suffers from the same BUG-001 as the vector service.

STAC conformance classes declared:
- `stacspec.org/v1.0.0/core`, `/collections`, `/item-search`, `/item-search#fields`, `/item-search#sort`, `/ogcapi-features`
- CQL2: basic, JSON, text
- OGC Features: core, geojson, filter

---

### pgSTAC Searches (16 endpoints)

```bash
curl -s "https://rmhtitiler-ghcyd7g0bxdvc2hc.eastus-01.azurewebsites.net/searches/" | python3 -m json.tool
```
```json
{
  "searches": [],
  "links": [{"href": ".../searches/list?limit=10&offset=0", "rel": "self"}],
  "context": {"returned": 0, "limit": 10, "matched": 0}
}
```

**Status:** No mosaic searches registered. Endpoints respond correctly but have no data. Tile/point/statistics endpoints untestable without a registered search ID.

---

### OpenAPI Compliance Audit

| Check | Expected | Actual | Verdict |
|-------|----------|--------|---------|
| OpenAPI spec accessible | `GET /openapi.json` returns spec | Returns full spec (90 paths) | PASS |
| Spec parseable | Valid OpenAPI 3.x | Parseable, all paths enumerable | PASS |
| `/api` metadata | `openapi_url` and `docs_url` populated | Both `null` | FAIL |
| `/vector/` landing page | 200 with links per OGC API | 500 (swagger_ui_html) | FAIL |
| `/stac/` landing page | 200 with links per STAC spec | 500 (swagger_ui_html) | FAIL |
| Resource not found → 404 | OGC API spec | 500 (item), 422 (collection) | FAIL |
| Client error → 4xx | HTTP semantics | 500 for CQL parse errors, bbox errors | FAIL |
| Content-Type headers | Correct per resource type | `application/geo+json` for features, `application/vnd.mapbox-vector-tile` for MVT | PASS |
| Pagination links | `next`/`prev` with `rel` and `type` | Correct | PASS |
| CORS | `Access-Control-Allow-Origin` | Present on OPTIONS preflight | PASS |
| `Link` response headers | OGC recommends rel links in headers | Not present | WARN |
| Swagger/Redoc UI | `/reference` or `/docs` | `/reference` works (Swagger UI) | PASS |

---

## Part 2: QGIS Integration Testing

**QGIS version:** 3.40.9-Bratislava (Ubuntu, system Python 3.12)
**Test method:** Headless PyQGIS with `QT_QPA_PLATFORM=offscreen`

### Results: 9/12 Passed

| # | Test | Provider | Layer Class | Result |
|---|------|----------|-------------|--------|
| A1 | OAPIF — Points | `OAPIF` | QgsVectorLayer | **FAIL** |
| A2 | OAPIF — Polygons | `OAPIF` | QgsVectorLayer | **FAIL** |
| A3 | GeoJSON direct — Points | `ogr` | QgsVectorLayer | **PASS** (100 features, EPSG:4326) |
| A4 | GeoJSON direct — Polygons | `ogr` | QgsVectorLayer | **PASS** (100 features) |
| A5 | GeoJSON + CQL filter | `ogr` | QgsVectorLayer | **PASS** (100 features) |
| A6 | GeoJSON + bbox | `ogr` | QgsVectorLayer | **FAIL** |
| A7 | GeoJSON + properties filter | `ogr` | QgsVectorLayer | **PASS** (100 features) |
| A8 | MVT — Points | `vectortile` | QgsVectorTileLayer | **PASS** |
| A9 | MVT — Polygons | `vectortile` | QgsVectorTileLayer | **PASS** |
| A10 | MVT — Lines | `vectortile` | QgsVectorTileLayer | **PASS** |
| B1 | COG direct (vsicurl) | `gdal` | QgsRasterLayer | **PASS** (10980x10980, 3 bands) |
| B2 | TiTiler XYZ tiles | `wms` | QgsRasterLayer | **PASS** (EPSG:3857) |

### Failure Analysis

**A1/A2 — OAPIF provider fails:**
QGIS's OAPIF provider fetches the landing page (`/vector/`) first to discover the API structure and enumerate collections. Since `/vector/` returns 500, the provider silently fails with `isValid()=False` and no error message.

```python
# Reproduction (PyQGIS)
from qgis.core import QgsApplication, QgsVectorLayer
uri = "url='https://rmhtitiler-ghcyd7g0bxdvc2hc.eastus-01.azurewebsites.net/vector' typename='geo.sg7_csv_test_acled_small_ord1'"
layer = QgsVectorLayer(uri, "test", "OAPIF")
print(layer.isValid())  # False
```

**A6 — GeoJSON + bbox fails:**
Server returns 500 on any bbox query (BUG-002). Since QGIS/OGR forwards the bbox parameter to the server, this fails silently.

### QGIS Provider Notes

- **`OAPIF` provider** requires a working OGC API landing page. No workaround exists within this provider.
- **`ogr` provider** works with direct HTTP GeoJSON URLs — GDAL/OGR fetches the full response and parses it as GeoJSON. This is a viable workaround but not scalable (loads all features into memory).
- **`QgsVectorTileLayer`** (not `QgsVectorLayer` with `vectortile` provider) is the correct class for MVT. The XYZ URL template format works directly.
- **`wms` provider** handles XYZ raster tile templates for TiTiler COG tiles.
- **`gdal` provider** with `/vsicurl/` prefix opens remote COGs directly via GDAL's virtual filesystem.

---

## Issues

### BUG-001: Landing pages return 500 (swagger_ui_html route missing)

**Severity:** CRITICAL
**Affects:** `/vector/`, `/stac/`
**Impact:** Blocks all OGC API client discovery, blocks QGIS OAPIF provider, blocks any standards-compliant client

**Reproduction:**
```bash
curl -s -w "\nHTTP: %{http_code}" \
  "https://rmhtitiler-ghcyd7g0bxdvc2hc.eastus-01.azurewebsites.net/vector/"
```
```json
{"code":"NoMatchFound","description":"No route exists for name \"swagger_ui_html\" and params \"\"."}
HTTP: 500
```

Same for `/stac/`:
```bash
curl -s -w "\nHTTP: %{http_code}" \
  "https://rmhtitiler-ghcyd7g0bxdvc2hc.eastus-01.azurewebsites.net/stac/"
```

**Root cause hypothesis:** The TiPG and stac-fastapi sub-applications reference a named route `swagger_ui_html` in their landing page link generation, but the Swagger UI route is either not mounted on the sub-apps (only on the root FastAPI app at `/reference`) or was explicitly disabled. The landing page template tries to resolve this route name via Starlette's `url_path_for()` and throws `NoMatchFound`.

**Suggested fix:**
- Option A: Mount Swagger UI on each sub-app (TiPG at `/vector/docs`, stac-fastapi at `/stac/docs`)
- Option B: Override the landing page route in each sub-app to not reference `swagger_ui_html`
- Option C: Add a named route alias that redirects to `/reference`

---

### BUG-002: bbox filtering returns 500 on all collections

**Severity:** CRITICAL
**Affects:** `GET /vector/collections/{id}/items?bbox=...` — every collection, every geometry type
**Impact:** Blocks spatial queries, map-driven browsing, QGIS viewport filtering

**Reproduction:**
```bash
# Points (ACLED)
curl -s "https://rmhtitiler-ghcyd7g0bxdvc2hc.eastus-01.azurewebsites.net/vector/collections/geo.sg7_csv_test_acled_small_ord1/items?limit=2&bbox=-120,10,-85,35"

# Polygons (cutlines)
curl -s "https://rmhtitiler-ghcyd7g0bxdvc2hc.eastus-01.azurewebsites.net/vector/collections/geo.sg7_vector_test_cutlines_ord1/items?limit=2&bbox=-80,3,-73,7"

# Lines (roads KML)
curl -s "https://rmhtitiler-ghcyd7g0bxdvc2hc.eastus-01.azurewebsites.net/vector/collections/geo.sg7_kml_test_roads_kml_ord1/items?limit=2&bbox=-88.04,15.47,-88.0,15.52"
```

All return:
```json
{
  "code": "AmbiguousFunctionError",
  "description": "function st_transform(unknown, integer) is not unique\nHINT: Could not choose a best candidate function. You might need to add explicit type casts."
}
```

**Root cause hypothesis:** TiPG generates SQL like:
```sql
SELECT ... FROM geo.table WHERE ST_Intersects(geom, ST_Transform(ST_MakeEnvelope(-120, 10, -85, 35, 4326), srid))
```
The `ST_MakeEnvelope()` returns a `geometry` type, but PostgreSQL can't resolve which `ST_Transform` overload to use — likely because there are multiple PostGIS-related extensions installed (e.g., `postgis`, `postgis_raster`, `postgis_topology`) that each register `st_transform` with slightly different signatures.

**Suggested fix:**
- Check for conflicting PostGIS extensions: `SELECT * FROM pg_extension WHERE extname LIKE 'postgis%';`
- Add explicit cast in TiPG's SQL generation: `ST_Transform(ST_MakeEnvelope(...)::geometry, srid)`
- Or set `search_path` to prioritize the correct PostGIS schema
- TiPG GitHub issue tracker may have reports of this with Azure PostgreSQL Flexible Server

---

### BUG-003: Item not found returns 500 instead of 404

**Severity:** MEDIUM
**Affects:** `GET /vector/collections/{id}/items/{itemId}` when item doesn't exist

**Reproduction:**
```bash
curl -s -w "\nHTTP: %{http_code}" \
  "https://rmhtitiler-ghcyd7g0bxdvc2hc.eastus-01.azurewebsites.net/vector/collections/geo.sg7_csv_test_acled_small_ord1/items/999999"
```
```json
{"code":"NotFound","description":"Item 999999 in Collection geo.sg7_csv_test_acled_small_ord1 does not exist."}
HTTP: 500
```

**Expected:** HTTP 404 per OGC API - Features Part 1, Section 7.15.4.
**Note:** The response body correctly identifies the error as "NotFound" — the issue is only the HTTP status code.

---

### BUG-004: Client errors return 500 instead of 4xx

**Severity:** MEDIUM
**Affects:** Multiple endpoints

**Reproduction:**

CQL parse error (should be 400):
```bash
curl -s -w "\nHTTP: %{http_code}" \
  "https://rmhtitiler-ghcyd7g0bxdvc2hc.eastus-01.azurewebsites.net/vector/collections/geo.sg7_csv_test_acled_small_ord1/items?filter=invalid_syntax&filter-lang=cql2-text"
# HTTP: 500 (should be 400)
```

Invalid collection format (returns 422, should be 404):
```bash
curl -s -w "\nHTTP: %{http_code}" \
  "https://rmhtitiler-ghcyd7g0bxdvc2hc.eastus-01.azurewebsites.net/vector/collections/nonexistent/items"
# HTTP: 422 (should be 404)
```

---

### BUG-005: xarray endpoints return empty responses

**Severity:** LOW (may be by design if no data is configured)
**Affects:** All `/xarray/*` endpoints

**Reproduction:**
```bash
# No URL parameter — should return 400 like /cog/ does
curl -s -w "\nHTTP: %{http_code}" \
  "https://rmhtitiler-ghcyd7g0bxdvc2hc.eastus-01.azurewebsites.net/xarray/variables"
# Returns empty body, HTTP 000 (timeout) or 200 with empty body

# With a public Zarr store
curl -s -w "\nHTTP: %{http_code}" \
  "https://rmhtitiler-ghcyd7g0bxdvc2hc.eastus-01.azurewebsites.net/xarray/variables?url=https://ncsa.osn.xsede.org/Pangeo/pangeo-forge/HadISST-feedstock/hadisst.zarr"
# Returns empty body
```

**Expected:** Missing `url` should return 400 with validation error (consistent with `/cog/info` behavior). Invalid/unreachable URLs should return a meaningful error.

---

### BUG-006: `/api` metadata fields are null

**Severity:** LOW

**Reproduction:**
```bash
curl -s "https://rmhtitiler-ghcyd7g0bxdvc2hc.eastus-01.azurewebsites.net/api" | python3 -m json.tool
```
```json
{
  "title": "geotiler",
  "openapi_url": null,
  "docs_url": null
}
```

**Expected:** `openapi_url` should point to `/openapi.json`, `docs_url` to `/reference` (or wherever the interactive docs live).

---

### PERF-001: Intermittent timeouts on data-fetching endpoints

**Severity:** MEDIUM
**Affects:** COG point queries, COG preview, COG bbox image, COG map viewer, vector diagnostics

**Observations:**
- COG tiles at z=8 and z=11 respond within 1-3s
- COG tiles at z=9 and z=10 timeout (>60s)
- COG `/point`, `/preview`, `/bbox` endpoints timeout
- Vector `/diagnostics` endpoint times out
- RAM at 92.6% utilization

**Possible causes:**
- Azure App Service front-end timeout (240s default, but load balancer may be lower)
- Remote COG overview reads at intermediate zoom levels are slow
- Memory pressure causing GC pauses or swap

---

## QGIS Workarounds (Current State)

Until BUG-001 and BUG-002 are fixed, QGIS users can connect using these methods:

### Vector Data via GeoJSON (works now)

**Layer > Add Layer > Add Vector Layer > Protocol: HTTP(S)**

```
URI: https://rmhtitiler-ghcyd7g0bxdvc2hc.eastus-01.azurewebsites.net/vector/collections/geo.sg7_csv_test_acled_small_ord1/items?limit=1000
```

With CQL filter:
```
URI: https://rmhtitiler-ghcyd7g0bxdvc2hc.eastus-01.azurewebsites.net/vector/collections/geo.sg7_csv_test_acled_small_ord1/items?limit=1000&filter=country%3D'Mexico'&filter-lang=cql2-text
```

With field selection:
```
URI: https://rmhtitiler-ghcyd7g0bxdvc2hc.eastus-01.azurewebsites.net/vector/collections/geo.sg7_csv_test_acled_small_ord1/items?limit=1000&properties=country,event_type,fatalities
```

**Limitations:** Loads all features into memory. No map-driven spatial filtering (bbox is broken). Must manually set limit.

### Vector Tiles via MVT (works now)

**Layer > Add Layer > Add Vector Tile Layer > New Generic Connection**

```
Name: rmhtitiler - ACLED Points
URL: https://rmhtitiler-ghcyd7g0bxdvc2hc.eastus-01.azurewebsites.net/vector/collections/geo.sg7_csv_test_acled_small_ord1/tiles/WebMercatorQuad/{z}/{x}/{y}
Min Zoom: 0
Max Zoom: 14
```

**Limitations:** No attribute queries (MVT is for visualization only). No popups/identify without additional config.

### COG Raster — Direct (works now)

**Layer > Add Layer > Add Raster Layer**

```
Source: /vsicurl/https://your-cog-url-here.tif
```

### COG Raster — TiTiler Dynamic Tiles (works now)

**Layer > Add Layer > Add XYZ Tiles > New Connection**

```
Name: rmhtitiler - Sentinel COG
URL: https://rmhtitiler-ghcyd7g0bxdvc2hc.eastus-01.azurewebsites.net/cog/tiles/WebMercatorQuad/{z}/{x}/{y}@1x.png?url=https%3A%2F%2Fsentinel-cogs.s3.us-west-2.amazonaws.com%2Fsentinel-s2-l2a-cogs%2F36%2FN%2FYF%2F2023%2F1%2FS2A_36NYF_20230101_0_L2A%2FTCI.tif
Min Zoom: 8
Max Zoom: 14
```

### What Will Work After Fixes

Once BUG-001 is fixed:
- **Layer > Add Layer > Add WFS / OGC API Features Layer** — enter `https://rmhtitiler-ghcyd7g0bxdvc2hc.eastus-01.azurewebsites.net/vector` and QGIS will auto-discover all 27 collections

Once BUG-002 is fixed:
- Map-driven spatial filtering (pan/zoom queries the visible extent)
- QGIS "restrict to request bounding box" option
- Any web map library's viewport-based feature loading

---

## Available Collections Reference

| Collection | Geometry | Features | Bbox |
|------------|----------|----------|------|
| `geo.sg7_csv_test_acled_small_ord1` | MultiPoint | 5,000 | Global |
| `geo.sg7_vector_test_cutlines_ord1` | MultiPolygon | 1,401 | Colombia |
| `geo.sg7_kml_test_roads_kml_ord1` | Lines | — | Honduras |
| `geo.sg7_shapefile_test_roads_shp_ord1` | Lines | — | Honduras |
| `geo.usa_shit_kml_v9_testing_ord1` | — | — | USA |
| `geo.custom_shit_json_v9_testing_ord1` | — | — | India |
| `geo.shitty_gpkg_v9_testing_ord1` | — | — | Portugal |
| `geo.sg6_hugepoly_test_hugepoly_ord1` | Polygon | — | DC/Maryland |
| `geo.sg6_nullgeom_test_nullgeom_ord1` | Point | — | DC |
| `geo.sg6_mixedgeom_test_*` | Point/Line/Polygon | — | DC |
| + 17 more test collections | Various | — | Various |
