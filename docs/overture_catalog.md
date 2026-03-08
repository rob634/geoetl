# Overture Maps Data Catalog

Reference for configuring GeoETL pipelines against Overture Maps S3 GeoParquet.

**Current release:** `2026-02-18.0` (schema v1.16.0)
**S3 bucket:** `overturemaps-us-west-2` (region `us-west-2`, no auth required)
**Format:** GeoParquet (WKB geometry, sorted by geohash-15)
**Release cadence:** Monthly, typically mid-month

## S3 Access

**Path template:**
```
s3://overturemaps-us-west-2/release/{release}/theme={theme}/type={type}/*
```

**DuckDB setup:**
```sql
INSTALL httpfs; LOAD httpfs;
INSTALL spatial; LOAD spatial;
INSTALL h3 FROM community; LOAD h3;
SET s3_region = 'us-west-2';
```

**Predicate pushdown** works on the `bbox` struct column — always include a bbox filter:
```sql
WHERE bbox.xmin > -1 AND bbox.xmax < 1 AND bbox.ymin > 51 AND bbox.ymax < 53
```

**Centroid-in-tile filter** (prevents double-counting at tile boundaries):
```sql
AND ST_X(ST_Centroid(geometry)) BETWEEN west AND east
AND ST_Y(ST_Centroid(geometry)) BETWEEN south AND north
```

## Common Columns (all types)

| Column | Type | Notes |
|--------|------|-------|
| `id` | string | GERS stable identifier |
| `geometry` | WKB binary | Point, LineString, Polygon, or Multi* |
| `bbox` | struct{xmin, xmax, ymin, ymax} | Enables predicate pushdown |
| `version` | int32 | Feature version number |
| `sources` | list\<struct\> | `{dataset, record_id, update_time, confidence}` |

---

## Theme: transportation

### type: segment (~342M features)

**Geometry:** LineString
**Path:** `theme=transportation/type=segment/*`

| Column | Type | Notes |
|--------|------|-------|
| `subtype` | string | `road`, `rail`, `water` |
| `class` | string | See enums below |
| `subclass` | string | `link`, `sidewalk`, `crosswalk`, `parking_aisle`, `driveway`, `alley`, `cycle_crossing` |
| `names` | struct | `{primary, common, rules}` |
| `connectors` | list\<struct\> | Min 2 items. `{connector_id: string, at: float}` — `at` is 0.0–1.0 linear ref |
| `access_restrictions` | list\<struct\> | Access rules with mode/direction scoping |
| `speed_limits` | list\<struct\> | Speed limits with units |
| `road_surface` | list\<struct\> | Surface material |
| `road_flags` | list\<string\> | See flags below |
| `rail_flags` | list\<string\> | See flags below |
| `routes` | list\<struct\> | Route references (highway numbers, etc.) |
| `prohibited_transitions` | list\<struct\> | Turn restrictions |
| `level_rules` | list\<struct\> | Z-level at bridges/tunnels |
| `width_rules` | list\<struct\> | Road width |
| `destinations` | list\<struct\> | Destination signs |

**Road class values (17):**
`motorway`, `trunk`, `primary`, `secondary`, `tertiary`, `residential`, `living_street`, `unclassified`, `service`, `pedestrian`, `footway`, `steps`, `path`, `track`, `cycleway`, `bridleway`, `unknown`

**Rail class values (8):**
`funicular`, `light_rail`, `monorail`, `narrow_gauge`, `standard_gauge`, `subway`, `tram`, `unknown`

**Road flags:** `is_bridge`, `is_link`, `is_tunnel`, `is_under_construction`, `is_abandoned`, `is_covered`, `is_indoor`

**Rail flags:** `is_bridge`, `is_tunnel`, `is_under_construction`, `is_abandoned`, `is_covered`, `is_passenger`, `is_freight`, `is_disused`

**Road surface:** `unknown`, `paved`, `unpaved`, `gravel`, `dirt`, `paving_stones`, `metal`

**Travel modes** (access_restrictions/speed_limits): `vehicle`, `motor_vehicle`, `car`, `truck`, `motorcycle`, `foot`, `bicycle`, `bus`, `hgv`, `hov`, `emergency`

### type: connector (~406M features)

**Geometry:** Point
**Path:** `theme=transportation/type=connector/*`

No additional columns beyond common fields. Connectors are topology nodes — routing decision points where segments meet. Each connector is referenced by the `connectors` array in segments.

---

## Theme: buildings

### type: building

**Geometry:** Polygon, MultiPolygon
**Path:** `theme=buildings/type=building/*`

| Column | Type | Notes |
|--------|------|-------|
| `subtype` | string | See enum below |
| `class` | string | 86 values, see enum below |
| `names` | struct | |
| `height` | float64 | Meters, ground to highest point |
| `min_height` | float64 | Meters, base altitude above ground |
| `roof_height` | float64 | Meters, roof base to peak |
| `num_floors` | int32 | |
| `num_floors_underground` | int32 | |
| `min_floor` | int32 | |
| `is_underground` | boolean | |
| `has_parts` | boolean | Whether building_part features exist |
| `level` | int | |
| `facade_color` | string | Hex color |
| `facade_material` | string | |
| `roof_material` | string | |
| `roof_shape` | string | |
| `roof_direction` | float64 | |
| `roof_orientation` | string | |
| `roof_color` | string | Hex color |

**Subtype values (13):**
`agricultural`, `civic`, `commercial`, `education`, `entertainment`, `industrial`, `medical`, `military`, `outbuilding`, `religious`, `residential`, `service`, `transportation`

**Class values (86):**
`agricultural`, `allotment_house`, `apartments`, `barn`, `beach_hut`, `boathouse`, `bridge_structure`, `bungalow`, `bunker`, `cabin`, `carport`, `cathedral`, `chapel`, `church`, `civic`, `college`, `commercial`, `cowshed`, `detached`, `digester`, `dormitory`, `dwelling_house`, `factory`, `farm`, `farm_auxiliary`, `fire_station`, `garage`, `garages`, `ger`, `glasshouse`, `government`, `grandstand`, `greenhouse`, `guardhouse`, `hangar`, `hospital`, `hotel`, `house`, `houseboat`, `hut`, `industrial`, `kindergarten`, `kiosk`, `library`, `manufacture`, `military`, `monastery`, `mosque`, `office`, `outbuilding`, `parking`, `pavilion`, `post_office`, `presbytery`, `public`, `religious`, `residential`, `retail`, `roof`, `school`, `semi`, `semidetached_house`, `service`, `shed`, `shrine`, `silo`, `slurry_tank`, `sports_centre`, `sports_hall`, `stable`, `stadium`, `static_caravan`, `stilt_house`, `storage_tank`, `sty`, `supermarket`, `synagogue`, `temple`, `terrace`, `toilets`, `train_station`, `transformer_tower`, `transportation`, `trullo`, `university`, `warehouse`, `wayside_shrine`

### type: building_part

**Geometry:** Polygon, MultiPolygon
**Path:** `theme=buildings/type=building_part/*`

Same schema as `building`. Represents 3D sub-volumes of a parent building.

---

## Theme: places

### type: place

**Geometry:** Point
**Path:** `theme=places/type=place/*`

| Column | Type | Notes |
|--------|------|-------|
| `names` | struct | `{primary, common, rules}` |
| `categories` | struct | **DEPRECATED June 2026.** `{primary: string, alternate: list<string>}` |
| `basic_category` | string | ~280 values, replaces categories.primary |
| `taxonomy` | struct | `{primary: string, hierarchy: list<string>, alternate: list<string>}` |
| `confidence` | float64 | 0–1 existence confidence |
| `operating_status` | string | Current operational state |
| `websites` | list\<string\> | |
| `socials` | list\<string\> | Social media URLs |
| `emails` | list\<string\> | |
| `phones` | list\<string\> | |
| `brand` | struct | Brand info |
| `addresses` | list\<struct\> | Address components |

**Taxonomy:** The `hierarchy` array is ordered general-to-specific. ~2,100 total category values across 13 top-level categories.

> **Migration note:** `categories` is removed June 2026. Use `basic_category` + `taxonomy` instead.

---

## Theme: addresses

### type: address

**Geometry:** Point
**Path:** `theme=addresses/type=address/*`

| Column | Type | Notes |
|--------|------|-------|
| `country` | string | ISO 3166-1 alpha-2 (required) |
| `number` | string | House/building number |
| `street` | string | Street name |
| `unit` | string | Apartment/suite/floor |
| `postcode` | string | Postal code |
| `postal_city` | string | Alternate city name for mailing |
| `address_levels` | list\<struct\> | 1–5 items, hierarchical admin levels |

---

## Theme: divisions

### type: division

**Geometry:** Point
**Path:** `theme=divisions/type=division/*`

| Column | Type | Notes |
|--------|------|-------|
| `subtype` | string | `country`, `dependency`, `region`, `macroregion`, `county`, `macrocounty`, `locality` |
| `class` | string | Further classification |
| `country` | string | ISO 3166-1 alpha-2 |
| `region` | string | ISO 3166-2 |
| `admin_level` | int | Hierarchical position (new in v1.16.0) |
| `parent_division_id` | string | Required except for countries |
| `names` | struct | |
| `local_type` | struct | Localized subtype names |
| `population` | int32 | |
| `capital_division_ids` | list\<string\> | |
| `hierarchies` | list\<list\<struct\>\> | Multiple perspective chains |
| `perspectives` | struct | Disputed territory handling |
| `norms` | struct | Local rules (driving side, etc.) |
| `wikidata` | string | Wikidata ID |

### type: division_area

**Geometry:** Polygon, MultiPolygon
**Path:** `theme=divisions/type=division_area/*`

References a `division` by ID, providing the area polygon.

### type: division_boundary

**Geometry:** LineString
**Path:** `theme=divisions/type=division_boundary/*`

Shared borders between two divisions.

---

## Theme: base

### type: land

**Geometry:** Polygon, MultiPolygon
**Path:** `theme=base/type=land/*`

**Subtypes (13):** `crater`, `desert`, `forest`, `glacier`, `grass`, `land`, `physical`, `reef`, `rock`, `sand`, `shrub`, `tree`, `wetland`

**Classes (41):** `archipelago`, `bare_rock`, `beach`, `cave_entrance`, `cliff`, `desert`, `dune`, `fell`, `forest`, `glacier`, `grass`, `grassland`, `heath`, `hill`, `island`, `islet`, `land`, `meadow`, `meteor_crater`, `mountain_range`, `peak`, `peninsula`, `plateau`, `reef`, `ridge`, `rock`, `saddle`, `sand`, `scree`, `scrub`, `shingle`, `shrub`, `shrubbery`, `stone`, `tree`, `tree_row`, `tundra`, `valley`, `volcanic_caldera_rim`, `volcano`, `wetland`, `wood`

### type: land_use

**Geometry:** Polygon, MultiPolygon
**Path:** `theme=base/type=land_use/*`

**Subtypes (24):** `agriculture`, `aquaculture`, `campground`, `cemetery`, `construction`, `developed`, `education`, `entertainment`, `golf`, `grass`, `horticulture`, `landfill`, `managed`, `medical`, `military`, `park`, `pedestrian`, `protected`, `recreation`, `religious`, `residential`, `resource_extraction`, `transportation`, `winter_sports`

**Classes:** 96+ values (facility-specific, from `airfield` to `zoo`)

### type: land_cover

**Geometry:** Polygon
**Path:** `theme=base/type=land_cover/*`

**Subtypes (10):** `barren`, `crop`, `forest`, `grass`, `mangrove`, `moss`, `shrub`, `snow`, `urban`, `wetland`

No class values. Sourced from ESA WorldCover.

### type: water

**Geometry:** Polygon, MultiPolygon, LineString
**Path:** `theme=base/type=water/*`

**Subtypes (12):** `canal`, `human_made`, `lake`, `ocean`, `physical`, `pond`, `reservoir`, `river`, `spring`, `stream`, `wastewater`, `water`

**Classes (28+):** `basin`, `bay`, `blowhole`, `canal`, `cape`, `ditch`, `dock`, `drain`, `fairway`, `fish_pass`, `fishpond`, `geyser`, `hot_spring`, `lagoon`, `lake`, `moat`, `ocean`, `oxbow`, `pond`, `reflecting_pool`, `reservoir`, `river`, `salt_pond`, `sea`, `sewage`, `shoal`, `spring`, `strait`, `stream`, `swimming_pool`, `tidal_channel`, `wastewater`, `water`, `water_storage`, `waterfall`

### type: infrastructure

**Geometry:** Point, LineString, Polygon
**Path:** `theme=base/type=infrastructure/*`

**Subtypes (18):** `aerialway`, `airport`, `barrier`, `bridge`, `communication`, `emergency`, `manhole`, `pedestrian`, `pier`, `power`, `quay`, `recreation`, `tower`, `transit`, `transportation`, `utility`, `waste_management`, `water`

**Classes:** 170+ values (very extensive, from `aerialway_station` to `zip_line`)

Additional columns: `height` (float64), `surface` (string), `source_tags` (map), `wikidata` (string)

### type: bathymetry

**Geometry:** Polygon
**Path:** `theme=base/type=bathymetry/*`

Depth contours of underwater areas.

---

## GeoETL Config Gaps

Comparing the full catalog against `geoetl/overture/config.py`:

| Item | Current | Full catalog | Action needed |
|------|---------|-------------|---------------|
| Road classes | 14 | 17 | Missing `pedestrian`, `bridleway`, `unknown` |
| Building subtypes | 3 tracked | 13 available | Intentional (only aggregate residential/commercial/industrial) |
| OvertureTheme enum | 3 themes | 6 themes | Missing `addresses`, `base`, `divisions` |
| Places categories | uses `categories` | deprecated June 2026 | Migrate to `basic_category` + `taxonomy` |

## Pipeline Configuration Reference

### Tile grid

Our pipeline uses 5° tiles covering -180°–180° lon, -60°–75° lat = **1,944 tiles** (1,020 non-empty).

### DuckDB memory budget

```
per_worker_gb = 48 // num_workers  (min 4GB)
```

| Workers | Memory/worker | Total | Throughput |
|---------|--------------|-------|------------|
| 4 | 12 GB | 48 GB | ~16 tiles/min |
| 3 | 16 GB | 48 GB | ~12 tiles/min |
| 2 | 24 GB | 48 GB | ~8 tiles/min |

### Performance characteristics

- Bottleneck is S3 query latency, not local compute
- Empty/ocean tiles: ~1–2s query, 0 bytes written
- Typical non-empty tile: 5–50s query
- Dense urban tiles (France, Belgium, NYC): 100–200s query, 1–2 GB parquet
- Write time is negligible (~1% of query time)

---

*Sources: [Overture Maps Schema Reference](https://docs.overturemaps.org/schema/reference/), [DuckDB Guide](https://docs.overturemaps.org/getting-data/duckdb/), [Release Notes](https://docs.overturemaps.org/blog/2026/02/18/release-notes/), direct S3 introspection via DuckDB*
