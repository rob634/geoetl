# MapSPAM to H3 Aggregation Methodology

## Purpose

This document defines the statistical methodology for aggregating MapSPAM 2020 gridded crop production data onto Uber's H3 hexagonal grid system. It covers variable classification, correct aggregation rules, resolution constraints, and the reasoning behind each decision.

The goal is to avoid statistical sin — specifically, treating intensive variables as extensive, fabricating sub-pixel spatial information, and conflating visualization with analysis.

---

## MapSPAM Data Structure

### Source

MapSPAM 2020 v2r0 is produced by IFPRI (International Food Policy Research Institute) using a **cross-entropy optimization model** that allocates national and subnational crop statistics to a 5 arc-minute global grid.

The model integrates:
- National/subnational crop production statistics (FAO, national statistical offices)
- Satellite-derived cropland extent
- Irrigation maps
- Crop suitability models
- Population density and market accessibility
- Prior crop presence estimates

The cross-entropy approach starts with informed prior estimates of physical cropping area per pixel, then updates them subject to constraints (allocated area cannot exceed suitable area, pixel totals must sum to reported subnational statistics).

### Resolution

**5 arc-minutes** (~10 km at equator). This is the fundamental resolution limit of the data.

Pixel area varies with latitude because the data is in geographic coordinates (WGS84):

| Latitude | Pixel width | Pixel height | Pixel area |
|----------|-------------|--------------|------------|
| 0° (equator) | 9.3 km | 9.3 km | 86 km² |
| 15° | 9.0 km | 9.3 km | 83 km² |
| 30° | 8.0 km | 9.3 km | 75 km² |
| 45° | 6.6 km | 9.3 km | 61 km² |
| 60° | 4.6 km | 9.3 km | 43 km² |

Formula: `pixel_area = (5/60 × 111.32)² × cos(latitude)` km²

### Output Variables

MapSPAM produces four variables per crop per production system:

| Variable | Code | Unit | Type | Pixel value represents |
|----------|------|------|------|----------------------|
| Production | P | metric tons | **Extensive** | Total MT produced in this pixel |
| Harvested Area | H | hectares | **Extensive** | Total ha harvested in this pixel |
| Physical Area | A | hectares | **Extensive** | Total ha of cropland in this pixel |
| Yield | Y | kg/ha | **Intensive** | Y = P / H (a derived ratio) |

### Production Systems

| Code | Description |
|------|-------------|
| A | All technologies combined |
| I | Irrigated |
| R | Rainfed (high-input + low-input + subsistence combined in 2020) |

In SPAM 2010, rainfed was split into three sub-categories (H=high-input, L=low-input, S=subsistence). SPAM 2020 aggregates these into a single rainfed category.

### Key Constraints in the Model

1. **Physical area constraint**: The sum of physical areas of all crops in a pixel cannot exceed the actual cropland area in that pixel.
2. **Harvested area ≥ Physical area**: Harvested area accounts for multiple harvests from the same land (cropping intensity > 1).
3. **National consistency**: Pixel totals are scaled to match FAO national averages (2017-2021 for SPAM 2020).
4. **Suitability constraint**: Allocated area per crop per system cannot exceed suitable area for that crop in that pixel.

### What the Pixel Is and Is Not

**The pixel is the atom of this dataset.** Each pixel value is a single aggregate estimate for the entire ~86 km² cell. The cross-entropy model tells us "we estimate X metric tons were produced somewhere in this cell."

There is **zero information about the spatial distribution within the cell**. The production could be concentrated in one corner, spread uniformly, or follow an unknown pattern driven by actual field locations. The model does not resolve this.

---

## Extensive vs. Intensive Variables

This distinction is the most important concept for correct aggregation.

### Extensive Variables (Production, Area)

Extensive variables **scale with the size of the region being measured**. If you combine two adjacent pixels, the total production is the sum of both pixels' production. Summing is the correct aggregation.

- Production: 1000 MT in pixel A + 2000 MT in pixel B = 3000 MT in the combined region
- Harvested Area: 500 ha + 800 ha = 1300 ha

### Intensive Variables (Yield)

Intensive variables are **rates or densities that do not scale with area**. Yield (kg/ha) is production per unit of harvested area. You cannot sum yields. You cannot simple-average yields (unless all pixels have identical harvested area, which they never do).

**Correct aggregation for yield**: recompute from the summed extensive variables.

```
hex_yield = sum(pixel_production) / sum(pixel_harvested_area)
```

This is mathematically equivalent to an area-weighted average of pixel yields:

```
hex_yield = Σ(pixel_yield × pixel_harvested_area) / Σ(pixel_harvested_area)
```

### The Statistical Sin

**Never aggregate yield rasters directly.** Taking the arithmetic mean of pixel yields weights every pixel equally regardless of how much production actually occurs there. A pixel with 0.1 ha and 10,000 kg/ha yield gets the same weight as a pixel with 5,000 ha and 3,000 kg/ha yield.

| Method | Formula | Result | Correct? |
|--------|---------|--------|----------|
| Sum yield pixels | Σ Y | Meaningless number | No |
| Mean yield pixels | mean(Y) | Unweighted average, biased toward low-area pixels | No |
| Derive from P/H | Σ P / Σ H | Area-weighted yield | **Yes** |

---

## H3 Resolution and Statistical Honesty

### Pixels Per Hex at Different Resolutions

H3 hexagon areas also vary with latitude (pentagons aside), but the variation is less extreme than for geographic grid cells:

| H3 Level | Hex area (km²) | Pixels per hex (equator) | Pixels per hex (45°) |
|----------|---------------|-------------------------|---------------------|
| L5 | 188–253 | ~2.2 | ~4.0 |
| L4 | 1,318–1,770 | ~15 | ~29 |
| L3 | 9,228–12,393 | ~107 | ~203 |

### Why L5 is Not Statistically Honest for Analysis

At H3 Level 5, each hexagon contains approximately **2–5 MapSPAM pixels**. This means:

1. **Boundary assignment dominates the result.** Whether a pixel's centroid falls inside hex A or hex B is a geometric accident. Moving one pixel between adjacent hexes changes the hex value by 25–50%.

2. **You are not aggregating — you are repackaging.** An L5 hex containing 3 pixels is not a meaningful spatial aggregate; it's approximately the same information as the source pixels, reshuffled into hexagonal containers.

3. **The hex values cannot be interpreted as independent spatial units.** Adjacent L5 hexes that share source pixels will have correlated errors. The apparent spatial resolution suggests precision that does not exist in the underlying data.

### L5: Hexagon-Shaped Pixels (Visualization Tier)

L5 is useful for **visualization only**. It resamples the rectangular 5-arcminute grid into hexagons, which:

- Render beautifully on 3D globes and deck.gl maps
- Provide uniform visual weight (unlike lat/lon cells that stretch at high latitudes)
- Enable H3-native spatial indexing, filtering, and join operations
- Support multi-resolution tiling (L5 detail → L4 overview → L3 regional)

The data at L5 should be treated as a **visual resampling**, not a spatial analysis. It's comparable to reprojecting a raster for display — the visual is useful, but you wouldn't run statistics on the reprojected pixel values without understanding the resampling artifacts.

### L4: Analysis Tier

At H3 Level 4, each hexagon contains **15–35 source pixels**. This is enough that:

- Boundary assignment noise is ~5% of the signal (a few pixels landing in the wrong hex doesn't matter much)
- Summed extensive variables are meaningful spatial aggregates
- Derived yield (P/H) represents a genuine area-weighted average across a region
- Adjacent hexes have minimal error correlation from shared boundary pixels

**L4 is the minimum resolution for honest statistical analysis with MapSPAM data.**

### L3: Regional Analysis Tier

At H3 Level 3, hexagons contain **100+ source pixels**. Boundary effects are negligible. This is the appropriate scale for:

- Cross-regional comparisons
- Correlation with other datasets (climate, demographics)
- Time-series analysis (comparing SPAM 2010 vs 2020)
- Input to ML models where spatial precision is less important than signal quality

### Resolution Tier Summary

| Tier | H3 Level | Purpose | Yield aggregation | Appropriate uses |
|------|----------|---------|-------------------|------------------|
| **Visualization** | L5 | Hexagon-shaped pixels | Derive P/H per hex | Globe rendering, interactive maps, visual exploration |
| **Analysis** | L4 | Honest spatial aggregate | Derive P/H per hex | Spatial statistics, bivariate analysis, policy dashboards |
| **Regional** | L3 | Stable regional aggregate | Derive P/H per hex | Cross-regional comparison, ML features, time-series |

---

## Aggregation Implementation

### Method: Centroid-in-Polygon Assignment

We use `rasterio.mask` with `all_touched=False`, which assigns each pixel to the hexagon containing the pixel's centroid. This is a binary assignment — each pixel belongs to exactly one hex.

**Why not area-weighted fractional assignment?** Because the pixel is indivisible. We have no information about the sub-pixel distribution of production, so assigning 40% of a pixel's production to hex A and 60% to hex B based on geometric overlap implies a uniform distribution within the pixel — an assumption with no basis in the data.

Centroid assignment and uniform-distribution fractional assignment are **equally wrong** in different ways:

- **Centroid assignment**: "this pixel's production belongs to whichever hex the center falls in" — arbitrary but makes no sub-pixel distribution assumptions
- **Fractional assignment**: "this pixel's production is evenly spread across its area" — sounds more sophisticated but fabricates spatial information

Neither is more correct than the other. Centroid assignment is simpler, faster, and equally valid. The errors it introduces are unbiased (they cancel out in expectation over many hexes) and become negligible at L4 resolution where hexes contain 15+ pixels.

### Aggregation Rules by Variable

```
# For each H3 hexagon:
hex_production   = sum(pixel_production)      # Extensive: sum
hex_harv_area    = sum(pixel_harvested_area)   # Extensive: sum
hex_phys_area    = sum(pixel_physical_area)    # Extensive: sum
hex_yield        = hex_production / hex_harv_area  # Intensive: derive
```

### Level Rollup Rules (L5 → L4 → L3)

When rolling up from L5 to L4 or L3:

```
# Extensive variables: sum child hexes
l4_production = sum(l5_production for children)
l4_harv_area  = sum(l5_harv_area for children)

# Intensive variables: recompute from rolled-up extensive variables
l4_yield = l4_production / l4_harv_area

# NEVER: l4_yield = mean(l5_yield for children)
```

This ensures yield is always area-weighted, regardless of how many levels of rollup are applied.

### Multi-Raster Pipeline

For MapSPAM, the practical pipeline is:

1. **Aggregate P (production) raster** → sum pixels per hex → `{crop}_{tech}_production_mt`
2. **Aggregate H (harvested area) raster** → sum pixels per hex → `{crop}_{tech}_harv_area_ha`
3. **Aggregate A (physical area) raster** → sum pixels per hex → `{crop}_{tech}_phys_area_ha`
4. **Derive yield** from step 1 and 2 → `{crop}_{tech}_yield_kgha = production / harv_area * 1000`
5. **Do not aggregate Y (yield) rasters.** They exist for convenience in the source data but must not be fed through zonal statistics.

---

## Validation

### Conservation Check

For extensive variables, the sum across all hexagons should equal the sum across all source pixels (minus any pixels that fall in ocean/no-hex areas):

```
assert abs(sum(hex_production) - sum(pixel_production)) / sum(pixel_production) < 0.01
```

A ratio near 1.0 confirms no double-counting or loss.

### National Consistency Check

MapSPAM pixel values are scaled to match FAO national statistics. After aggregation to H3, summing all hexes within a country should approximately reproduce the FAO national total. Deviations indicate boundary assignment losses (pixels near country borders assigned to the wrong hex).

### Yield Sanity Check

Derived hex yield should fall within the range of source pixel yields for that hex:

```
min(pixel_yields) <= hex_yield <= max(pixel_yields)
```

If hex yield falls outside pixel yield range, it indicates an error in the derivation (likely using yield rasters directly instead of deriving from P/H).

---

## Sources

### MapSPAM Methodology
- [MapSPAM Methodology](https://www.mapspam.info/methodology/) — Official methodology overview
- [MapSPAM Data Center](https://www.mapspam.info/data/) — Data downloads and variable descriptions
- [MapSPAM FAQ](https://www.mapspam.info/faq/) — Variable definitions, units, constraints

### Peer-Reviewed Papers
- [Yu, Q. et al. (2020). "A cultivated planet in 2010 — Part 2: The global gridded agricultural-production maps." *Earth System Science Data*, 12, 3545–3572.](https://essd.copernicus.org/articles/12/3545/2020/) — SPAM 2010 methodology paper, applicable to SPAM 2020
- ["; et al. (2019). "Pixelating crop production: Consequences of methodological choices." *PLOS ONE*, 14(2), e0212281.](https://pmc.ncbi.nlm.nih.gov/articles/PMC6380596/) — Sensitivity analysis of cross-entropy allocation, shows dependence on subnational statistics granularity

### Technical References
- [WRI MAPSPAM GitHub](https://github.com/wri/MAPSPAM) — Data processing reference (nodata = -1, float32)
- [mapspamc R package](https://michielvandijk.github.io/mapspamc/) — Country-level downscaling implementation
- [Actual pixel sizes of unprojected raster maps](https://modtools.wordpress.com/2024/01/23/actual-pixel-sizes-of-unprojected-raster-maps/) — Pixel area latitude variation
- [H3 Documentation](https://h3geo.org/) — Hexagonal grid system reference

### MapSPAM Data Citation
> International Food Policy Research Institute (IFPRI), 2024. *Spatial Production Allocation Model (SPAM) 2020 Version 2 Release 0 (SPAM 2020 V2r0)*. Harvard Dataverse. doi:10.7910/DVN/PRFF8V

---

## Appendix: Pixel Area and H3 Hex Area Comparison

Computed using `h3.cell_area(unit='km^2')` and `(5/60 × 111.32)² × cos(lat)`:

```
Latitude  | MapSPAM pixel | H3 L5 hex | H3 L4 hex  | Pixels/L5 | Pixels/L4
----------|---------------|-----------|------------|-----------|----------
 0°       |  86 km²       | 188 km²   | 1,318 km²  |  2.2      |  15
15°       |  83 km²       | 227 km²   | 1,590 km²  |  2.7      |  19
30°       |  75 km²       | 253 km²   | 1,770 km²  |  3.4      |  24
45°       |  61 km²       | 241 km²   | 1,690 km²  |  4.0      |  28
60°       |  43 km²       | 195 km²   | 1,362 km²  |  4.5      |  32
```

H3 L5 hexes are 2–5× the source pixel area — essentially the same order of magnitude.
H3 L4 hexes are 15–35× the source pixel area — a genuine spatial aggregate.
