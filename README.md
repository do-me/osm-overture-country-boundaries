# Overture vs OSM Layercake — Country Polygons & Multi-Country Disputed-Areas Mask

Two complementary deliverables for working with country boundaries across data
sources: a row-by-row Overture-vs-OSM territorial divergence file, and a
multi-source mask of areas where multiple country tags can be valid (political
disputes, overseas-territory ISO encoding differences, maritime slivers).

## Outputs

All three live at the repo root and can be opened directly in DuckDB,
GeoPandas, QGIS, or the geoparquet-visualizer (links below).

| File | Size | Rows | What it is |
| --- | ---: | ---: | --- |
| [`country_diff.parquet`](./country_diff.parquet) | 2.5 MB | 142 | Overture (`subtype IN (country, dependency)`, dissolved) vs OSM Layercake `admin_level=2`. One row per connected diverging region with a human-readable explanation column. |
| [`migurski_disputes.parquet`](./migurski_disputes.parquet) | 2.9 MB | 42 | Named geopolitical dispute polygons assembled from [`migurski/boundary-issues`](https://github.com/migurski/boundary-issues) per-country YAML configs (Kashmir sub-features, Crimea/Donbas oblasts, Esequibo, Spratly, West Bank, Golan, Abkhazia/S.Ossetia, W.Sahara, Falklands, Kuril, Liancourt, Halaib, Abyei, Tigri, Kafia Kingi…). |
| [`multi_country_mask.parquet`](./multi_country_mask.parquet) | 5.4 MB | 283 | UNION of `country_diff` + `migurski_disputes` + Natural Earth admin-0 disputed areas. The multi-country-valid mask designed for the Overture Places lat/lon-vs-`country` filter ([Overture Places issue #3692](https://github.com/OvertureMaps/data/issues/3692)). |

Interactive viewers (geoparquet-visualizer):

* [`country_diff.parquet`](https://do-me.github.io/geoparquet-visualizer/?map=1.83%2F52.04509%2F-23.90047%2F0.00%2F0.00&sidebar=1&style=https%3A%2F%2Ftiles.openfreemap.org%2Fstyles%2Fliberty&background=%23f8fafc&spinSpeedX=0.1&spinSpeedY=0&url=https%3A%2F%2Fraw.githubusercontent.com%2Fdo-me%2Fosm-overture-country-boundaries%2Fmain%2Fcountry_diff.parquet)
* [`migurski_disputes.parquet`](https://do-me.github.io/geoparquet-visualizer/?map=1.83%2F52.04509%2F-23.90047%2F0.00%2F0.00&sidebar=1&style=https%3A%2F%2Ftiles.openfreemap.org%2Fstyles%2Fliberty&background=%23f8fafc&spinSpeedX=0.1&spinSpeedY=0&url=https%3A%2F%2Fraw.githubusercontent.com%2Fdo-me%2Fosm-overture-country-boundaries%2Fmain%2Fmigurski_disputes.parquet)
* [`multi_country_mask.parquet`](https://do-me.github.io/geoparquet-visualizer/?map=1.83%2F52.04509%2F-23.90047%2F0.00%2F0.00&sidebar=1&style=https%3A%2F%2Ftiles.openfreemap.org%2Fstyles%2Fliberty&background=%23f8fafc&spinSpeedX=0.1&spinSpeedY=0&url=https%3A%2F%2Fraw.githubusercontent.com%2Fdo-me%2Fosm-overture-country-boundaries%2Fmain%2Fmulti_country_mask.parquet)

## Run

Clone the repo, install [uv](https://docs.astral.sh/uv/), and run either script.

```bash
# Just the Overture-vs-OSM territorial diff -> country_diff.parquet
uv run osm_overture_country_diff.py

# End-to-end disputed-areas pipeline -> all three parquets at repo root
uv run disputed_areas.py
```

Both scripts are PEP-723 inline-dep self-contained and idempotent: every
output and every cache file is skipped when already present (use `--force`
to rebuild). First-run cost is ~5.3 GB of downloads (Overture
`division_area` + OSM Layercake `boundaries.parquet`); subsequent runs are
local-only.

## `country_diff.parquet` — Overture vs OSM Layercake

Comparison of **Overture full territorial extent**
(`subtype IN (country, dependency)`, dissolved by country)
vs **OSM Layercake `admin_level=2` polygons**

* **Overture release:** 2026-04-15
* **Layercake snapshot:** 2026-05-02

### Summary Metrics

| Metric                                                 | Value     |
| ------------------------------------------------------ | --------- |
| ISOs in both                                           | 216       |
| IoU = 1.0 or Δ < 1 km² (operationally identical)       | 148 (76%) |
| IoU ≥ 0.99 (≤300 m buffer difference where meaningful) | 183 (93%) |
| IoU < 0.99 (material divergence)                       | 13 (7%)   |
| Median Hausdorff distance                              | 0 km      |
| Global symmetric difference / union                    | 0.77%     |

### Interpretation

For ~93% of countries, differences between datasets fall within a 300 m buffer—effectively negligible for most spatial filtering workflows.

### Divergent Cases (n = 13)

Differences are driven by **policy choices**, not geometric accuracy:

* **FR / NO / NL**
  OSM aggregates overseas territories (e.g., DOM-TOMs, Svalbard, Caribbean Netherlands) into the parent country.
  Overture represents them as separate ISO entities (e.g., PF, RE, GP, MQ, GF, YT, NC, SJ, CW, BQ, AW, SX).

* **MA**
  Overture includes Western Sahara under Morocco (de facto).
  OSM leaves it unmapped.

* **RU / UA (Crimea)**
  OSM assigns Crimea to Russia.
  Overture assigns it to Ukraine.

* **CN / VN / PH / MY (South China Sea)**
  OSM incorporates maritime claims into the de facto administering state.
  Overture models them as distinct pseudo-ISO regions (e.g., XP, XR).

## `multi_country_mask.parquet` — disputed-areas mask

Built by `disputed_areas.py` as the UNION of three complementary sources.
Each row carries a `source` and a human-readable `description` so provenance
is preserved per polygon.

### Why three sources?

A place's `country` tag may legitimately disagree with the territorial
polygon containing its lat/lon when that lat/lon falls inside any of:

* **(A) a politically disputed area** — Crimea, Western Sahara, Kashmir, Donbas oblasts, …
* **(B) an overseas-territory ISO encoding difference** — FR vs PF/RE/GP/MQ/GF/YT/NC, NO vs SJ, NL vs CW/BQ/AW/SX, US vs PR/VI/…
* **(C) a maritime/coastal sliver** — one dataset including territorial waters another excludes

No single source covers all three. Combining them produces the smallest
mask that still keeps every legitimate place tagged the way it is.

| source | rows | dissolved area | unique vs union of the other two |
| --- | ---: | ---: | ---: |
| `overture_osm_diff` (Overture vs OSM Layercake — categories A∪B∪C) | 142 | 1,170,765 km² | **924,563 km² (79%)** |
| `natural_earth_disputed` (NE admin-0 disputed areas — symmetric disputes both datasets agree on) | 99 | 1,155,080 km² | **327,461 km² (28%)** |
| `migurski_disputes` (curated OSM-relation polygons from migurski/boundary-issues YAML configs) | 42 | 1,141,714 km² | **200,841 km² (18%)** |
| **UNION (after dissolve)** | | **2,394,272 km²** | |

### Filter rule (sketch, per Overture Places issue #3692)

```
if Overture[place.country] contains place.lat_lon                → keep
elif place.lat_lon ∈ multi_country_mask                          → keep
elif place.lat_lon ∈ water_mask                                  → drop
elif place.country IS NULL AND place.lat_lon ∈ land              → keep
else                                                              → drop
```

## `migurski_disputes.parquet` — named geopolitical disputes

Per-feature dispute polygons assembled from
[`migurski/boundary-issues`](https://github.com/migurski/boundary-issues)
YAML configs. Every OSM relation referenced under a country `perspectives:`
block (where that country's `base:` does **not** contain it) becomes one
row. Whole-country relations re-used as set-op operands (e.g. France's
`perspectives.FRA: [+France]` re-add after Mont Blanc subtraction) are
filtered out via a 500 KB size cap on the OSM XML snapshot.

Provenance fields:

* `relation_id` — OSM relation ID (clickable on osm.org)
* `label` — human-readable dispute name from the YAML inline comments
* `country_blocks` — semicolon-list of country perspectives that reference this dispute
* `all_comments` — pipe-separated YAML inline comments naming the dispute

## Reproducibility

Both scripts use [PEP 723 inline metadata](https://peps.python.org/pep-0723/):
`uv run` resolves the deps automatically into a per-script venv. No
`requirements.txt` or `pyproject.toml` required.

* `osm_overture_country_diff.py` — deps: `duckdb`, `requests`. Single DuckDB pipeline.
* `disputed_areas.py` — deps: `duckdb`, `requests`, `osmium` (for OSM-XML multipolygon assembly), `shapely`, `pyarrow`.

Caches go into `data/` (gitignored). The largest cache is the Overture
`division_area` parquet at ~5.25 GB. Re-running with `data/` populated
finishes in seconds.

Pipeline stages (`disputed_areas.py`):

1. Overture `division_area` parquet for the release (default `2026-04-15.0`)
2. OSM US Layercake `boundaries.parquet`
3. `migurski/boundary-issues` `config-*.yaml`
4. Discover dispute relation IDs from those configs
5. `migurski/boundary-issues` OSM-relation snapshots (`data/sources/relation/{id}.osm.xml.gz`)
6. Natural Earth admin-0 disputed areas GeoJSON
7. DuckDB pipeline → `country_diff.parquet`
8. pyosmium polygon assembly → `migurski_disputes.parquet`
9. DuckDB UNION → `multi_country_mask.parquet`

<img width="3840" height="2216" alt="image" src="https://github.com/user-attachments/assets/10bb0629-be4a-4024-a70c-750b1cb10eba" />
