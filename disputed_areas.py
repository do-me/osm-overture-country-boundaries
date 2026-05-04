#!/usr/bin/env python3
# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "duckdb>=1.4",
#     "requests>=2.32",
#     "osmium>=4",
#     "shapely>=2.0",
#     "pyarrow>=15",
# ]
# ///
"""End-to-end disputed-areas pipeline producing ./multi_country_mask.parquet
(plus the two intermediates country_diff.parquet + migurski_disputes.parquet).

This script is the all-in-one orchestrator. For the focused Overture-vs-OSM
territorial diff alone, use `osm_overture_country_diff.py`. Both scripts can
build country_diff.parquet; this one then layers migurski + Natural Earth on top.

The headline output is the multi-country-valid mask for the Overture Places
lat/lon-vs-country filter (issue #3692). It unions three complementary sources:

  A. Overture-vs-OSM territorial divergence (overture_osm_diff)
       Picks up category-A political disputes only one dataset acknowledges,
       category-B overseas-territory ISO encoding differences (FR vs PF/RE/GP/
       MQ/GF/YT/NC, NO vs SJ, NL vs CW/BQ/AW/SX, US vs PR/VI/…), and
       category-C maritime/coastal slivers.
  B. migurski/boundary-issues disputes (migurski_disputes)
       42 named OSM-relation polygons extracted from the per-country YAML
       perspectives in https://github.com/migurski/boundary-issues. Provides
       expert-curated dispute geometries with country-block provenance
       (Kashmir sub-features, Crimea/Donbas oblasts, Esequibo, Spratly, West
       Bank, Golan, Abkhazia/S.Ossetia, W.Sahara, Falklands, Kuril, Liancourt,
       Halaib, Abyei, Tigri, Kafia Kingi, …).
  C. Natural Earth admin-0 disputed areas (natural_earth_disputed)
       Curated set including disputes BOTH Overture and OSM agree on
       (Kashmir, Antarctic claims, Falklands, Cyprus, Somaliland, …) which
       therefore aren't in the diff.

Stages — all idempotent; existing outputs are skipped unless --force.

  Network (sizes shown only for missing files):
    1. Overture division_area parquet for --release       (~5.25 GB)
    2. OSM US Layercake boundaries.parquet                (~70 MB)
    3. migurski/boundary-issues config-*.yaml             (24 files, ~54 KB)
    4. migurski OSM-relation XML snapshots                (~42 files, ~3.7 MB)
    5. Natural Earth admin-0 disputed areas GeoJSON       (~685 KB)

  Compute:
    6. ./country_diff.parquet
            DuckDB Overture-vs-OSM territorial divergence with per-row
            counterpart attribution and human-readable explanations.
    7. ./migurski_disputes.parquet
            42 named dispute polygons assembled from OSM relation XML via
            pyosmium. Country-baseline relations (whole-country relations
            re-used as set-op operands in someone else's perspective) are
            filtered out via a 500 KB size cap.
    8. ./multi_country_mask.parquet  (final)
            UNION of all three sources with provenance preserved per row.

Usage:
    uv run disputed_areas.py                           # build all (skip cached)
    uv run disputed_areas.py --force                   # rebuild from scratch
    uv run disputed_areas.py --no-mask                 # stop after country_diff
    uv run disputed_areas.py --release 2026-04-15.0
"""
from __future__ import annotations

import argparse
import gzip
import json
import re
import sys
import urllib.parse
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import duckdb
import osmium
import pyarrow as pa
import requests
from shapely.geometry import shape


# =============================================================================
# Constants
# =============================================================================

OVERTURE_BUCKET = "overturemaps-us-west-2"
OVERTURE_REGION = "us-west-2"
DEFAULT_RELEASE = "2026-04-15.0"

OSM_LAYERCAKE_URL = "https://data.openstreetmap.us/layercake/boundaries.parquet"

MIGURSKI_REPO = "migurski/boundary-issues"
MIGURSKI_RAW = f"https://raw.githubusercontent.com/{MIGURSKI_REPO}/main"

# Pinning to a release tag (e.g. v5.1.2) would be more reproducible. Bump if
# you need deterministic builds.
NE_DISPUTED_URL = (
    "https://raw.githubusercontent.com/nvkelso/natural-earth-vector/"
    "master/geojson/ne_10m_admin_0_disputed_areas.geojson"
)

# Country-baseline OSM relations are huge (whole-country boundaries with
# thousands of nodes). Sub-national disputes are <500KB in migurski's gzipped
# cache. This cap drops the few cases where a whole-country relation is used
# as a set-op operand in someone else's perspective (e.g. France subtracting
# all of France at Mont Blanc).
DISPUTE_SIZE_CAP_BYTES = 500 * 1024


# =============================================================================
# Generic download helpers
# =============================================================================

def _gh_headers() -> dict[str, str]:
    """Honour GITHUB_TOKEN to lift the 60/hr unauthenticated rate limit."""
    import os
    h = {"Accept": "application/vnd.github+json"}
    tok = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")
    if tok:
        h["Authorization"] = f"Bearer {tok}"
    return h


def remote_content_length(url: str, timeout: int = 30) -> int:
    """HEAD a URL; return content-length, or 0 on failure."""
    try:
        r = requests.head(url, allow_redirects=True, timeout=timeout)
        r.raise_for_status()
        return int(r.headers.get("content-length", 0))
    except Exception:
        return 0


def download_one(url: str, dest: Path, expected_size: int) -> str:
    """Download `url` to `dest`. Skip if local size matches expected_size."""
    if dest.exists():
        actual = dest.stat().st_size
        if expected_size and actual == expected_size:
            return f"  skip    {dest.name:<70s} {actual / 1e6:>8.2f} MB (match)"
        if not expected_size and actual > 0:
            return f"  skip    {dest.name:<70s} {actual / 1e6:>8.2f} MB (no size check)"
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".part")
    with requests.get(url, stream=True, timeout=600) as r:
        r.raise_for_status()
        with tmp.open("wb") as f:
            for chunk in r.iter_content(chunk_size=8 << 20):
                f.write(chunk)
    tmp.rename(dest)
    return f"  fetched {dest.name:<70s} {dest.stat().st_size / 1e6:>8.2f} MB"


def download_all(jobs: list[tuple[str, Path, int]], parallel: int) -> None:
    if not jobs:
        return
    with ThreadPoolExecutor(max_workers=parallel) as ex:
        futs = {ex.submit(download_one, u, d, s): d for u, d, s in jobs}
        for f in as_completed(futs):
            print(f.result(), flush=True)


def plan_summary(jobs: list[tuple[str, Path, int]]) -> tuple[int, int]:
    """Return (n_new, total_new_bytes) for jobs whose dest is missing or
    differs from the expected size. Lets us print up-front download cost."""
    new_jobs = [
        (u, d, s) for u, d, s in jobs
        if not d.exists() or (s and d.stat().st_size != s)
    ]
    return len(new_jobs), sum(s for _, _, s in new_jobs)


# =============================================================================
# Stage 1: Overture division_area discovery + download
# =============================================================================

def list_overture_parquets(release: str) -> list[tuple[str, int]]:
    """Return [(url, size_bytes)] for every division_area parquet in the release."""
    prefix = f"release/{release}/theme=divisions/type=division_area/"
    listing_url = (
        f"https://{OVERTURE_BUCKET}.s3.{OVERTURE_REGION}.amazonaws.com/"
        f"?list-type=2&prefix={urllib.parse.quote(prefix, safe='')}"
    )
    r = requests.get(listing_url, timeout=60)
    r.raise_for_status()
    ns = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
    root = ET.fromstring(r.text)
    out: list[tuple[str, int]] = []
    for c in root.findall("s3:Contents", ns):
        key = c.find("s3:Key", ns).text  # type: ignore[union-attr]
        if not key.endswith(".parquet"):
            continue
        size = int(c.find("s3:Size", ns).text)  # type: ignore[union-attr]
        url = f"https://{OVERTURE_BUCKET}.s3.{OVERTURE_REGION}.amazonaws.com/{key}"
        out.append((url, size))
    return out


# =============================================================================
# Stage 3 + 4: migurski configs + dispute relation discovery
# =============================================================================

# Match `- [op, relation, NNNN] # optional comment` inside a YAML list.
LINE_RE = re.compile(
    r"^\s*-\s*\[\s*(plus|minus)\s*,\s*relation\s*,\s*(\d+)\s*\](?:\s*#\s*(.*))?\s*$"
)
KEY_RE = re.compile(r"^(\s*)([A-Za-z0-9_-]+)\s*:\s*$")


def walk_config(text: str) -> tuple[list[dict], dict[str, set[int]]]:
    """For one config file: return (perspective_entries, per_country_base_ids).

    A relation under country C's `perspectives:` block is a TRUE dispute iff
    R ∉ per_country_base_ids[C]. Self-rebadges (e.g. FRA's `perspectives.FRA:
    [+France]` re-adding the whole country after Mont Blanc subtraction) get
    filtered out by that test in `discover_disputes`.
    """
    rows: list[dict] = []
    bases: dict[str, set[int]] = {}
    stack: list[tuple[int, str]] = []
    section: str | None = None
    current_country: str | None = None
    current_perspective: str | None = None
    for raw in text.splitlines():
        if not raw.strip() or raw.lstrip().startswith("#"):
            continue
        m = KEY_RE.match(raw)
        if m:
            indent = len(m.group(1))
            key = m.group(2)
            while stack and stack[-1][0] >= indent:
                stack.pop()
            stack.append((indent, key))
            path = [k for _, k in stack]
            if len(path) == 1:
                current_country = path[0]
                section = None
                current_perspective = None
            elif len(path) == 2:
                section = ("perspectives" if path[1] == "perspectives"
                           else "base" if path[1] == "base" else None)
                current_perspective = None
            elif len(path) >= 3 and section == "perspectives":
                current_perspective = path[2]
            continue
        if section == "base" and current_country is not None:
            mm = LINE_RE.match(raw)
            if mm:
                bases.setdefault(current_country, set()).add(int(mm.group(2)))
        elif section == "perspectives" and current_perspective is not None:
            mm = LINE_RE.match(raw)
            if mm:
                op, rid, comment = mm.group(1), int(mm.group(2)), (mm.group(3) or "").strip()
                rows.append({
                    "country_block": current_country,
                    "perspective": current_perspective,
                    "op": op,
                    "relation_id": rid,
                    "comment": comment,
                })
    return rows, bases


def discover_migurski_yamls(cfg_dir: Path, parallel: int) -> list[Path]:
    """Discover + download every config-*.yaml from migurski/boundary-issues."""
    cfg_dir.mkdir(parents=True, exist_ok=True)
    r = requests.get(
        f"https://api.github.com/repos/{MIGURSKI_REPO}/git/trees/main?recursive=1",
        timeout=30, headers=_gh_headers(),
    )
    r.raise_for_status()
    tree = r.json()
    if tree.get("truncated"):
        sys.exit("ERROR: repo tree exceeded GitHub API limit; need a paginated walker.")
    files = [
        (e["path"], e.get("size", 0))
        for e in tree["tree"]
        if e["type"] == "blob"
           and e["path"].startswith("config-")
           and e["path"].endswith(".yaml")
    ]
    jobs = [
        (f"{MIGURSKI_RAW}/{urllib.parse.quote(p)}",
         cfg_dir / Path(p).name, s)
        for p, s in files
    ]
    print(f"  discovered {len(files)} config-*.yaml files "
          f"({sum(s for _, s in files) / 1024:.1f} KB total)")
    download_all(jobs, parallel=parallel)
    return sorted(cfg_dir.glob("config-*.yaml"))


def discover_disputes(cfg_files: list[Path]) -> dict[int, list[dict]]:
    """Parse all configs; return {relation_id: [refs]} for TRUE disputes
    (excludes self-rebadges where the relation is the country's own base)."""
    rows: list[dict] = []
    bases_by_country: dict[str, set[int]] = {}
    for f in cfg_files:
        r, b = walk_config(f.read_text())
        rows.extend(r)
        for c, ids in b.items():
            bases_by_country.setdefault(c, set()).update(ids)
    by_id: dict[int, list[dict]] = {}
    for r in rows:
        by_id.setdefault(r["relation_id"], []).append(r)
    return {
        rid: refs for rid, refs in by_id.items()
        if any(rid not in bases_by_country.get(r["country_block"], set()) for r in refs)
    }


def ensure_dispute_relation_xmls(rel_dir: Path,
                                 dispute_ids: list[int],
                                 parallel: int) -> dict[int, Path]:
    """HEAD-probe each dispute relation, drop oversize country-baselines via
    DISPUTE_SIZE_CAP_BYTES, download the rest. Returns {id: local_path}."""
    rel_dir.mkdir(parents=True, exist_ok=True)
    print(f"  HEAD-probing {len(dispute_ids)} relation snapshots…")

    def head_one(rid: int) -> tuple[int, int]:
        url = f"{MIGURSKI_RAW}/data/sources/relation/{rid}.osm.xml.gz"
        return rid, remote_content_length(url, timeout=15)

    sizes: dict[int, int] = {}
    with ThreadPoolExecutor(max_workers=parallel) as ex:
        for rid, sz in ex.map(head_one, dispute_ids):
            sizes[rid] = sz

    too_big = sorted(rid for rid, sz in sizes.items() if sz > DISPUTE_SIZE_CAP_BYTES)
    if too_big:
        print(f"  filtering out {len(too_big)} country-baseline relations "
              f"(>{DISPUTE_SIZE_CAP_BYTES/1024:.0f} KB):")
        for rid in too_big:
            print(f"    rel {rid:>10}  {sizes[rid]/1024:>8.1f} KB")
    keep = [rid for rid in dispute_ids if sizes.get(rid, 0) <= DISPUTE_SIZE_CAP_BYTES
                                          and sizes.get(rid, 0) > 0]
    total = sum(sizes[rid] for rid in keep)
    print(f"  retaining {len(keep)} dispute relations "
          f"({total/1024:.1f} KB total)")

    jobs = [
        (f"{MIGURSKI_RAW}/data/sources/relation/{rid}.osm.xml.gz",
         rel_dir / f"{rid}.osm.xml.gz",
         sizes[rid])
        for rid in keep
    ]
    download_all(jobs, parallel=parallel)
    return {rid: rel_dir / f"{rid}.osm.xml.gz" for rid in keep}


# =============================================================================
# Compute Stage 1: country_diff.parquet
# =============================================================================
#
# `__VARNAME__` placeholders are substituted via str.replace before execution
# so the literal `{}` in KV_METADATA blocks doesn't need brace escaping.

PIPELINE_SQL = r"""
INSTALL spatial; LOAD spatial;
SET threads = __THREADS__;
SET preserve_insertion_order = false;
SET memory_limit = '10GB';
SET max_temp_directory_size = '30GB';

-- ===== A. Overture territorial dissolve =====
-- One geometry per ISO covering all subtype IN (country, dependency).
-- Best name: prefer the subtype=country row's primary name; fall back for
-- dependencies (RE, SJ, YT, …) that have no `country` row of their own.
CREATE OR REPLACE TABLE ov AS
SELECT
    country AS iso_alpha2,
    LIST(DISTINCT subtype) AS contributing_subtypes,
    COUNT(*)               AS n_features_ov,
    COALESCE(
        ANY_VALUE(names.primary) FILTER (WHERE subtype = 'country'),
        ANY_VALUE(names.primary)
    ) AS name_ov,
    ST_Union_Agg(ST_MakeValid(geometry)) AS geom_ov
FROM read_parquet('__DIVISION_GLOB__')
WHERE country IS NOT NULL
  AND subtype IN ('country', 'dependency')
GROUP BY country;

ALTER TABLE ov ADD COLUMN n_vertices_ov BIGINT;
UPDATE ov SET n_vertices_ov = ST_NPoints(geom_ov);

-- ===== B. OSM Layercake admin_level=2 country boundaries =====
CREATE OR REPLACE TABLE osm AS
SELECT
    "ISO3166-1:alpha2"     AS iso_alpha2,
    name[1]                AS name_osm,
    ST_MakeValid(geometry) AS geom_osm,
    ST_NPoints(geometry)   AS n_vertices_osm
FROM read_parquet('__OSM_PATH__')
WHERE boundary    = 'administrative'
  AND admin_level = '2'
  AND "ISO3166-1:alpha2" IS NOT NULL;

-- ===== C. Per-ISO geometry algebra (intersect / diff / union) =====
CREATE OR REPLACE TABLE pair_geom AS
SELECT
    o.iso_alpha2, o.contributing_subtypes,
    o.name_ov, m.name_osm,
    o.n_features_ov, o.n_vertices_ov, m.n_vertices_osm,
    o.geom_ov, m.geom_osm,
    ST_Intersection(o.geom_ov, m.geom_osm) AS geom_inter,
    ST_Difference  (o.geom_ov, m.geom_osm) AS geom_ov_only,
    ST_Difference  (m.geom_osm, o.geom_ov) AS geom_osm_only,
    ST_Union       (o.geom_ov, m.geom_osm) AS geom_union
FROM ov o JOIN osm m USING (iso_alpha2);

-- ===== D. Equal Earth (EPSG:8857) reprojection cache for area math =====
CREATE OR REPLACE TABLE pg AS
SELECT
    iso_alpha2, contributing_subtypes, name_ov, name_osm,
    n_features_ov, n_vertices_ov, n_vertices_osm,
    ST_Transform(geom_ov_only,  'EPSG:4326', 'EPSG:8857', true) AS govonly,
    ST_Transform(geom_osm_only, 'EPSG:4326', 'EPSG:8857', true) AS gosmonly,
    geom_ov_only, geom_osm_only,
    geom_ov, geom_osm
FROM pair_geom;

-- ===== E. Raw diff: one row per connected diverging region =====
CREATE OR REPLACE TABLE diff_raw AS
SELECT ROW_NUMBER() OVER () AS did, *
FROM (
    SELECT
        iso_alpha2 AS compared_iso,
        name_ov    AS compared_name,
        'overture_only' AS side,
        ST_Area(govonly) / 1e6 AS area_km2,
        ST_NPoints(govonly)    AS n_vertices,
        contributing_subtypes,
        geom_ov_only AS geometry
    FROM pg
    WHERE NOT ST_IsEmpty(govonly)
    UNION ALL
    SELECT
        iso_alpha2 AS compared_iso,
        name_osm   AS compared_name,
        'osm_only' AS side,
        ST_Area(gosmonly) / 1e6 AS area_km2,
        ST_NPoints(gosmonly)    AS n_vertices,
        contributing_subtypes,
        geom_osm_only AS geometry
    FROM pg
    WHERE NOT ST_IsEmpty(gosmonly)
) t;

-- ===== F. Counterpart attribution =====
CREATE OR REPLACE TABLE candidates AS
SELECT 'osm_only'      AS for_side, iso_alpha2 AS c_iso, name_ov  AS c_name, geom_ov  AS c_geom FROM ov
UNION ALL
SELECT 'overture_only',                iso_alpha2,        name_osm,           geom_osm           FROM osm;

CREATE OR REPLACE TABLE diff_overlaps AS
SELECT
    d.did, d.side, d.compared_iso, d.area_km2,
    c.c_iso, c.c_name,
    ST_Area(ST_Transform(ST_Intersection(d.geometry, c.c_geom),
                         'EPSG:4326', 'EPSG:8857', true)) / 1e6 AS overlap_km2
FROM diff_raw d
JOIN candidates c
  ON c.for_side = d.side
 AND ST_Intersects(d.geometry, c.c_geom)
WHERE c.c_iso != d.compared_iso;

CREATE OR REPLACE TABLE counterpart_summary AS
SELECT
    did,
    ARG_MAX(c_iso,  overlap_km2) AS dominant_iso,
    ARG_MAX(c_name, overlap_km2) AS dominant_name,
    MAX(overlap_km2)             AS dominant_km2,
    COUNT(*)                     AS n_counterparts,
    LIST({iso: c_iso, name: c_name, km2: ROUND(overlap_km2, 2)}
         ORDER BY overlap_km2 DESC) AS counterparts
FROM diff_overlaps
WHERE overlap_km2 > 0.01
GROUP BY did;

-- ===== G. Adaptive percentage formatter =====
CREATE OR REPLACE TEMP MACRO pct_str(p) AS
    CASE
        WHEN p IS NULL OR p <= 0 THEN '0%'
        WHEN p < 0.1             THEN '<0.1%'
        WHEN p < 1               THEN ROUND(p, 2)::VARCHAR || '%'
        ELSE ROUND(p, 1)::VARCHAR || '%'
    END;

-- ===== H. Final enriched diff (drop <0.01 km² noise rows) =====
CREATE OR REPLACE TABLE final_diff AS
WITH base AS (
    SELECT
        d.compared_iso, d.compared_name, d.side,
        ROUND(d.area_km2, 2) AS area_km2,
        d.n_vertices, d.contributing_subtypes,
        cs.dominant_iso  AS counterpart_iso,
        cs.dominant_name AS counterpart_name,
        ROUND(cs.dominant_km2, 2) AS counterpart_overlap_km2,
        ROUND(cs.dominant_km2 * 100.0 / NULLIF(d.area_km2, 0), 1) AS counterpart_coverage_pct,
        cs.n_counterparts,
        cs.counterparts,
        d.geometry
    FROM diff_raw d
    LEFT JOIN counterpart_summary cs USING (did)
    WHERE ROUND(d.area_km2, 2) >= 0.01
),
enriched AS (
    SELECT
        b.*,
        ROUND(
            COALESCE(LIST_AGGREGATE(LIST_TRANSFORM(counterparts, x -> x.km2), 'sum'), 0)
              * 100.0 / NULLIF(area_km2, 0)
        , 1) AS total_coverage_pct
    FROM base b
)
SELECT
    e.*,
    COALESCE(
        LIST_AGGREGATE(
            LIST_TRANSFORM(
                counterparts[1:LEAST(3, COALESCE(n_counterparts, 0))],
                c -> c.iso || ' (' || c.name || ') '
                     || pct_str(c.km2 * 100.0 / NULLIF(area_km2, 0))
            ),
            'string_agg', ', '
        ), ''
    ) AS top3_str,
    CASE WHEN n_counterparts > 3
         THEN ' (plus ' || (n_counterparts - 3)::VARCHAR || ' more)'
         ELSE '' END AS plus_more,
    pct_str(total_coverage_pct)         AS coverage_str,
    pct_str(100.0 - total_coverage_pct) AS unattrib_str
FROM enriched e;

-- ===== I. Write country_diff.parquet =====
COPY (
    SELECT
        compared_iso, compared_name, side,
        area_km2, n_vertices, contributing_subtypes,
        counterpart_iso, counterpart_name,
        counterpart_overlap_km2, counterpart_coverage_pct,
        n_counterparts, total_coverage_pct, counterparts,
        CASE
            WHEN side = 'osm_only' AND counterpart_iso IS NULL THEN
                'This ' || area_km2::VARCHAR || ' km² region is inside the OSM admin_level=2 polygon for '
                || compared_iso || ' (' || compared_name || '). '
                || 'No Overture country or dependency polygon covers any of it '
                || '(typically OSM is including territorial waters or coastal extent that Overture excludes).'
            WHEN side = 'osm_only' AND total_coverage_pct >= 99.95 AND n_counterparts = 1 THEN
                'This ' || area_km2::VARCHAR || ' km² region is inside the OSM polygon for '
                || compared_iso || ' (' || compared_name || '). '
                || 'In Overture the same region falls entirely inside '
                || counterpart_iso || ' (' || counterpart_name || ').'
            WHEN side = 'osm_only' AND total_coverage_pct >= 99.95 THEN
                'This ' || area_km2::VARCHAR || ' km² region is inside the OSM polygon for '
                || compared_iso || ' (' || compared_name || '). '
                || 'In Overture the same region is split across ' || n_counterparts::VARCHAR || ' ISOs: '
                || top3_str || plus_more
                || '. (Each percentage = share of this diff polygon.)'
            WHEN side = 'osm_only' THEN
                'This ' || area_km2::VARCHAR || ' km² region is inside the OSM polygon for '
                || compared_iso || ' (' || compared_name || '). '
                || 'In Overture, ' || coverage_str || ' of it lies in any country or dependency polygon — '
                || top3_str || plus_more
                || '. The remaining ' || unattrib_str || ' lies in no Overture polygon '
                || '(typically OSM territorial waters Overture excludes).'
            WHEN side = 'overture_only' AND counterpart_iso IS NULL THEN
                'This ' || area_km2::VARCHAR || ' km² region is inside the Overture territorial polygon for '
                || compared_iso || ' (' || compared_name || '). '
                || 'No OSM admin_level=2 country polygon covers any of it '
                || '(typically a disputed region OSM leaves unassigned, like Western Sahara claimed by MA, '
                || 'or Abyei XY).'
            WHEN side = 'overture_only' AND total_coverage_pct >= 99.95 AND n_counterparts = 1 THEN
                'This ' || area_km2::VARCHAR || ' km² region is inside the Overture polygon for '
                || compared_iso || ' (' || compared_name || '). '
                || 'In OSM the same region falls entirely inside '
                || counterpart_iso || ' (' || counterpart_name || ').'
            WHEN side = 'overture_only' AND total_coverage_pct >= 99.95 THEN
                'This ' || area_km2::VARCHAR || ' km² region is inside the Overture polygon for '
                || compared_iso || ' (' || compared_name || '). '
                || 'In OSM the same region is split across ' || n_counterparts::VARCHAR || ' ISOs: '
                || top3_str || plus_more
                || '. (Each percentage = share of this diff polygon.)'
            ELSE
                'This ' || area_km2::VARCHAR || ' km² region is inside the Overture polygon for '
                || compared_iso || ' (' || compared_name || '). '
                || 'In OSM, ' || coverage_str || ' of it lies in any admin_level=2 country polygon — '
                || top3_str || plus_more
                || '. The remaining ' || unattrib_str || ' lies in no OSM admin_level=2 polygon '
                || '(typical for disputed territory: Western Sahara, Abyei, etc.).'
        END AS explanation,
        geometry
    FROM final_diff
    ORDER BY area_km2 DESC
) TO '__OUT_PATH__' (
    FORMAT parquet, COMPRESSION zstd,
    KV_METADATA {
        title:
            'Country boundary divergence (TERRITORIAL): Overture (__RELEASE__) vs OSM US Layercake',
        semantics:
            'Each row = one connected sub-region where the two datasets disagree on which ISO contains it. compared_iso = the ISO whose Overture-polygon vs OSM-polygon pair was being compared (both datasets emit a polygon for this ISO; they disagree on its footprint). side=osm_only: this region is inside OSM[compared_iso] but outside Overture[compared_iso]. side=overture_only: vice versa. counterpart_iso = the ISO that the OTHER dataset assigns the LARGEST share of this region to. counterpart_coverage_pct = the dominant single counterpart''s share of this diff polygon. total_coverage_pct = how much of this diff polygon has ANY counterpart in the other dataset (100 = fully attributed elsewhere; 0 = unattributed; in between = mixed).',
        methodology:
            'Overture: every division_area row with country IS NOT NULL AND subtype IN (country, dependency), grouped + dissolved by `country` column. OSM: boundary=administrative AND admin_level=2 AND ISO3166-1:alpha2 IS NOT NULL. Areas measured in EPSG:8857 (Equal Earth) for global validity, intersections in EPSG:4326 then reprojected. Native vertex precision; no simplification.',
        explanation_format:
            'In every explanation, percentages refer to share of THIS row''s diff polygon (area_km2). counterparts STRUCT[] gives the full breakdown if you need more than the top-3 listed.',
        columns:
            'compared_iso, compared_name, side, area_km2, n_vertices, contributing_subtypes, counterpart_iso, counterpart_name, counterpart_overlap_km2, counterpart_coverage_pct, n_counterparts, total_coverage_pct, counterparts (STRUCT[iso,name,km2][]), explanation, geometry (WKB EPSG:4326). Sorted by area_km2 desc.'
    }
);
"""


def produce_country_diff(*, division_glob: str, osm_path: Path, out_path: Path,
                         threads: int, release: str, db_path: Path | None) -> None:
    sql = (
        PIPELINE_SQL
        .replace("__THREADS__", str(threads))
        .replace("__RELEASE__", release)
        .replace("__DIVISION_GLOB__", division_glob.replace("'", "''"))
        .replace("__OSM_PATH__", str(osm_path).replace("'", "''"))
        .replace("__OUT_PATH__", str(out_path).replace("'", "''"))
    )
    con = duckdb.connect(":memory:" if db_path is None else str(db_path))
    try:
        con.execute(sql)
        n_total, total_km2, n_osm, n_ov = con.execute(f"""
            SELECT COUNT(*),
                   ROUND(SUM(area_km2)),
                   COUNT_IF(side = 'osm_only'),
                   COUNT_IF(side = 'overture_only')
            FROM read_parquet('{str(out_path).replace("'", "''")}')
        """).fetchone()
        print(f"  Wrote {out_path}")
        print(f"    rows:           {n_total} ({n_osm} osm_only + {n_ov} overture_only)")
        print(f"    total disagree: {total_km2:,.0f} km²")
    finally:
        con.close()


# =============================================================================
# Compute Stage 2: migurski_disputes.parquet
# =============================================================================

def assemble_polygon(xml_gz_path: Path, target_relation_id: int):
    """Return a Shapely (Multi)Polygon for the named relation, or None.

    pyosmium's `apply_file(..., locations=True)` caches node positions and
    emits `area()` callbacks for closed ways AND for type=multipolygon /
    type=boundary relations. We filter to from_way()=False and matching
    orig_id().
    """
    fab = osmium.geom.GeoJSONFactory()
    found: list[dict] = []

    class Handler(osmium.SimpleHandler):
        def area(self, a):
            if a.from_way() or a.orig_id() != target_relation_id:
                return
            try:
                geo = fab.create_multipolygon(a)
            except Exception:
                return
            try:
                found.append(json.loads(geo))
            except Exception:
                return

    tmp = xml_gz_path.with_suffix("")  # strip .gz -> .xml
    with gzip.open(xml_gz_path, "rb") as f:
        tmp.write_bytes(f.read())
    try:
        Handler().apply_file(str(tmp), locations=True)
    finally:
        tmp.unlink(missing_ok=True)

    if not found:
        return None
    geoms = [shape(g) for g in found]
    if len(geoms) == 1:
        return geoms[0]
    from shapely.ops import unary_union
    return unary_union(geoms)


def produce_migurski_disputes(*,
                              relation_paths: dict[int, Path],
                              dispute_refs: dict[int, list[dict]],
                              out_path: Path,
                              threads: int) -> None:
    """Assemble each relation's polygon via pyosmium; write parquet."""
    print(f"  Assembling {len(relation_paths)} dispute polygons via pyosmium…")
    rows: list[dict] = []
    failed: list[tuple[int, str]] = []
    for rid in sorted(relation_paths):
        try:
            geom = assemble_polygon(relation_paths[rid], rid)
        except Exception as e:
            failed.append((rid, f"exception: {e}"))
            continue
        if geom is None or geom.is_empty:
            failed.append((rid, "no polygon assembled"))
            continue
        refs = dispute_refs[rid]
        comments = sorted({r["comment"] for r in refs if r["comment"]})
        country_blocks = sorted({r["country_block"] for r in refs})
        rows.append({
            "relation_id": rid,
            "label": comments[0] if comments else f"OSM relation {rid}",
            "country_blocks": ";".join(country_blocks),
            "all_comments": " | ".join(comments),
            "wkb": geom.wkb,
        })
    if failed:
        print(f"  {len(failed)} relations failed to assemble:")
        for rid, why in failed:
            print(f"    rel {rid}: {why}")
    if not rows:
        raise RuntimeError("no polygons assembled — cannot write migurski_disputes.parquet")

    table = pa.table({
        "relation_id":    [r["relation_id"]    for r in rows],
        "label":          [r["label"]          for r in rows],
        "country_blocks": [r["country_blocks"] for r in rows],
        "all_comments":   [r["all_comments"]   for r in rows],
        "wkb":            [r["wkb"]            for r in rows],
    })
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    con.execute(f"SET threads={threads}")
    con.register("staging", table)
    con.execute(f"""
        COPY (
            SELECT relation_id, label, country_blocks, all_comments,
                   ROUND(
                     ST_Area(ST_Transform(ST_GeomFromWKB(wkb),
                                          'EPSG:4326','EPSG:8857',true)) / 1e6, 2
                   ) AS area_km2,
                   ST_GeomFromWKB(wkb) AS geometry
            FROM staging
            ORDER BY area_km2 DESC
        ) TO '{str(out_path).replace("'", "''")}' (
            FORMAT parquet, COMPRESSION zstd,
            KV_METADATA {{
                title:
                    'Geopolitical dispute polygons sourced from migurski/boundary-issues YAML configs',
                source:
                    'https://github.com/migurski/boundary-issues — config-*.yaml files. Each row is one OSM relation referenced under a country perspectives: block (where that country base: does NOT contain it). Whole-country relations used as set-op operands have been filtered out via a 500KB size cap.',
                semantics:
                    'One row per OSM relation = one named dispute polygon (Kashmir sub-features, Crimea/Donbas oblasts, Esequibo, Spratly, West Bank, Golan, Abkhazia, Western Sahara, etc.). country_blocks = semicolon-list of country perspectives that reference this dispute. all_comments = pipe-separated YAML inline comments naming the dispute.',
                geometry:
                    'WKB EPSG:4326. Assembled from OSM relation XML via pyosmium. ST_MakeValid not yet applied (run on consumer side if needed).',
                area_basis: 'EPSG:8857 (Equal Earth)'
            }}
        )
    """)
    n, km2 = con.execute(
        f"SELECT COUNT(*), ROUND(SUM(area_km2),0) FROM read_parquet('{out_path}')"
    ).fetchone()
    print(f"  Wrote {out_path} : {n} rows, {km2:,.0f} km² total")
    con.close()


# =============================================================================
# Compute Stage 3: multi_country_mask.parquet
# =============================================================================

MASK_SQL = r"""
INSTALL spatial; LOAD spatial;
SET threads = __THREADS__;
SET preserve_insertion_order = false;

CREATE OR REPLACE TABLE mask (
    source           VARCHAR,
    name             VARCHAR,
    description      VARCHAR,
    compared_iso     VARCHAR,
    counterpart_iso  VARCHAR,
    side             VARCHAR,
    area_km2         DOUBLE,
    geometry         GEOMETRY
);

-- ----- A. migurski disputes -----
INSERT INTO mask (source, name, description, area_km2, geometry)
SELECT
    'migurski_disputes',
    label                                                 AS name,
    'Disputed area from migurski/boundary-issues (OSM rel '
      || relation_id::VARCHAR
      || '): ' || label
      || ' — claimed across: ' || country_blocks           AS description,
    area_km2,
    ST_MakeValid(geometry)
FROM read_parquet('__MIG_PATH__');

-- ----- B. country_diff (Overture vs OSM) -----
INSERT INTO mask
SELECT
    'overture_osm_diff'                       AS source,
    compared_iso || '_' || side               AS name,
    explanation                               AS description,
    compared_iso, counterpart_iso, side,
    area_km2,
    geometry
FROM read_parquet('__DIFF_PATH__');
"""

# Natural Earth loading is schema-dependent so it stays in Python (probes
# columns via DESCRIBE before building the SQL).

MASK_WRITE_SQL = r"""
UPDATE mask
SET area_km2 = ROUND(
    ST_Area(ST_Transform(geometry, 'EPSG:4326', 'EPSG:8857', true)) / 1e6, 2
)
WHERE area_km2 IS NULL;

COPY (
    SELECT source, name, description,
           compared_iso, counterpart_iso, side,
           area_km2, geometry
    FROM mask
    ORDER BY area_km2 DESC NULLS LAST
) TO '__OUT_PATH__' (
    FORMAT parquet, COMPRESSION zstd,
    KV_METADATA {
        title:
            'Multi-country-valid mask: Natural Earth disputed areas UNION migurski/boundary-issues disputes UNION Overture-vs-OSM divergence',
        purpose:
            'For Overture Places filter (issue #3692). When a place lat/lon does NOT fall inside the Overture territorial polygon for its country tag, intersect the lat/lon with this mask: if any polygon contains it, KEEP the place — its country tag could be legitimately valid under a different boundary convention (political dispute, overseas-territory roll-up, or maritime extent difference).',
        sources:
            'source=natural_earth_disputed: every polygon in nvkelso/natural-earth-vector ne_10m_admin_0_disputed_areas.geojson (curated, ~50-100 disputes including ones BOTH Overture and OSM agree on — Kashmir, Falklands, Antarctic claims, etc.). source=migurski_disputes: ~42 named OSM-relation polygons extracted from migurski/boundary-issues YAML configs — every relation referenced under a country `perspectives:` block where that country `base:` does not contain it, with country-baseline relations filtered out via 500KB size cap. source=overture_osm_diff: every row from country_diff.parquet (Overture territorial dissolve vs OSM US Layercake admin_level=2; covers categories A political disputes, B overseas-territory ISO encoding differences, C maritime/coastal slivers).',
        semantics:
            'One row per polygon. source identifies origin. name = NE feature name / migurski label / <compared_iso>_<side> for diff. description = human-readable provenance. compared_iso / counterpart_iso / side are populated for overture_osm_diff rows only.',
        geometry:
            'WKB EPSG:4326. ST_MakeValid applied. No simplification.',
        area_basis:
            'EPSG:8857 (Equal Earth) for global validity.',
        overlap_note:
            'Polygons from the three sources overlap heavily (e.g. Crimea appears in all three). This is intentional — provenance is preserved per row. For ST_Intersects-based filtering this is harmless. To get a single dissolved mask: SELECT ST_Union_Agg(geometry) FROM read_parquet(this_file).',
        usage:
            'SELECT EXISTS (SELECT 1 FROM mask m WHERE ST_Intersects(m.geometry, ST_Point(lon, lat)))'
    }
);
"""


def produce_multi_country_mask(*,
                               diff_path: Path,
                               migurski_path: Path,
                               ne_path: Path,
                               out_path: Path,
                               threads: int) -> None:
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    con.execute(f"SET threads={threads}")
    con.execute(
        MASK_SQL
        .replace("__THREADS__", str(threads))
        .replace("__DIFF_PATH__", str(diff_path).replace("'", "''"))
        .replace("__MIG_PATH__", str(migurski_path).replace("'", "''"))
    )

    # Natural Earth: schema-aware so column names that don't exist become NULL.
    p = str(ne_path.resolve()).replace("'", "''")
    schema = con.execute(
        f"DESCRIBE SELECT * FROM ST_Read('{p}') LIMIT 0"
    ).fetchall()
    cols = {row[0].lower() for row in schema}
    pick = lambda n: n if n.lower() in cols else "NULL"
    con.execute(f"""
        INSERT INTO mask (source, name, description, geometry)
        WITH src AS (
            SELECT
                {pick("name")}      AS f_name,
                {pick("name_long")} AS f_long,
                {pick("brk_name")}  AS f_brk,
                {pick("featurecla")} AS f_cls,
                {pick("note_brk")}  AS f_note,
                ST_MakeValid(geom)  AS valid_geom
            FROM ST_Read('{p}')
            WHERE geom IS NOT NULL AND NOT ST_IsEmpty(geom)
        )
        SELECT
            'natural_earth_disputed',
            COALESCE(NULLIF(f_name, ''), NULLIF(f_long, ''),
                     NULLIF(f_brk,  ''), 'unnamed dispute'),
            'Natural Earth disputed area: '
              || COALESCE(NULLIF(f_name, ''), NULLIF(f_long, ''),
                          NULLIF(f_brk,  ''), 'unnamed')
              || COALESCE(' [' || NULLIF(f_cls,  '') || ']', '')
              || COALESCE(' — ' || NULLIF(f_note, ''), ''),
            valid_geom
        FROM src
        WHERE valid_geom IS NOT NULL
          AND NOT ST_IsEmpty(valid_geom)
          AND ST_Area(valid_geom) > 0
    """)

    con.execute(MASK_WRITE_SQL.replace("__OUT_PATH__", str(out_path).replace("'", "''")))

    out_str = str(out_path).replace("'", "''")
    rows = con.execute(f"""
        SELECT source, COUNT(*) AS n,
               COALESCE(ROUND(SUM(area_km2)), 0) AS total_km2
        FROM read_parquet('{out_str}')
        GROUP BY source ORDER BY total_km2 DESC
    """).fetchall()
    print(f"  Wrote {out_path}")
    for src, n, km2 in rows:
        print(f"    {src:<22s} {n:>5d} polygons   {km2:>14,.0f} km²")
    n, km2 = con.execute(
        f"SELECT COUNT(*), COALESCE(ROUND(SUM(area_km2)),0) FROM read_parquet('{out_str}')"
    ).fetchone()
    print(f"    {'TOTAL':<22s} {n:>5d} polygons   {km2:>14,.0f} km² (sum; not dissolved)")
    con.close()


# =============================================================================
# Main
# =============================================================================

def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("--release", default=DEFAULT_RELEASE,
                    help=f"Overture release version (default: {DEFAULT_RELEASE})")
    ap.add_argument("--data-dir", default=Path("data"), type=Path,
                    help="Working directory (default: ./data)")
    ap.add_argument("--threads", type=int, default=12,
                    help="DuckDB worker threads (default: 12)")
    ap.add_argument("--parallel-downloads", type=int, default=4,
                    help="Concurrent download streams (default: 4)")
    ap.add_argument("--db-path", type=Path, default=Path("compare.ddb"),
                    help="DuckDB scratch DB. Use ':memory:' for ephemeral. (default: ./compare.ddb)")
    ap.add_argument("--country-diff-out", type=Path, default=None,
                    help="country_diff.parquet output (default: ./country_diff.parquet)")
    ap.add_argument("--migurski-out", type=Path, default=None,
                    help="migurski_disputes.parquet output (default: ./migurski_disputes.parquet)")
    ap.add_argument("--mask-out", type=Path, default=None,
                    help="multi_country_mask.parquet output (default: ./multi_country_mask.parquet)")
    ap.add_argument("--no-mask", action="store_true",
                    help="Stop after country_diff.parquet (skip migurski + Natural Earth + mask).")
    ap.add_argument("--force", action="store_true",
                    help="Rebuild every parquet output even if it already exists.")
    args = ap.parse_args()

    data = args.data_dir.resolve()
    div_dir = data / "division_area"
    cfg_dir = data / "migurski_configs"
    rel_dir = data / "migurski_relations"
    ne_dir  = data / "natural_earth"
    osm_path        = data / "osm_boundaries_layercake.parquet"
    ne_path         = ne_dir / Path(NE_DISPUTED_URL).name
    diff_out        = (args.country_diff_out or Path("country_diff.parquet")).resolve()
    migurski_out    = (args.migurski_out     or Path("migurski_disputes.parquet")).resolve()
    mask_out        = (args.mask_out         or Path("multi_country_mask.parquet")).resolve()
    db_path         = None if str(args.db_path) == ":memory:" else args.db_path.resolve()

    print("=" * 78)
    print(" disputed_areas.py — Overture × OSM × migurski × Natural Earth pipeline")
    print("=" * 78)
    print(f" Release          : {args.release}")
    print(f" Data dir         : {data}")
    print(f" country_diff out : {diff_out}")
    if not args.no_mask:
        print(f" migurski out     : {migurski_out}")
        print(f" mask out         : {mask_out}")
    print(f" --force          : {args.force}")
    print(f" --no-mask        : {args.no_mask}")
    print()

    # ------------------------------------------------------------------ Stage 1
    print("[1/8] Overture division_area parquet")
    need_diff = args.force or not diff_out.exists()
    if need_diff:
        overture = list_overture_parquets(args.release)
        ov_jobs = [
            (u, div_dir / Path(urllib.parse.urlparse(u).path).name, s)
            for u, s in overture
        ]
        n_new, bytes_new = plan_summary(ov_jobs)
        print(f"  release lists {len(ov_jobs)} files "
              f"({sum(s for _, s in overture) / 1e9:.2f} GB total); "
              f"{n_new} missing locally ({bytes_new / 1e9:.2f} GB to download)")
        download_all(ov_jobs, parallel=args.parallel_downloads)
    else:
        print(f"  skipped (country_diff output already exists at {diff_out})")
    print()

    # ------------------------------------------------------------------ Stage 2
    print("[2/8] OSM US Layercake boundaries.parquet")
    if need_diff:
        osm_size = remote_content_length(OSM_LAYERCAKE_URL)
        n_new, bytes_new = plan_summary([(OSM_LAYERCAKE_URL, osm_path, osm_size)])
        print(f"  remote {osm_size / 1e9:.2f} GB; "
              f"{'will download' if n_new else 'already cached'}")
        download_all([(OSM_LAYERCAKE_URL, osm_path, osm_size)],
                     parallel=args.parallel_downloads)
    else:
        print("  skipped (country_diff output already exists)")
    print()

    # ------------------------------------------------------------------ Stage 6
    # Run country_diff compute first so we can short-circuit downstream work
    # on --no-mask.
    print("[6/8] DuckDB pipeline → country_diff.parquet")
    if need_diff:
        produce_country_diff(
            division_glob=str(div_dir / "*.parquet"),
            osm_path=osm_path,
            out_path=diff_out,
            threads=args.threads,
            release=args.release,
            db_path=db_path,
        )
    else:
        print(f"  skipped ({diff_out} exists; pass --force to rebuild)")
    print()

    if args.no_mask:
        print("[--no-mask] stopping after country_diff.parquet")
        return 0

    # ------------------------------------------------------------------ Stage 3
    print("[3/8] migurski/boundary-issues YAML configs")
    cfg_files = sorted(cfg_dir.glob("config-*.yaml"))
    if args.force or not cfg_files:
        cfg_files = discover_migurski_yamls(cfg_dir, args.parallel_downloads)
    else:
        print(f"  found {len(cfg_files)} cached configs in {cfg_dir}/")
    print()

    # ------------------------------------------------------------------ Stage 4
    print("[4/8] Discover dispute relation IDs from configs")
    dispute_refs = discover_disputes(cfg_files)
    print(f"  {len(dispute_refs)} TRUE dispute relations "
          f"(self-rebadges already filtered out)")
    print()

    # ------------------------------------------------------------------ Stage 5
    print("[5/8] migurski OSM-relation snapshots")
    need_migurski = args.force or not migurski_out.exists()
    if need_migurski:
        relation_paths = ensure_dispute_relation_xmls(
            rel_dir, list(dispute_refs.keys()), args.parallel_downloads
        )
    else:
        print(f"  skipped ({migurski_out} exists; pass --force to rebuild)")
        relation_paths = {}
    print()

    # ------------------------------------------------------------------ Stage 7
    print("[7/8] Assemble migurski_disputes.parquet (pyosmium)")
    if need_migurski:
        produce_migurski_disputes(
            relation_paths=relation_paths,
            dispute_refs=dispute_refs,
            out_path=migurski_out,
            threads=args.threads,
        )
    else:
        print(f"  skipped ({migurski_out} exists)")
    print()

    # ------------------------------------------------------------------ Stages 5b + 8
    # Natural Earth download is wired here so it only runs when we need the
    # mask. (685 KB, single GeoJSON file.)
    print("[5b/8] Natural Earth admin-0 disputed areas")
    need_mask = args.force or not mask_out.exists()
    if need_mask:
        ne_size = remote_content_length(NE_DISPUTED_URL)
        download_all([(NE_DISPUTED_URL, ne_path, ne_size)],
                     parallel=args.parallel_downloads)
    else:
        print(f"  skipped ({mask_out} exists)")
    print()

    print("[8/8] Build multi_country_mask.parquet")
    if need_mask:
        produce_multi_country_mask(
            diff_path=diff_out,
            migurski_path=migurski_out,
            ne_path=ne_path,
            out_path=mask_out,
            threads=args.threads,
        )
    else:
        print(f"  skipped ({mask_out} exists; pass --force to rebuild)")
    print()

    print("All stages complete.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
