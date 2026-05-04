#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = ["duckdb>=1.4", "requests>=2.32"]
# ///
"""
Reproduce country_diff.parquet end-to-end.

  1. List + download every division_area parquet for the Overture release
     (default 2026-04-15.0) into <data-dir>/division_area/
  2. Download the OSM US Layercake boundaries.parquet into <data-dir>/
  3. Run a single DuckDB pipeline that produces <data-dir>/country_diff.parquet:
       - Overture: dissolve every division_area row whose `country` column is set
         and whose subtype IN (country, dependency), grouped by country.
       - OSM: filter boundary=administrative AND admin_level=2 AND ISO3166-1:alpha2 IS NOT NULL.
       - Per ISO: ST_Intersection / Difference / Union; emit one row per
         connected diverging region.
       - Spatial-join each diverging region against every country in the
         opposite dataset to attribute it.
       - Compute total_coverage_pct, counterpart_coverage_pct, and a single
         human-readable explanation per row.
       - Drop sub-0.01 km² numerical-noise rows; sort by area desc; embed
         KV metadata describing semantics, methodology and column meanings.

Usage:
    uv run reproduce_country_diff.py
    uv run reproduce_country_diff.py --release 2026-04-15.0 --data-dir data --threads 12

Idempotent: existing local files are skipped when their size matches the
remote (Overture sizes from S3 ListObjectsV2; OSM size from HEAD).

First run downloads ~7 GB (≈5.25 GB Overture + ≈1.98 GB OSM) and runs the
pipeline in ~10–15 min on a 12-thread laptop. Re-runs with files already
present skip the network entirely.
"""
from __future__ import annotations

import argparse
import sys
import urllib.parse
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import duckdb
import requests


# ---- constants ------------------------------------------------------------

OVERTURE_BUCKET = "overturemaps-us-west-2"
OVERTURE_REGION = "us-west-2"
OSM_LAYERCAKE_URL = "https://data.openstreetmap.us/layercake/boundaries.parquet"
DEFAULT_RELEASE = "2026-04-15.0"


# ---- discovery + download -------------------------------------------------

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


def remote_content_length(url: str) -> int:
    r = requests.head(url, allow_redirects=True, timeout=30)
    r.raise_for_status()
    return int(r.headers.get("content-length", 0))


def download_one(url: str, dest: Path, expected_size: int) -> str:
    """Download `url` to `dest`. Skip if the local file size matches expected_size."""
    if dest.exists():
        actual = dest.stat().st_size
        if expected_size and actual == expected_size:
            return f"  skip    {dest.name:<70s} {actual / 1e6:>8.1f} MB (match)"
        if not expected_size and actual > 0:
            return f"  skip    {dest.name:<70s} {actual / 1e6:>8.1f} MB (no remote check)"
    tmp = dest.with_suffix(dest.suffix + ".part")
    with requests.get(url, stream=True, timeout=600) as r:
        r.raise_for_status()
        with tmp.open("wb") as f:
            for chunk in r.iter_content(chunk_size=8 << 20):  # 8 MB chunks
                f.write(chunk)
    tmp.rename(dest)
    return f"  fetched {dest.name:<70s} {dest.stat().st_size / 1e6:>8.1f} MB"


def download_all(jobs: list[tuple[str, Path, int]], parallel: int) -> None:
    with ThreadPoolExecutor(max_workers=parallel) as ex:
        futs = {ex.submit(download_one, u, d, s): d for u, d, s in jobs}
        for f in as_completed(futs):
            print(f.result(), flush=True)


# ---- DuckDB pipeline ------------------------------------------------------

# `__VARNAME__` placeholders are substituted via str.replace before execution.
# Using placeholders (not str.format) keeps the literal `{}` of KV_METADATA
# blocks unescaped.
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
-- Single ROW_NUMBER over the unioned set so `did` is globally unique.
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
-- For each diff polygon, find every country in the OPPOSITE dataset whose
-- polygon overlaps it, and quantify the overlap in km².
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
-- Honest with tiny non-zero values: "0.0%" would lie about a 13 km² ES
-- sliver inside an 81 791 km² MA diff (Western Sahara case).
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

-- ===== I. Write country_diff.parquet (final shape, KV metadata, sorted) =====
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


def run_pipeline(*, division_glob: str, osm_path: Path, out_path: Path,
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
        # Sanity report
        n_total, total_km2, n_osm, n_ov = con.execute(f"""
            SELECT COUNT(*),
                   ROUND(SUM(area_km2)),
                   COUNT_IF(side = 'osm_only'),
                   COUNT_IF(side = 'overture_only')
            FROM read_parquet('{str(out_path).replace("'", "''")}')
        """).fetchone()
        print(f"\n  Wrote {out_path}")
        print(f"    rows:           {n_total} ({n_osm} osm_only + {n_ov} overture_only)")
        print(f"    total disagree: {total_km2:,.0f} km²")
        # Show top 3 explanations as a smoke test
        rows = con.execute(f"""
            SELECT compared_iso, side, ROUND(area_km2)::VARCHAR || ' km²' AS km2,
                   substr(explanation, 1, 160) || '…' AS preview
            FROM read_parquet('{str(out_path).replace("'", "''")}')
            ORDER BY area_km2 DESC
            LIMIT 3
        """).fetchall()
        print("    sample top 3:")
        for r in rows:
            print(f"      {r[0]:<5s} {r[1]:<14s} {r[2]:>14s}  {r[3]}")
    finally:
        con.close()


# ---- main -----------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("--release", default=DEFAULT_RELEASE,
                    help=f"Overture release version (default: {DEFAULT_RELEASE})")
    ap.add_argument("--data-dir", default=Path("data"), type=Path,
                    help="Directory for downloaded parquet files (default: ./data)")
    ap.add_argument("--threads", type=int, default=12,
                    help="DuckDB worker threads (default: 12)")
    ap.add_argument("--parallel-downloads", type=int, default=4,
                    help="Concurrent download streams (default: 4)")
    ap.add_argument("--db-path", type=Path, default=Path("compare.ddb"),
                    help="DuckDB scratch DB. Use ':memory:' for ephemeral. (default: ./compare.ddb)")
    ap.add_argument("--out", type=Path, default=None,
                    help="Output path (default: <data-dir>/country_diff.parquet)")
    args = ap.parse_args()

    data = args.data_dir.resolve()
    div_dir = data / "division_area"
    div_dir.mkdir(parents=True, exist_ok=True)
    out_path = (args.out or data / "country_diff.parquet").resolve()
    db_path = None if str(args.db_path) == ":memory:" else args.db_path.resolve()

    print(f"Overture release : {args.release}")
    print(f"Data directory   : {data}")
    print(f"Output           : {out_path}")
    print(f"DuckDB scratch   : {db_path or ':memory:'}")
    print()

    print("Discovering Overture division_area parquet files…")
    overture = list_overture_parquets(args.release)
    total_gb = sum(s for _, s in overture) / 1e9
    print(f"  {len(overture)} files, {total_gb:.2f} GB total")

    jobs: list[tuple[str, Path, int]] = [
        (u, div_dir / Path(urllib.parse.urlparse(u).path).name, s) for u, s in overture
    ]

    print("Probing OSM Layercake boundaries.parquet…")
    osm_size = remote_content_length(OSM_LAYERCAKE_URL)
    osm_path = data / "osm_boundaries_layercake.parquet"
    print(f"  {osm_size / 1e9:.2f} GB")
    jobs.append((OSM_LAYERCAKE_URL, osm_path, osm_size))

    print(f"\nDownloading {len(jobs)} files (up to {args.parallel_downloads} concurrent)…")
    download_all(jobs, parallel=args.parallel_downloads)

    print(f"\nRunning DuckDB pipeline ({args.threads} threads)…")
    run_pipeline(
        division_glob=str(div_dir / "*.parquet"),
        osm_path=osm_path,
        out_path=out_path,
        threads=args.threads,
        release=args.release,
        db_path=db_path,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
