# Overture vs OSM Layercake — Country Polygon Comparison
A comparison between OSM and Overture boundaries

## Run
Clone the repo, install uv and run with `uv run country_diff.py`.
Outputs a parquet file. 

## Summary

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


Interactive viewer here: https://do-me.github.io/geoparquet-visualizer/?map=1.83%2F52.04509%2F-23.90047%2F0.00%2F0.00&sidebar=1&style=https%3A%2F%2Ftiles.openfreemap.org%2Fstyles%2Fliberty&background=%23f8fafc&spinSpeedX=0.1&spinSpeedY=0&url=https%3A%2F%2Fraw.githubusercontent.com%2Fdo-me%2Fosm-overture-country-boundaries%2Fmain%2Fcountry_diff.parquet

<img width="3840" height="2216" alt="image" src="https://github.com/user-attachments/assets/10bb0629-be4a-4024-a70c-750b1cb10eba" />
