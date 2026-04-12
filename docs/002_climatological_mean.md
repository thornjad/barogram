# 002 Climatological Mean Model

## Overview

The climatological mean model forecasts the historical average conditions for the target time's calendar month and hour of day. For each lead time it looks up the average temperature, humidity, pressure, and wind speed across all past Tempest observations that fall in the same (month, hour) bucket, and issues that average as the forecast. Unlike persistence, which anchors to current conditions, the climatological mean anchors to the historical baseline for this time of year and time of day.

## Method

For each lead time (+6h, +12h, +18h, +24h), the target `valid_at` timestamp is converted to local time. The calendar month and hour are extracted and used to query all accumulated Tempest observations in that (month, hour) bucket. The arithmetic mean of each variable across those observations is the forecast value.

Bucket matching uses local time for both the query (SQLite `'localtime'` modifier) and the Python-side extraction (`datetime.fromtimestamp`), keeping them consistent.

A bucket with fewer than `MIN_OBS = 30` observations produces `value = None` for all variables, which the scoring engine treats as skipped. This threshold guards against garbage forecasts from a bucket that has barely been observed — 30 samples represents roughly one reading per day over a month.

## Variables Forecast

| Variable    | Unit | Source column      |
|-------------|------|--------------------|
| temperature | °C   | air_temp           |
| humidity    | %    | relative_humidity  |
| pressure    | mb   | station_pressure   |
| wind_speed  | m/s  | wind_avg           |

## Lead Times

6, 12, 18, 24 hours from the most recent Tempest observation.

## Historical Background

Climatological forecasting is the oldest form of numerical guidance. Before numerical weather prediction, forecasters relied on the historical record: if the average April temperature at 15:00 was 12°C, forecast 12°C. The WMO defines official climate normals as 30-year averages computed over a fixed period (currently 1991–2020), updated every decade. National agencies like NCEI publish these for official ASOS stations.

This model does not use official NCEI normals. Those represent KMSP — a large airport with concrete and jet exhaust, eight miles from the Tempest station. Instead the model computes its own normals from the local Tempest record. The trade-off is that NCEI normals are immediately available and span 30 years, while local normals take years to become truly representative. For now the model is more accurately described as a diurnal average from recent history than a climatological normal in the WMO sense.

## Data Requirements

The wxlog database must contain Tempest observations in the (month, hour) bucket for the target lead time. At least `MIN_OBS = 30` observations are required per bucket before the model produces a value.

As of April 2026, only March and April 2026 are populated (one year, not 30). March buckets have approximately 228 observations each; April buckets have 120–132. May through February will return `value = None` until the station has observed those months. The model's skill will improve gradually as the local historical record grows; it will not be a true climatological normal until several years have accumulated.

## Pros and Cons

### Pros

- No external APIs, rate limits, or registration required
- Improves automatically as the local station accumulates history
- Captures the diurnal cycle and seasonal baseline that persistence ignores
- Likely to outperform persistence on slowly-varying variables (pressure, humidity) at longer lead times once sufficient data accumulates

### Cons

- Currently a single-year diurnal average, not a 30-year climatological normal; the name carries an asterisk until multi-year data exists
- No synoptic-scale skill — cannot anticipate fronts, storm systems, or anomalous patterns
- Seasonal gaps: months the station has not yet observed produce null forecasts indefinitely
- Cannot forecast variables not measured by the Tempest (dewpoint, sky cover, sea-level pressure)

## Failure Modes

- **Thin bucket** (`n < MIN_OBS`): returns `value = None`; forecast row is inserted but scored as skipped
- **Unseen month**: any month with zero historical observations returns empty means; all variables null
- **Station gap or outage**: if the station was offline during part of a month, that hour's bucket may be abnormally thin or biased toward the hours when it was online
- **Single-year bias**: an anomalous month (e.g., an unusually warm April) will bias the bucket until additional years dilute it

## Implementation Notes

The bucket query runs against the read-only input database (`wxlog-read-only.db`) using `db.climo_bucket_means()`. Results are not cached between lead times within a single run — four separate queries are issued per forecast run. At current data volumes this is negligible; if the database grows very large, caching all (month, hour) means at the start of `run()` would be a straightforward optimization.

See `migrations/003_climatological_mean.sql` for the model registration and `db.py` (`climo_bucket_means`) for the query.
