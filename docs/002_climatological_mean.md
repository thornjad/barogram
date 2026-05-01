# 002 Climatological Mean Model

## Overview

This model recognizes the cyclical nature of the seasons, and forecasts based on the historical average conditions for the calendar month and hour of the day. This is based on past Tempest observations, and so improves as more historical data is logged. The historical data includes the current year, which reveals this method to be, at least in a way, an iterative improvement over the persistence model (model 001). The persistence model is based on the current conditions only, and in the extreme situation where the only historical information available is the current conditions, the climatological model would hypothetically produce the same result (though we gate to at least 30 observations for this model).

## Method
For each lead time (+6h, +12h, +18h, +24h), the calendar month and hour are used to query all accumulated Tempest observations in that (month, hour) bucket, including the current year. The arithmetic mean of each continuous variable across those observations is the forecast value.

For `precip_prob`, the model computes the historical precipitation occurrence rate in the same (month, hour) bucket: each obs is flagged 1 if it shows a measurable precip delta (>0.1 mm from the previous obs on the same calendar day) and 0 otherwise; the mean of those flags is the probability.

Bucket matching uses local time for both the query (SQLite `'localtime'` modifier) and the Python-side extraction (`datetime.fromtimestamp`), keeping them consistent.

A bucket with fewer than `MIN_OBS = 30` observations produces `value = None` for all variables, which the scoring engine treats as skipped. This threshold guards against garbage forecasts from a bucket that has barely been observed — 30 samples represents roughly one reading per day over a month.

## Lead Times

6, 12, 18, 24 hours from the most recent Tempest observation.

## Historical Background
Climatological forecasting is the oldest form of numerical guidance. Before numerical weather prediction, but after we began recording historical weather conditions, forecasters relied on the historical record (not counting reading animal signs). If the average April temperature at 15:00 was 12°C, forecast 12°C. The WMO defines official climate normals as 30-year averages computed over a fixed period (currently 1991–2020), updated every decade. National agencies like NCEI publish these for official ASOS stations.

This particular model does not use official NCEI normals. Instead, for fun and education, the model computes its own normals from the locally-input Tempest record. The trade-off is that while NCEI normals span the true 30 years, local normals take years to become truly representative, and as I'm writing this, the main station I'm using has less than a full year. For now the model is more accurately described as a diurnal average from recent history than a climatological normal in the WMO sense.

## Pros and Cons

### Pros
- Captures the diurnal cycle and seasonal baseline that persistence ignores
- Likely to outperform persistence on slowly-varying variables (pressure, dewpoint) at longer lead times once sufficient data accumulates

### Cons
- Currently a single-year diurnal average, not a 30-year climatological normal
- No synoptic-scale skill, this cannot anticipate fronts, storm systems, or anomalous patterns
- Cannot forecast variables not measured by the Tempest (sky cover, sea-level pressure)
