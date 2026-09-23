# Model 6: diurnal_curve

## What it does

Fits a daily temperature cycle to recent station observations and projects it
forward to each lead time. The curve captures the solar-driven heating and
cooling pattern that repeats each day: temperatures rise after sunrise, peak in
early afternoon, and fall overnight toward a minimum near dawn.

Unlike the climatological mean family (models 2–4), which look up historical
means for a given hour of day, this model fits an explicit curve shape and can
project that shape forward from a known anchor point — the current observation
or the most recent overnight minimum.

Variables: **temperature, dewpoint**. Pressure is omitted; the
diurnal barometric tide (~0.5–1.5 hPa amplitude at mid-latitudes) is too small
relative to synoptic noise to be useful.

## Curve types

**Sine**: fits `y = A·sin(2πt/24) + B·cos(2πt/24) + C` to the hour-of-day
means using `np.linalg.lstsq`. Smooth, single-frequency, symmetric. Best when
the diurnal cycle is well-behaved and data is dense.

**Piecewise**: circular linear interpolation between populated hour-of-day mean
buckets. Non-parametric — follows the data shape exactly, including deviations
from a clean sinusoid. Smoother than asymmetric but more flexible than sine.

**Asymmetric**: two half-cosine segments connecting the trough and peak hours.
The warming half (trough → peak) and cooling half (peak → trough) have
independent durations, reflecting the physical reality that temperatures rise
faster after sunrise than they fall after the afternoon peak.

**Solar**: sinusoidal with phase derived from solar geometry (solar noon +
2 hours) rather than fitted from data. Amplitude and baseline come from 30 days
of observations. The phase is physically anchored rather than data-driven,
which may help when the diurnal cycle in recent data is distorted by weather.
Station latitude is read from the database at runtime.

## Anchor strategies

All curve types are combined with three anchoring strategies:

**current**: shifts the curve vertically so it passes through the current
observed value at the current time. Reduces systematic offset when conditions
differ from the historical mean.

**midnight**: shifts the curve to pass through the most recent observed
overnight value (nearest observation within ±1 hour of local midnight). Anchors
at a well-defined point on the cycle (near the temperature minimum).

**none**: uses the historical curve as-is, without adjustment. Equivalent to
a climatological projection of the diurnal shape.

## Lookback windows

| Label | Data range |
|-------|-----------|
| 7d    | Last 7 days |
| 14d   | Last 14 days |
| 30d   | Last 30 days |
| yr    | ±15 days around the same calendar date one year ago |

The year-ago window returns None until a full year of data has accumulated.
Once data exists, it provides same-season climatological context rather than
recent-weather context.

## Member inventory

41 numbered members + member_id=0 (weighted ensemble mean).

| Member IDs | Curve      | Lookbacks             | Anchors                  |
|------------|------------|-----------------------|--------------------------|
| 1–12       | sine       | 7d, 14d, 30d, yr      | current, midnight, none  |
| 13–24      | piecewise  | 7d, 14d, 30d, yr      | current, midnight, none  |
| 25–36      | asymmetric | 7d, 14d, 30d, yr      | current, midnight, none  |
| 37–39      | solar      | 30d (amp/base only)   | current, midnight, none  |
| 40         | range_scaled | 7d                  | current                  |
| 41         | wind_sector_conditioned | 30d        | current                  |

Member naming: `{curve}-{lookback}-{anchor}` (e.g. `sine-7d-current`,
`asymmetric-30d-none`, `solar-midnight`). Members 40 and 41 are named directly
(`range_scaled`, `wind_sector_conditioned`) since each is a single fixed
lookback/anchor combination rather than a member of a swept family.

## Range-scaled member (40)

Cloudy or windy days compress the day's temperature/dewpoint range; clear,
calm days expand it. This member forecasts that range directly, rather than
just the peak timing and shape the curve/anchor combinations above touch.

Baseline range is the trailing-24h actual high minus low (falls back to the
7d curve's own range if fewer than 20 trailing observations exist). That
baseline is scaled by a ratio comparing today's actual rate of change since
local midnight to the 7d curve's own rate over the same window: warming
faster than climatology expands the predicted range, slower compresses it.
The ratio is clamped to [0.3, 2.5] to keep a single unusual morning from
blowing out the whole day's forecast.

The 7d piecewise curve is then rescaled around its own mean by
`predicted_range / curve_range` and current-anchored (shifted to pass through
the live observation), the same anchoring the other current members use.

## Wind-sector-conditioned member (41)

One pooled (hour-of-day) curve assumes the day's shape doesn't depend on wind
sector, but at this latitude an NW day and a S day have genuinely different
diurnal shapes (winter cold-advection days vs summer humid-advection days).

This member fits the piecewise curve only against the last 30 days of
observations sharing the current prevailing 8-point wind sector (the sector
of the live wind_direction reading, same 8-point convention as
`airmass_diurnal`'s wind-sector members: 0=N, 1=NE, ... 7=NW), then
current-anchors it. Filtering by sector shrinks the effective sample size
roughly eightfold versus the pooled 30d curve, so this member returns None
far more often in its early weeks than the pooled members do.

member_id=0 is the skill-score weighted mean across all members with valid
forecasts for a given (variable, lead_hours) pair.

## Data requirements

- Minimum 3 observations per hour-of-day bucket for that bucket to count
- Minimum 12 of 24 hour buckets populated for a curve to be fitted
- If either threshold is not met, all members using that (lookback, variable)
  combination return None

The 7-day and 14-day lookbacks may fail during the first week of operation.
The 30-day lookback typically becomes reliable after ~2 weeks. Year-ago members
produce no data until approximately one year after the station comes online.

## Failure modes

- **Insufficient data** (first weeks of operation): short-lookback members
  return None; model still runs with fewer active members
- **Midnight obs unavailable**: all `-midnight` members return None for that
  run (no special handling needed)
- **Station location unavailable**: solar members (37–39) return None
- **Year-ago window empty**: members 10–12, 22–24, 34–36 return None
- **Sine fit near-singular**: `np.linalg.lstsq` handles via SVD; if
  coefficients are non-finite, sine members return None for that lookback
- **Fewer than 20 trailing-24h observations**: member 40 falls back to the 7d
  curve's own range instead of an actual trailing high/low
- **No live wind_direction, or too few 30d obs in the prevailing sector** (fewer
  than 3 obs in 12 of 24 hour buckets, the same threshold as every other
  member here): member 41 returns None

## Confidence

Every member here gets a confidence value computed against the shared default
fingerprint (`air_temp`, `dew_point`, `station_pressure`, `wind_avg`), matched
against its own scored history by calendar day. member_id=0's combination now
multiplies each member's weight by its own confidence (floored, defaulted to the
group's own average when unknown) via `models/_confidence.py`'s `combine_pattern`,
which also fixed a pre-existing bug: a member missing a weight used to collapse the
whole group to a plain average, now only that member is dropped. See
[confidence.md](confidence.md) for the full design.
