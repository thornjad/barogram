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

39 numbered members + member_id=0 (weighted ensemble mean).

| Member IDs | Curve      | Lookbacks             | Anchors                  |
|------------|------------|-----------------------|--------------------------|
| 1–12       | sine       | 7d, 14d, 30d, yr      | current, midnight, none  |
| 13–24      | piecewise  | 7d, 14d, 30d, yr      | current, midnight, none  |
| 25–36      | asymmetric | 7d, 14d, 30d, yr      | current, midnight, none  |
| 37–39      | solar      | 30d (amp/base only)   | current, midnight, none  |

Member naming: `{curve}-{lookback}-{anchor}` (e.g. `sine-7d-current`,
`asymmetric-30d-none`, `solar-midnight`).

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
