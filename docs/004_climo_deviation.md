# 004 Climatological Mean with Deviation

## Overview
This model uses the weighted climatological mean (model 003) as a baseline reference and augments it with the current anomaly from the norm. The idea is that if the current conditions deviate from what climatology says is normal for this time of year and day, then carry that deviation forward into all future forecast periods. In this model, we carry the deviation forward as both a naïve static value and as exponentially decaying values at several decay rates. The decaying members come from an idea that the current anomaly matters less further into the future, but this also bakes in the dubious idea that the weather will trend toward the historical norm.

For example, if climatology says the current hour should average 50°F but it's actually 60°F, the model adds +10°F to every future lead-time climatological value. If the 6-hour climatological mean is 45°F, this model forecasts 55°F (static); if the 12-hour mean is 40°F, this model forecasts 50°F (static). Decay members fade the anomaly toward zero as lead time increases.

## Method
The deviation is computed once at issue time per member: `deviation = current_obs - climo_now`, where `climo_now` is the weighted climatological mean for the current (month, hour) bucket using that member's weight function.

For each lead time `h`:
- **Static**: `forecast = future_baseline + deviation`
- **Decay**: `forecast = future_baseline + deviation * exp(-k * h)`

## Expected Behavior
At short lead times, this model should be more skillful than the raw climatological mean in anomalous conditions, because it acknowledges that the current state is different from average. At longer lead times, the static assumption becomes increasingly unrealistic; decay members converge back toward the climatological baseline.

Comparing member groups against each other reveals how quickly carrying the anomaly forward becomes a liability versus an asset, and which decay rate best matches observed anomaly persistence in this dataset.

## Members

9 base weighting hypotheses (same as model 003) × 6 deviation groups = 54 members. Member_id=0 is the skill-score weighted mean of all 54 members (equal-weighted when no scoring history exists).

| member_id range | group  |
|-----------------|--------|
| 1–9             | static |
| 10–18           | k=0.03 |
| 19–27           | k=0.05 |
| 28–36           | k=0.10 |
| 37–45           | a03 (amplifying, beta=0.30) |
| 46–54           | a06 (amplifying, beta=0.60) |

The a03/a06 groups were added after the original three decay rates all carried the
anomaly toward zero and none tested whether it should instead grow — see
`migrations/014_climo_deviation_v2.sql` and `models/climo_deviation.py`'s `_GROUPS`.

### Deviation Groups

Decay groups fade the anomaly toward zero as lead time increases (formula and % below).
Amplifying groups instead scale the anomaly by a diurnal factor that peaks at 13:00 local
(`1 + beta * sin(pi * (valid_hour - 6) / 14)` for valid hours 06:00–20:00, else 1.0) —
they never decay with lead time, only with time of day.

| Group  | Formula                       | Deviation at +6h | Deviation at +12h | Deviation at +24h |
|--------|-------------------------------|------------------|-------------------|-------------------|
| static | `baseline + dev`              | 100%             | 100%              | 100%              |
| k=0.03 | `baseline + dev * e^(-0.03h)` | 84%              | 70%               | 49%               |
| k=0.05 | `baseline + dev * e^(-0.05h)` | 74%              | 55%               | 30%               |
| k=0.10 | `baseline + dev * e^(-0.10h)` | 55%              | 30%               | 9%                |
| a03    | `baseline + dev * (1 + 0.30*diurnal)` | 100–130% of dev, peaking at 13:00 local | same | same |
| a06    | `baseline + dev * (1 + 0.60*diurnal)` | 100–160% of dev, peaking at 13:00 local | same | same |

## Confidence

Every member here gets a confidence value computed against the shared default
fingerprint (`air_temp`, `dew_point`, `station_pressure`, `wind_avg`), matched
against its own scored history by calendar day. member_id=0's combination now
multiplies each member's weight by its own confidence (floored, defaulted to the
group's own average when unknown) via `models/_confidence.py`'s `combine_pattern`,
which also fixed a pre-existing bug: a member missing a weight used to collapse the
whole group to a plain average, now only that member is dropped. See
[confidence.md](confidence.md) for the full design.
