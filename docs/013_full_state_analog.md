# full_state_analog (model 13)

Extends the analog model to use all available Tempest sensor variables in the similarity
distance metric. Different members specialize in different atmospheric signatures by
selecting feature subsets, while full-sensor members use all 10 variables.

## Motivation

The analog model (model 8) uses only 4 features — temperature, dewpoint, pressure, and
wind speed — leaving solar radiation, UV index, wind gust, wind direction, precipitation
accumulation, and lightning counts unused. These variables carry real atmospheric
information: solar radiation indicates cloud cover and daytime heating, wind direction
reveals synoptic flow, lightning indicates convective activity. The full_state_analog
uses all of them.

The key structural difference from the analog model is that members vary by **which
features define similarity**, not just K. This allows the ensemble to capture different
aspects of the current state: a convective-focused member may find better analogs when
lightning is present, while a synoptic member homes in on pressure-flow patterns.

## Algorithm

1. **Candidate selection**: Same time-of-day-aligned historical pool as the analog model
   — one observation per historical calendar day, closest to the current local
   time-of-day, up to 365 days back (`db.full_analog_candidates`).
2. **Similarity**: Weighted Euclidean distance in sigma-normalized feature space. Each
   member uses its own feature subset, and sigmas are computed from the candidate pool
   scoped to that subset.
3. **Wind direction**: Uses arc distance (shortest circular path) rather than signed
   difference. Sigma is fixed at 90° (one compass quadrant) rather than computed from
   the pool, because circular statistics don't map cleanly onto z-score normalization.
4. **Analog selection, forecasting, precip_prob, member_id=0**: Identical to model 8 —
   see `docs/008_analog.md` for details on K-nearest selection, inverse-distance
   weighting, and precip fraction logic.

## Features

| Feature | Type | Notes |
|---------|------|-------|
| `air_temp` | continuous | °C |
| `dew_point` | continuous | °C |
| `station_pressure` | continuous | hPa |
| `wind_avg` | continuous | m/s |
| `wind_direction` | circular | arc distance; fixed sigma = 90° |
| `wind_gust` | continuous | m/s |
| `solar_radiation` | continuous | W/m² |
| `uv_index` | continuous | correlated with solar_radiation |
| `precip_accum_day` | continuous | mm since midnight |
| `lightning_count` | continuous | sparse; non-zero = convective signal |

## Members

| ID | Name | K | Features | Notes |
|----|------|---|----------|-------|
| 0 | ensemble mean | — | — | inverse-MAE weighted mean + spread across members 1–8 |
| 1 | full-k5 | 5 | all 10 | equal weights |
| 2 | full-k10 | 10 | all 10 | more stable as data accumulates |
| 3 | thermo-wind | 5 | temp, dp, pressure, wind_avg, wind_dir | thermal + kinematic state |
| 4 | solar-thermo | 5 | temp, dp, solar, uv | radiation-driven thermal signature |
| 5 | synoptic | 5 | pressure, wind_avg, wind_dir | flow pattern only |
| 6 | precip-signal | 5 | dp, precip_accum, lightning | moisture + convective activity |
| 7 | full-seasonal | 5 | all 10 | penalizes analogs from distant calendar months (×1+0.2×month_diff) |
| 8 | full-dist-weighted | 10 | all 10 | inverse-distance-weighted final value |

## Notes

- Data-starved early: starts with ~43 candidates and improves as history accumulates.
- Members that include correlated features (solar + uv, wind_avg + wind_gust) accept
  some redundancy in exchange for reinforcing those signals.
- When fewer candidates exist than K, all available analogs are used.
