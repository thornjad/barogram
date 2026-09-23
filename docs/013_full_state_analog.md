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
4. **Analog selection, forecasting, member_id=0**: Identical to model 8 — see `docs/008_analog.md` for details on K-nearest selection and inverse-distance weighting.
5. **Pool-variant members (14-17)**: Instead of varying the feature subset within the
   same whole-history candidate pool, these restrict or reshape the pool itself before
   the same K-nearest/mean-forecast machinery runs — a hard seasonal window, a
   pressure-regime filter, or matching on trend deltas (`models/_confidence.py`'s
   `compute_trends`) instead of instantaneous snapshots. See the members table below.

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
| 0 | ensemble mean | — | — | inverse-MAE weighted mean + spread across members 1–17 |
| 1 | full-k5 | 5 | all 10 | equal weights |
| 2 | full-k10 | 10 | all 10 | more stable as data accumulates |
| 3 | thermo-wind | 5 | temp, dp, pressure, wind_avg, wind_dir | thermal + kinematic state |
| 4 | solar-thermo | 5 | temp, dp, solar, uv | radiation-driven thermal signature |
| 5 | synoptic | 5 | pressure, wind_avg, wind_dir | flow pattern only |
| 6 | precip-signal | 5 | dp, precip_accum, lightning | moisture + convective activity |
| 7 | full-seasonal | 5 | all 10 | penalizes analogs from distant calendar months (×1+0.2×month_diff) |
| 8 | full-dist-weighted | 10 | all 10 | inverse-distance-weighted final value |
| 9 | full-k3 | 3 | all 10 | tightest K, most local |
| 10 | full-k15 | 15 | all 10 | |
| 11 | full-k20 | 20 | all 10 | |
| 12 | full-k35 | 35 | all 10 | |
| 13 | full-k50 | 50 | all 10 | widest K, most stable |
| 14 | seasonal-window | 15 | all 10 | candidate pool hard-restricted to ±21 calendar days across years, instead of full-seasonal's whole-year decay penalty |
| 15 | regime-gated | 15 | all 10 | candidate pool filtered to the current 3h pressure-trend regime (rising/falling/steady, same 0.5 hPa threshold as `synoptic_state_machine`) before ranking by distance; abstains when the current regime itself is unknown |
| 16 | trajectory-analog | 10 | all 10, each as its own 3h trend delta | matches on recent trajectory rather than instantaneous snapshot; promotes the trend-vector matching `models/_confidence.py` already computes for confidence into an actual forecasting member |
| 17 | full-fingerprint-lookup | 20 | full 34-feature snapshot+trend confidence fingerprint (`_confidence._DEFAULT_FEATURES` + `_TREND_FEATURES`) | reuses the confidence fingerprint as a forecast-state lookup rather than only a confidence-matching input; real curse-of-dimensionality risk against ~365 candidate days, cheap enough to be worth trying |
| 18 | self_correction | — | — | standard self-correction member, see [self_correction.md](self_correction.md) |

## Notes

- Data-starved early: starts with ~43 candidates and improves as history accumulates.
- Members that include correlated features (solar + uv, wind_avg + wind_gust) accept
  some redundancy in exchange for reinforcing those signals.
- When fewer candidates exist than K, all available analogs are used.

## Confidence

Each member's confidence is computed against the exact same analog days it already
selected for its own value forecast, not the shared default fingerprint every other
model uses. Combination is confidence-adjusted the same way as every other model,
via `models/_confidence.py`'s `combine_pattern`. See [confidence.md](confidence.md)
for the full design.
