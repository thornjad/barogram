# moisture_trajectory (model 29)

Every existing model reads dewpoint; almost none read `relative_humidity` as its
own dimension, and nothing tracks the moisture picture's *rate of change*, only
its level. Each member here reads a different moisture-trend signal and forecasts
temperature and/or dewpoint from it. Forecasts temperature and dewpoint.

## Motivation

From the shared "Synoptic Signal Ideas" model-ideas brainstorm, moisture_trajectory
section (Part A).

## Members

| member_id | name | predictor |
|-----------|------|-----------|
| 0 | — | weighted/equal mean of members 1-5 |
| 1 | rh_dd_divergence | relative_humidity trend vs dewpoint-depression trend over the same 2h window |
| 2 | moisture_convergence | dew_point trend plus wind_direction steadiness |
| 3 | dd_closing_rate | rate of change of the dewpoint depression itself, not its level |
| 4 | saturation_countdown | degree-1 extrapolation of the depression, floored at zero; temperature only, calm-wind gated |
| 5 | delta_t_trend | rate of change of delta_t (wet-bulb depression, partial coverage) |

## Algorithm

**Member 1 (rh_dd_divergence).** Compares `relative_humidity` now against 2h ago
with the dewpoint depression (`air_temp - dew_point`) over the same window. RH
climbing (≥5 points) while the depression stays flat (<0.5°C change) is
`divergent_rh_rise` — the fog/frost-setup signature that depression alone can
miss when it looks unremarkable. RH climbing alongside a meaningfully narrowing
depression is `concurrent_moistening`; RH falling is `drying`; anything else is
`steady`.

**Member 2 (moisture_convergence).** Compares `dew_point` now against 3h ago.
Rising ≥0.3°C with `wind_veer_detector`'s rotation category reading `steady`
is `advection_moistening` (moist air being brought in); rising with the wind
veering or backing is `local_moistening` (evapotranspiration/drainage rather
than advection). Falling is `drying`. When the wind-rotation read is
unavailable, a rising dewpoint abstains (`None`) rather than guessing which
cause it is.

**Member 3 (dd_closing_rate).** Same depression as member 1, but reads its own
3h *rate* of change rather than its level (`dry_airmass_diurnal` already scales
the diurnal amplitude on the level). Closing faster than 1.5°C/3h is
`closing_fast`, faster than 0.5°C/3h is `closing_slow`, widening by 0.5°C/3h or
more is `widening`, otherwise `steady`.

**Member 4 (saturation_countdown).** The only non-bucket member: a degree-1 fit
of the depression itself over a trailing 2h window (`models.pressure_tendency`'s
own `_poly_fit`/`_poly_eval`), extrapolated to each lead hour and floored at
zero — temperature can't fall below the dewpoint. Forecasts temperature only,
as `dew_point + max(0, depression_extrapolated)`. Gated to calm wind
(`wind_avg` ≤ 1.5 m/s, the same floor `surface_signs`/`synoptic_state_machine`
use elsewhere): Tempest has no cloud-cover column, so the "clear" half of
"clear/calm only" isn't directly checkable here, and calm wind is the closest
available proxy. Abstains entirely (all leads `None`) without it.

**Member 5 (delta_t_trend).** Reads `delta_t` (wet-bulb depression) via
`db.tempest_delta_t_in_range`, a query kept separate from the shared
`tempest_obs_in_range` all_obs blob because `delta_t` is station-derived with
only partial firmware coverage — widening the shared query for every
`NEEDS_ALL_OBS` model over a column most of them can't use wasn't worth it.
Rising ≥0.5°C over 3h is `drying_accelerating`, falling is
`drying_decelerating`, otherwise `steady`. Partial coverage means this member
abstains far more often than the others, both live and when building its own
conditional-delta history — it seeds a signal rather than backfilling one
cleanly yet.

Members 1, 2, 3, and 5 each walk history once to build a conditional mean
future delta per (category, column, lead) — the standard bucket/average
pattern every categorical model in this ensemble uses, restricted here to
`air_temp` and `dew_point` (no `station_pressure` — this model has no
pressure hypothesis).

## Confidence

Same graceful cold-start as every newly added model — confidence starts at the
neutral fallback and stays there until enough scored history accumulates. See
[confidence.md](confidence.md) for the full design.

## Self-correction

Member 6 (`self_correction`) is the standard self-correction member (migration
`065_batch_b_self_correction_members.sql`) — member_id=0 minus this model's own
learned historical bias at each (variable, lead_hours) cell. See
[self_correction.md](self_correction.md).
