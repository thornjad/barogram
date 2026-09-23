# radiational_cooling (model 28)

The textbook clear-plus-calm-plus-dry setup for strong overnight cooling, built as
one explicit small model instead of left implicit and scattered across several
others (`airmass_diurnal`'s clearness scaling, `dry_airmass_diurnal`'s
dewpoint-depression age, `surface_signs`' cloud/wind/dewpoint categories taken
individually). Night-only by design — during the day the mechanism this model
reasons about doesn't apply, so every member abstains. Forecasts temperature and
dewpoint.

## Motivation

From the 2026-09-22 "Synoptic Signal Ideas" brainstorm, radiational_cooling
section. Clear skies, calm wind, and dry air each individually favor stronger
overnight cooling, but nothing in the existing roster combines the three into one
joint trigger the way a forecaster actually reasons about the setup.

## Members

| member_id | name | predictor |
|-----------|------|-----------|
| 0 | — | combine_pattern aggregate across whichever members produced a value |
| 1 | cooling_potential_index | joint (wind calm, cloud, dewpoint depression) trigger state |
| 2 | wind_lull_frequency | fraction of trailing-window readings with `wind_lull` at/below the calm floor |
| 3 | metro_heat_retention_correction | learned residual: actual cooling rate vs member 1's own predicted rate |

## Algorithm

Night is defined the same way `surface_signs` defines it: `solar_radiation` at or
below a small floor (5 W/m²), or missing. Every member below abstains (`None`)
outside that window, live and during history-building alike — the model's whole
premise is the overnight radiational-loss mechanism, which doesn't apply in
daylight.

**Member 1, cooling_potential_index.** Three categorical reads, combined into one
joint state:

- **wind**: mean `wind_avg` over a trailing 3h window, bucketed `calm` (< 2 m/s),
  `windy` (> 5 m/s), or `moderate`.
- **cloud**: `surface_signs`' own `_solar_cloud_category` (solar-radiation deficit
  vs climatological mean for this month/hour), but *persisted forward* — a night
  observation borrows the most recent daytime cloud read within a 15h lookback,
  since cloud cover more often than not persists across sunset. Without this, the
  cloud read would be `None` all night, since solar radiation itself is `None` at
  night.
- **dewpoint depression**: `air_temp - dew_point`, bucketed `moist` (< 3°C), `dry`
  (> 8°C), or `moderate`.

All three must resolve for the joint state to fire. History is walked once
(night timestamps only) to learn a conditional mean delta per (joint state,
column, lead) — the standard bucket/average-future-delta pattern every
categorical model in this ensemble uses — and the live joint state looks up its
own learned delta at forecast time.

**Member 2, wind_lull_frequency.** `wind_lull` (Tempest's minimum wind speed
within the reporting interval) isn't read by name anywhere else in barogram yet.
Rather than member 1's single trailing average, this counts the *fraction* of
readings in the same trailing 3h window where `wind_lull` sits at or below 0.5
m/s, bucketed `frequent` (> 0.6), `rare` (< 0.2), or `occasional`. Frequent lulls
suggest a decoupled, stable boundary layer — the classic setup member 1 also
reads for, just from a frequency instead of an average. Rare lulls suggest
mechanical mixing overriding the local diurnal cycle even when the average wind
speed itself looks calm. Its own conditional-delta table is learned and looked up
the same way as member 1's, independently.

**Member 3, metro_heat_retention_correction.** Not an independent signal — a
learned bias correction on top of member 1. For every night timestamp in
history, member 1's own transfer function predicts a delta for that timestamp's
joint state; the residual (actual observed delta minus that prediction) is
accumulated per (column, lead) and averaged. At forecast time, member 3's value
is member 1's live prediction plus that standing residual. The hypothesis this
tests: Twin Cities metro proximity keeps this station's overnight lows a touch
warmer than a rural station under an identical clear/calm/dry setup would run,
and that gap should show up as a small negative residual on cooling delta results
that's fairly stable lead-to-lead rather than something worth re-deriving per
joint state.

## `wind_lull`, `relative_humidity`, `precip` availability

`db.py`'s `tempest_obs_in_range` (the query behind the shared `all_obs` kwarg
every `NEEDS_ALL_OBS` model reads) was widened to add `wind_lull` for this model
— member 2 needs it by name and it wasn't previously selected there (see the
thornlog `barogram-model-analysis` message board, 2026-09-22, for the earlier gap
and why `regime_stability` chose to scope around it instead of widening this same
query: that model didn't have a named, load-bearing need for the missing
columns the way this one does for `wind_lull`). `relative_humidity` and `precip`
were already added to that query by an earlier session for an unrelated member;
this model doesn't read either.

## Confidence

Same graceful cold-start as every newly added model — confidence starts at the
neutral fallback and stays there until enough scored history accumulates. See
[confidence.md](confidence.md) for the full design.

## Self-correction

Member 4 (`self_correction`) is the standard self-correction member (migration
`065_batch_b_self_correction_members.sql`) — member_id=0 minus this model's own
learned historical bias at each (variable, lead_hours) cell. See
[self_correction.md](self_correction.md).
