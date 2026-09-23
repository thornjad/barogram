# regime_stability (model 24)

Classifies current conditions into a calm/transitional/active meta-state from the
rolled-up variance across every Tempest sensor over a trailing window, then looks
up the learned conditional-mean delta for that state. Same
bucket-history/average-future-delta/look-it-up-live pattern as `wind_veer_detector`
and `frontal_trigger`. Forecasts temperature, dewpoint, and pressure.

## Motivation

From the 2026-09-22 "Synoptic Signal Ideas" brainstorm: every other model reads a
specific physical signal (pressure tendency, wind veer, solar ramp, ...) and learns
a conditional delta from it. None of them ask a simpler question first — is the
station currently sitting in a calm, boring, low-volatility regime, or an active,
fast-changing one? The hypothesis is that knowing the regime alone beats any single
physical signal at short range, since a calm regime implies high persistence
confidence regardless of which direction any one sensor happens to be pointing.

Deliberately starts as a single member, the same narrow-then-expand discipline used
throughout that brainstorm. If it earns weight during `tune`, it grows a family; if
it doesn't, it's a cheap experiment to have run.

## Members

| member_id | name | predictor |
|-----------|------|-----------|
| 0 | — | (single-member passthrough) |
| 1 | regime_meta_state | volatility index bucketed into calm / transitional / active |

## Algorithm

For every raw Tempest sensor column present in the shared `all_obs` row shape
(`db.py`'s `tempest_obs_in_range`) with enough history — `air_temp`, `dew_point`,
`station_pressure`, `wind_avg`, `wind_gust`, `solar_radiation`, `uv_index`,
`lightning_count` — compute the ratio of this moment's trailing-3h stdev to that
column's own long-run (population) stdev. Average the available ratios into a
single volatility index: roughly 1.0 means "as variable as usual right now," below
1 calmer than usual, above 1 more active than usual.

`relative_humidity`, `wind_lull`, and raw `precip` aren't selected by
`tempest_obs_in_range` (only `precip_accum_day` is), so they aren't available to
this model the way they are to `models/_confidence.py`'s own separate snapshot
queries. Of what is available, `wind_direction` is excluded (circular, plain stdev
doesn't apply — same reasoning as `models/_similarity.py`'s own `_CIRCULAR`
exclusion), and `precip_accum_day` is excluded (resets at local midnight, so a
trailing window straddling that reset reads as a fake variance spike — the same
reasoning that made the 2026-09-16 confidence-fingerprint decision prefer raw
`precip` over `precip_accum_day` for its own trend signal; here there's no raw
`precip` column to fall back to, so it's dropped rather than substituted).

The index is bucketed with two fixed thresholds: below 0.5 is `calm`, above 1.5 is
`active`, everything else is `transitional`. History is then walked once to build a
conditional mean delta per (category, column, lead) — the standard bucket/average
pattern every categorical model in this ensemble uses — and the live category looks
up its own learned delta at forecast time.

## Confidence

Same graceful cold-start as every newly added model — confidence starts at the
neutral fallback and stays there until enough scored history accumulates. See
[confidence.md](confidence.md) for the full design.

## Self-correction

Member 2 (`self_correction`) is the standard self-correction member (migration
`065_batch_b_self_correction_members.sql`) — member_id=0 minus this model's own
learned historical bias at each (variable, lead_hours) cell. See
[self_correction.md](self_correction.md).
