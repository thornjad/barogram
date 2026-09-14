# pressure_trend_cascade (model 16)

Fork of [pressure_tendency](005_pressure_tendency.md) that predicts pressure first, then
uses its own predicted pressure — not the instantaneous tendency rate — to predict
temperature and dewpoint.

## Motivation

pressure_tendency's transfer function maps the 3h tendency *rate* at t=0 to an expected
variable delta. That rate is a proxy for what's actually going to happen to pressure over
the forecast horizon, but it isn't the same thing: a steep rate right now can decay well
before a 24h lead, and a quadratic fit's curvature isn't captured by the derivative alone.
This model closes that gap: extrapolate pressure to the actual lead first, then transfer
from *that* predicted total delta.

## Members

- **linear_extrap** — degree-1 fit over a 3h window, OU mean-reverted extrapolation
  (same reversion as pressure_tendency, λ=0.10/h)
- **quad_extrap** — degree-2 fit over a 6h window, same mean reversion
- **damped_extrap** — 3h tendency rate decayed toward zero over the lead
  (`delta = rate0 * (1 - exp(-λ*lead)) / λ`, λ=0.15/h), rather than extrapolating the
  raw polynomial
- **fast_damped_extrap** (added 2026-09-14) — same formula as damped_extrap, but
  λ=0.35/h (~2h half-life vs ~4.6h). damped_extrap scored best of all internal
  models on dewpoint during the 2026-09-12 dry-airmass intrusion but still lagged
  the actual crash; this member tests whether decaying the rate faster tracks rapid
  sub-6h transitions better, at the cost of overreacting to noise on slower days

All four reuse pressure_tendency's polynomial-fit and mean-reversion functions directly
— the numerics are the same, only what feeds the transfer function differs.

## Transfer functions

Trained on total observed pressure delta over `[t, t+lead]` versus each variable's own
delta over the same window (OLS, refit from full history on every run) — as opposed to
pressure_tendency's fixed 3h backward rate as predictor.

## member_id=0

Weighted mean (skill-score weights when available, else equal) + spread across the three
members, per variable and lead.

## Confidence

Every member here gets a confidence value computed against the shared default
fingerprint (`air_temp`, `dew_point`, `station_pressure`, `wind_avg`), matched
against its own scored history by calendar day. member_id=0's combination now
multiplies each member's weight by its own confidence (floored, defaulted to the
group's own average when unknown) via `models/_confidence.py`'s `combine_pattern`,
which also fixed a pre-existing bug: a member missing a weight used to collapse the
whole group to a plain average, now only that member is dropped. See
[confidence.md](confidence.md) for the full design.
