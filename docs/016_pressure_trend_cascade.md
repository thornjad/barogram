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
- **damped_extrap** — 3h tendency rate decayed toward zero over the lead
  (`delta = rate0 * (1 - exp(-λ*lead)) / λ`, λ=0.15/h), rather than extrapolating the
  raw polynomial
- **fast_damped_extrap** (added 2026-09-14) — same formula as damped_extrap, but
  λ=0.35/h (~2h half-life vs ~4.6h). damped_extrap scored best of all internal
  models on dewpoint during the 2026-09-12 dry-airmass intrusion but still lagged
  the actual crash; this member tests whether decaying the rate faster tracks rapid
  sub-6h transitions better, at the cost of overreacting to noise on slower days

These reuse pressure_tendency's polynomial-fit and mean-reversion functions directly
— the numerics are the same, only what feeds the transfer function differs.

### Retired member (2026-09-18)

ID 2 (`quad_extrap`, degree-2 fit over a 6h window) was consistently among the worst
performers in the whole barogram roster — a quadratic fit over a 6h window overshoots
on extrapolation regardless of how much history accumulates, a structural mismatch
rather than a data-maturity gap. Historical forecast rows and its `members`-table entry
are kept; it just no longer runs. Full writeup: thornlog message board
"barogram-model-analysis".

- **sector_conditioned_extrap** (added 2026-09-17) — reuses fast_damped_extrap's own
  pressure extrapolation unchanged, but applies a transfer function trained separately
  per time-of-day sector instead of the pooled one
- **solar_gated_extrap** (added 2026-09-17) — same pressure extrapolation and the same
  pooled transfer function as fast_damped_extrap, but abstains on temperature/dewpoint
  entirely while solar_radiation is climbing more than 1.0 W/m²/min (an active heating
  ramp), instead of applying a transfer function trained mostly on non-solar cases

### Root cause for members 5-6

fast_damped_extrap carried ensemble weight up to 0.84 on the 2026-09-16 09:00 run and
forecast a temp drop while pressure rose smoothly all morning under strong clear-sky
solar heating. `_build_delta_transfer_fns` pools every rising-pressure case in history
into one regression, dominated by post-frontal cooling — it has no way to distinguish
that from a high building under a warming sun. Both new members target this gap without
touching members 1-4's own extrapolation.

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
