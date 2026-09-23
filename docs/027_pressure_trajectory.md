# pressure_trajectory (model 27)

Pressure is an intermediate signal here, not a forecast target the ensemble cares
about on its own — same philosophy as `pressure_tendency`, `pressure_trend_cascade`,
and `pressure_consensus_transfer`. A pressure signal gets classified (or, for
member 3, extrapolated), then a transfer function learned from the station's own
history maps that state to the expected temperature and dewpoint delta. Forecasts
temperature, dewpoint, and pressure.

## Motivation

From the 2026-09-22 "Synoptic Signal Ideas" brainstorm, pressure_trajectory
section. Every member below reads a different aspect of pressure's *trajectory* —
its second derivative, whether trends at different timescales agree, its recovery
shape after a frontal passage, and how long it's been since the last one — rather
than the single instantaneous rate `pressure_tendency` already reads or the total
predicted delta `pressure_trend_cascade` already extrapolates.

## Members

| member_id | name | predictor |
|-----------|------|-----------|
| 0 | — | weighted/equal mean of members 1-5, confidence-gated by member 2 |
| 1 | pressure_jerk | second derivative of station_pressure (this window's 3h tendency rate minus the prior 3h rate), bucketed into accelerating/decelerating fall or rise, or steady |
| 2 | trend_agreement | whether the 1h/6h/24h tendency rates agree in sign (agree_rising / agree_falling / diverging) |
| 3 | post_frontal_ringing | decaying return of pressure toward its pre-event baseline after a confirmed frontal passage; event-gated, abstains outside the ringing window |
| 4 | days_since_front | hours since the most recent pressure trough (any trough, unconfirmed), bucketed into an airmass-age category |
| 5 | front_phase_state | combines members 1, 2, 3, and 4 into pre_frontal / at_passage / post_frontal_recovery / quiescent |

## Semidiurnal tide removal

Every member reads `station_pressure` through `models._pressure_tide` before
computing any signal from it. That module learns this station's own small
twice-daily solar pressure tide from its long-run mean residual by hour of day
(real synoptic weather passes through every hour across a long history and
averages out; what's left is the systematic clock-time-locked component) and
subtracts it from the *inputs* to every categorization and trough search below.
Transfer-function targets — the actual forecast values, including member 3's own
predicted pressure — stay in real, tide-included space.

`pressure_tendency`'s own `_build_transfer_fns` and `_build_zambretti_conditionals`
also detide their pressure inputs the same way, for the same accuracy reason,
without changing what that model's regression members actually extrapolate.

## Algorithm

**Member 1 (pressure_jerk).** `_tendency_rate` computes an hPa/h rate over a
trailing window (3h by default) ending at a given timestamp, on detided pressure.
The jerk is that rate minus the rate one window further back. If the current rate
is falling beyond a steady threshold, the category is `accelerating_fall` when the
jerk itself is sufficiently negative, else `decelerating_fall`; symmetric for
rising. Otherwise `steady`.

**Member 2 (trend_agreement).** The same rate function evaluated at 1h, 6h, and
24h windows, each independently classified rising/falling/steady against a small
dead zone. All three agreeing on a non-steady direction gives `agree_rising` or
`agree_falling`; anything else is `diverging`.

**Members 3 and 4 (troughs).** `_detect_troughs` walks the full detided pressure
history once and marks a point as a trough when it's the local minimum within
±6h and at least 1 hPa below the nearest point on each side — a airmass-turnover
marker independent of cause. `days_since_front` (member 4) buckets the age since
the most recent trough at or before the current time into `at_trough` (<6h),
`recovering` (<24h), `aging` (<72h), or `stale`, with `None` when no trough has
ever been recorded. `post_frontal_ringing` (member 3) additionally requires the
most recent trough to be confirmed by a coincident wind veer or backing
(`models.wind_veer_detector._rotation_category`) within ±6h, and only within
12h of that trough — outside that window, or without a confirmed trough, it
abstains entirely. When active, it forecasts pressure decaying (`exp(-0.15 * lead_h)`,
the same e-folding family as `pressure_trend_cascade`'s damped members) from its
current value back toward the mean pressure over `[trough-24h, trough-12h]`, the
pre-event baseline. That predicted pressure (re-tided) is emitted directly for the
`pressure` variable; its predicted delta from the current observation feeds
`pressure_trend_cascade`'s own continuous transfer functions
(`_build_delta_transfer_fns`, reused rather than re-derived) for temperature and
dewpoint.

**Member 5 (front_phase_state).** Combines the live jerk category, agreement
category, ringing state, and age bucket: ringing active but not yet settling
(<3h past the trough) with disagreeing trends is `at_passage`; ringing settling,
or no ringing but a young airmass, is `post_frontal_recovery`; no ringing with an
accelerating fall is `pre_frontal`; no ringing, no front signature at all, and
steady/agreeing trends is `quiescent`. Any other combination abstains (`None`)
rather than forcing a state.

Members 1, 2, 4, and 5 each walk history once to build a conditional mean delta
per (category, column, lead) — the standard bucket/average pattern every
categorical model in this ensemble uses — including the `station_pressure` column
itself, the same as `frontal_trigger`, `wind_veer_detector`, `regime_stability`,
and `diurnal_rate_anomaly` already do.

## Confidence gate

Member 2's live category also gates this model's own ensemble row (member_id=0):
its group confidence is multiplied by 1.0 when the three trend windows agree, or
by 0.6 when they diverge — full trust in a confident continuation, damped trust
right at an inflection point the single-window members can miss. This is local to
this model's own row, not a cross-model architecture change.

## Confidence

Same graceful cold-start as every newly added model — confidence starts at the
neutral fallback and stays there until enough scored history accumulates. See
[confidence.md](confidence.md) for the full design.

## Self-correction

Member 6 (`self_correction`) is the standard self-correction member (migration
`065_batch_b_self_correction_members.sql`) — member_id=0 minus this model's own
learned historical bias at each (variable, lead_hours) cell. See
[self_correction.md](self_correction.md).
