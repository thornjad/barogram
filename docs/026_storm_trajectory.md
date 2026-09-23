# storm_trajectory (model 26)

Classifies an active convective (thunderstorm) event using actual lightning
distance, not just count, then looks up the learned conditional-mean delta for
that state. Same bucket-history/average-future-delta/look-it-up-live pattern
as `frontal_trigger` and `regime_stability`. Forecasts temperature, dewpoint,
and pressure.

`lightning_avg_distance` and the station-derived `lightning_strike_*` columns
are Tempest API fields in kilometers, not miles. All distance thresholds in
this model are km, converted from the mile figures in the brainstorm this
model came from.

## Motivation

From the 2026-09-18 "Synoptic Signal Ideas" brainstorm, storm_trajectory
section. Migration 004 (wxlog) closed the gap that made this speculative:
wxlog now captures actual lightning distance every row, plus a
station-derived `lightning_strike_last_epoch` / `lightning_strike_last_distance`
/ `lightning_strike_count_last_3hr` batch (~55% coverage, only the current
grid point at collection time). `synoptic_state_machine` already reads a
binary lightning-detected flag; nothing existing reads the closing/opening
distance trend.

## Members

| member_id | name | predictor |
|-----------|------|-----------|
| 0 | — | weighted/equal mean of members 1-5 |
| 1 | lightning_accel | lightning_count rate over a 30min window vs the previous 30min, cross-checked against lightning_strike_count_last_3hr's own 3h delta |
| 2 | precip_onset_lag | (distance bucket, elapsed bucket) since the current lightning event's first detection |
| 3 | storm_state | approaching / overhead / departing / near_miss, from pressure + lightning + distance + precip + wind veer trends jointly |
| 4 | precip_pressure_state | while actively raining: intensifying (pressure still falling) or past_peak (pressure recovering) |
| 5 | dry_lightning_flag | single dry_lightning_risk state: high lightning count at large average distance, flat precip_accum_day, low relative_humidity |

## Algorithm

An "event" is an unbroken run of `lightning_count > 0`, tolerating gaps up to
1h before it's considered over. `_event_start` walks backward from the
current observation through this run once, returning the run's start
timestamp, the earliest available distance reading in it, and the closest
distance seen — used by both member 2 (onset lag) and member 3's near-miss
check.

Member 1 buckets a fine 30min-vs-previous-30min lightning_count trend into
increasing/decreasing/steady, cross-checked against the coarser
`lightning_strike_count_last_3hr` delta where populated; disagreement between
the two falls back to steady rather than trusting either alone.

Member 2's state is (distance bucket at first detection, time elapsed since
then) — near/mid/far crossed with 0-30min/30-90min/90min+. Distance buckets:
near ≤8km (~5mi), mid ≤24km (~15mi), far beyond.

Member 3 classifies jointly from a 3h pressure trend, the lightning and
distance trends above, whether precip is currently or was recently active,
wind veer, and gust ratio, into approaching / overhead / departing /
near_miss — or `None` (abstain) when none of the four patterns match. See
`_storm_state`'s docstring-equivalent comment in `models/storm_trajectory.py`
for the exact conditions per state.

Member 4 only fires while precip is actively falling (raw `precip` reading
above zero, not the day-total `precip_accum_day` — see `regime_stability`'s
own reasoning for preferring raw precip for trend signals), using a 1h
pressure delta rather than the 3h window member 3 uses.

Member 5 requires all four conditions at once: at least 5 strikes summed over
a trailing 3h window, average distance in that window at least 24km, current
relative humidity at or below 40%, and less than 0.5mm change in
`precip_accum_day` over the window.

Every member's state, when present, feeds the standard bucket/average
conditional-delta table (state, column, lead) → mean future delta, the same
as every other categorical model here.

## Confidence

Same graceful cold-start as every newly added model — confidence starts at
the neutral fallback and stays there until enough scored history
accumulates. See [confidence.md](confidence.md) for the full design.

## Self-correction

Member 6 (`self_correction`) is the standard self-correction member (migration
`065_batch_b_self_correction_members.sql`) — member_id=0 minus this model's own
learned historical bias at each (variable, lead_hours) cell. See
[self_correction.md](self_correction.md).
