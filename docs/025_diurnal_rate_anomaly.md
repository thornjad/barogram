# diurnal_rate_anomaly (model 25)

Is a signal changing faster or slower than its own climatological rate/level for
this specific (month, hour) right now, rather than whether its current level is
high or low (`climo_deviation`'s domain). One mechanism, two variables: z-score
the live signal against its own (month, hour) bucket's historical mean and
stdev, bucket the z-score into above/normal/below, then look up the learned
conditional-mean delta for that category. Same bucket-history/average-future-
delta/look-it-up-live pattern as `wind_veer_detector`, `frontal_trigger`, and
`regime_stability`. Forecasts temperature, dewpoint, and pressure.

## Motivation

From the 2026-09-22 "Synoptic Signal Ideas" brainstorm, diurnal_rate_anomaly
section. `solar_ramp` reads temperature's raw trend; `climo_deviation` reads its
level anomaly. Neither asks whether temperature is warming or cooling faster or
slower than normal *for this specific hour* — the derivative of the diurnal
curve rather than just its level. Anomalously windy at an hour that's normally
calm is a related but distinct signal: it usually means synoptic-scale forcing
is overriding the local diurnal wind cycle, a useful trigger independent of
which direction any one variable happens to be moving.

## Members

| member_id | name | predictor |
|-----------|------|-----------|
| 0 | — | weighted/equal mean of members 1-2 |
| 1 | temp_slope_anomaly | air_temp's trailing-1h rate of change, z-scored against the historical rate at this (month, hour) |
| 2 | wind_rate_anomaly | wind_avg's current level, z-scored against its own (month, hour) climatology |

## Algorithm

For member 1, compute air_temp's trailing-1h rate of change (degrees/hour)
ending at the current observation. Walk the full observation history once to
bucket that same trailing-1h rate by (month, hour) of the observation's own
local time, and take each bucket's mean and (population) stdev. For member 2,
bucket wind_avg's raw level the same way instead of a rate.

Both members then z-score their live value against its own bucket:
`z = (value - bucket_mean) / bucket_stdev`. A bucket with fewer than 3 samples
or zero variance is dropped (too little history, or nothing to z-score
against), and z is `None` when either input is missing. z is bucketed into
`above` (z > 0.75), `below` (z < -0.75), or `normal` otherwise.

History is then walked once more to build a conditional mean delta per (member,
category, column, lead) — the standard bucket/average pattern every categorical
model in this ensemble uses — and each member's live category looks up its own
learned delta at forecast time, added to the current observation's value for
that variable.

## Confidence

Same graceful cold-start as every newly added model — confidence starts at the
neutral fallback and stays there until enough scored history accumulates. See
[confidence.md](confidence.md) for the full design.

## Self-correction

Member 3 (`self_correction`) is the standard self-correction member (migration
`065_batch_b_self_correction_members.sql`) — member_id=0 minus this model's own
learned historical bias at each (variable, lead_hours) cell. See
[self_correction.md](self_correction.md).
