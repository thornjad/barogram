# solar_ramp (model 23)

Forecasts temperature directly off how fast solar_radiation and the dewpoint
depression are moving right now, instead of off pressure or a fixed diurnal curve.
Forecasts temperature only.

## Motivation

The 2026-09-16 09:00 run: pressure_trend_cascade and wind_veer_detector both predicted
a temp drop that morning because their transfer functions and classifiers pool history
without regard to solar heating in progress (see those models' member 5/6 and 4/5
additions, same date). airmass_diurnal already has an extensive clearness-index member
family (9-11, 13, 15-16 — see [007_airmass_diurnal.md](007_airmass_diurnal.md)) that
scales a climatological diurnal curve by today's solar clearness, but it still missed
this run by a wide margin (-8.8°C at 4h lead). The gap isn't "no solar awareness
anywhere in the ensemble" — it's specifically that no member regresses temperature's
own recent rate directly against the rate solar/dewpoint signals are moving, the same
kind of fixed-lag-free regression pressure_tendency does for pressure.

## Members

| member_id | name | predictor |
|-----------|------|-----------|
| 0 | — | weighted mean of 1-2 |
| 1 | solar_temp_transfer | trailing-1h delta in solar_radiation, transfer function conditioned by time-of-day sector |
| 2 | dewpoint_depression_ramp | trailing-1h delta in dewpoint depression (temp - dewpoint), pooled transfer function |

## Algorithm

Both members are transfer functions in the pressure_tendency mold: regress
temperature's own delta over `[t, t+lead]` against a predictor's trailing-1h delta,
refit from full history on every run. member 1 is sector-conditioned for the same
reason pressure_trend_cascade's `sector_conditioned_extrap` is — a solar ramp's
relationship to temperature differs by time of day, so pooling would blur the exact
signal this model exists to isolate. member 2 is pooled: a widening dewpoint
depression is inherently a daytime-heating signal already, so it isn't confounded by
time-of-day the way a bare solar or pressure delta is.

## Confidence

Same graceful cold-start as every newly added model — confidence starts at the
neutral fallback and member_id=0's combination stays identical to an equal-weight mean
until it accumulates enough scored history of its own. See
[confidence.md](confidence.md) for the full design.
