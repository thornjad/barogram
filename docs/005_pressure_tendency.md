# pressure_tendency (model 5)

Uses the recent barometric pressure time series to forecast all four variables via
polynomial extrapolation and empirical transfer functions.

## Motivation

A physical barogram records pressure tendency over time — the rate and direction of
pressure change. This is one of the oldest and most reliable single-variable forecasting
signals. This model explores whether different ways of reading that signal (different
window lengths, polynomial degrees, and weighting schemes) produce meaningfully different
forecasts, and how they compare to a century-old rules-based approach (Zambretti).

## Pressure forecasting

For each regression member, a polynomial is fit to recent `(timestamp, station_pressure)`
observations. Time is centered relative to `issued_at` (t=0 = now, t=-1 = one hour ago)
for numerical stability. The polynomial is then extrapolated to each `valid_at` to
produce the pressure forecast.

Members differ in:
- **Window length**: how far back in time observations are drawn from (1h, 3h, or 6h)
- **Polynomial degree**: linear (degree 1) or quadratic (degree 2)
- **Weighting scheme**: uniform (all observations equally weighted) or exponentially
  decaying (more recent observations receive higher weight)

The exponential decay weighting uses the form `w = exp(ln(2)/hl * t)` where t is the
centered time in hours and hl is the half-life. At t=0 (current time), w=1; at t=-hl,
w=0.5.

## Transfer functions for other variables

For temperature, dew point, and wind speed, a simple linear transfer function is learned
from the full observation history in the input database:

    delta_variable(lead) = slope * tendency_rate + intercept

where `tendency_rate` is the 3h window rate (hPa/h) for training. At forecast time, each
regression member supplies its own tendency rate estimate (the derivative of its fitted
polynomial at t=0), which is applied to the shared transfer function.

Transfer functions are re-fitted from scratch on every `run()` call, so they
automatically incorporate new observations as the history grows. No retraining step is
needed.

## Zambretti member

The Zambretti member (id=1) classifies the 3h pressure change into one of five tendency
categories, then applies historical conditional mean deltas:

| Category | Threshold |
|----------|-----------|
| rapid_rise | ≥ +1.6 hPa |
| slow_rise | +0.1 to +1.6 hPa |
| steady | −0.1 to +0.1 hPa |
| slow_fall | −0.1 to −1.6 hPa |
| rapid_fall | ≤ −1.6 hPa |

For each `(category, variable, lead_hours)` cell, the model computes the mean observed
variable delta across all historical occurrences of that tendency category. Cells with
fewer than 3 historical examples return `None` (unscored).

This is a categorical, rules-based approach — contrasting with the continuous regression
used by the other members. The comparison illustrates a classic question: does explicit
categorical classification beat direct extrapolation?

## Zambretti dashboard display

The Zambretti algorithm is also used for a display-only panel in the dashboard's Latest
Conditions section. It maps the current tendency category to one of five traditional
weather descriptions (simplified from the original 26-letter Negretti & Zambra system).

Note: the traditional algorithm uses sea-level pressure. This implementation uses
station pressure directly, which may bias the letter code but does not affect tendency
classification.

## Members

| ID | Name | Degree | Window | Weighting |
|----|------|--------|--------|-----------|
| 0 | (ensemble mean) | — | — | inverse-MAE (or equal) |
| 1 | zambretti | categorical | 3h | — |
| 2 | linear_1h | 1 | 1h | uniform |
| 3 | linear_3h | 1 | 3h | uniform |
| 4 | linear_6h | 1 | 6h | uniform |
| 5 | linear_3h_hl45 | 1 | 3h | exp hl=45 min |
| 6 | quad_3h | 2 | 3h | uniform |
| 7 | quad_6h | 2 | 6h | uniform |
| 8 | quad_3h_hl20 | 2 | 3h | exp hl=20 min |
| 9 | quad_3h_hl45 | 2 | 3h | exp hl=45 min |
| 10 | quad_6h_hl20 | 2 | 6h | exp hl=20 min |
| 11 | quad_6h_hl45 | 2 | 6h | exp hl=45 min |

## Maintenance

Transfer functions and Zambretti conditionals auto-update each run — no intervention
needed. Member weights should be re-tuned periodically via `uv run barogram tune` as
more scored history accumulates.
