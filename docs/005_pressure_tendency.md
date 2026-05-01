# pressure_tendency (model 5)

Uses the recent barometric pressure time series to forecast all four variables via
polynomial extrapolation and empirical transfer functions.

## Motivation

A physical barogram records pressure tendency over time. This is one of the oldest and most reliable single-variable forecasting signals. This model explores whether different ways of reading that signal (different window lengths, polynomial degrees, and weighting schemes) produce meaningfully different forecasts, and how they compare to a century-old rules-based approach (Zambretti).

## Pressure forecasting

For each regression member, a polynomial is fit to recent `(timestamp, station_pressure)` observations. Time is centered relative to `issued_at` (t=0 = now, t=-1 = one hour ago) for numerical stability. The polynomial is then extrapolated to each `valid_at` to produce the pressure forecast.

Members differ in:
- **Window length**: how far back in time observations are drawn from (1h, 3h, or 6h)
- **Polynomial degree**: linear (degree 1) or quadratic (degree 2)
- **Weighting scheme**: uniform (all observations equally weighted) or exponentially
  decaying (more recent observations receive higher weight)

The exponential decay weighting uses the form `w = exp(ln(2)/hl * t)` where t is the
centered time in hours and hl is the half-life. At t=0 (current time), w=1; at t=-hl,
w=0.5.

## Transfer functions for other variables

For temperature and dew point, a simple linear transfer function is learned from the full observation history in the input database:

    delta_variable(lead) = slope * tendency_rate + intercept

where `tendency_rate` is the 3h window rate (hPa/h) for training. At forecast time, each regression member supplies its own tendency rate estimate (the derivative of its fitted polynomial at t=0), which is applied to the shared transfer function.

Transfer functions are re-fitted from scratch on every `run()` call, so they automatically incorporate new observations as the history grows. No retraining step is needed.

## Zambretti member

The Zambretti member (id=1) classifies the 3h pressure change into one of five tendency categories, then applies historical conditional mean deltas:

| Category | Threshold |
|----------|-----------|
| rapid_rise | ≥ +1.6 hPa |
| slow_rise | +0.1 to +1.6 hPa |
| steady | −0.1 to +0.1 hPa |
| slow_fall | −0.1 to −1.6 hPa |
| rapid_fall | ≤ −1.6 hPa |

For each `(category, variable, lead_hours)` cell, the model computes the mean observed variable delta across all historical occurrences of that tendency category.
