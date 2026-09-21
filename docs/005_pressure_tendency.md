# pressure_tendency (model 5)

Uses the recent barometric pressure time series to forecast all four variables via
polynomial extrapolation and empirical transfer functions.

## Motivation

A physical barogram records pressure tendency over time. This is one of the oldest and most reliable single-variable forecasting signals. This model explores whether different ways of reading that signal (different window lengths, polynomial degrees, and weighting schemes) produce meaningfully different forecasts, and how they compare to a century-old rules-based approach (Zambretti).

## Pressure forecasting

For each regression member, a polynomial is fit to recent `(timestamp, station_pressure)` observations. Time is centered relative to `issued_at` (t=0 = now, t=-1 = one hour ago) for numerical stability. The polynomial is then extrapolated to each `valid_at`, and the result is passed through an Ornstein–Uhlenbeck mean reversion step before being stored:

    p_forecast = p_mean + (p_raw - p_mean) * exp(-λ * lead_hours)

where `p_mean` is the all-time mean of historical station pressures and `λ = 0.10` per hour (e-folding time of 10 hours). At a 6h lead, roughly 55% of the polynomial's deviation from the mean is retained; at 24h, only 9% is retained. This prevents physically impossible extrapolations (unconstrained quadratic fits over 1–6h of data can otherwise diverge to thousands of hPa at the 24h lead) while still allowing the polynomial signal to meaningfully influence shorter-range forecasts. The Zambretti member is unaffected — it already uses conditional historical means and does not extrapolate.

Members differ in:
- **Window length**: how far back in time observations are drawn from (1h, 3h, or 6h)
- **Weighting scheme**: uniform (all observations equally weighted) or exponentially
  decaying (more recent observations receive higher weight)

### Retired members (2026-09-18)

IDs 6, 7, 8, 9, 10, 11 (`quad_3h` through `quad_6h_hl45`, all degree-2/quadratic) were
consistently the worst performers in the whole barogram roster — a quadratic fit over a
3-6h window overshoots on extrapolation regardless of how much history accumulates, a
structural mismatch rather than a data-maturity gap. Historical forecast rows and their
`members`-table entries are kept; they just no longer run. Full writeup: thornlog
message board "barogram-model-analysis". Only linear (degree-1) members remain.

The exponential decay weighting uses the form `w = exp(ln(2)/hl * t)` where t is the
centered time in hours and hl is the half-life. At t=0 (current time), w=1; at t=-hl,
w=0.5.

## Transfer functions for other variables

For temperature and dew point, a simple linear transfer function is learned from the full observation history in the input database:

    delta_variable(lead) = slope * tendency_rate + intercept

where `tendency_rate` is the 3h window rate (hPa/h) for training. At forecast time, each regression member supplies its own tendency rate estimate (the derivative of its fitted polynomial at t=0), which is applied to the shared transfer function.

Transfer functions are re-fitted from scratch on every `run()` call, so they automatically incorporate new observations as the history grows. No retraining step is needed.

## Zambretti member (ensemble contribution)

The Zambretti member (id=1) classifies the 3h pressure change into one of five tendency categories, then applies historical conditional mean deltas:

| Category | Threshold |
|----------|-----------|
| rapid_rise | ≥ +1.6 hPa |
| slow_rise | +0.1 to +1.6 hPa |
| steady | −0.1 to +0.1 hPa |
| slow_fall | −0.1 to −1.6 hPa |
| rapid_fall | ≤ −1.6 hPa |

For each `(category, variable, lead_hours)` cell, the model computes the mean observed variable delta across all historical occurrences of that tendency category. This is a simplified, rules-based contrast piece for the ensemble — not the historical Zambretti algorithm — and is not scored on its own text output.

## Zambretti dashboard display (`zambretti_text()`)

The dashboard's "Zambretti forecast for today" panel uses the actual Zambretti forecaster algorithm (Negretti & Zambra, 1915), not the five-category classifier above. It runs the classic formula documented by beteljuice.com and implemented widely (e.g. `pywws.ZambrettiCore`; every letter/lookup-table constant here has been diffed byte-for-byte against that source):

1. **Sea-level pressure** — station pressure reduced to sea level via the hypsometric formula when elevation is configured.
2. **Wind direction** — the current wind reading is bucketed into one of 16 compass sectors, each with a fixed pressure adjustment (`+5.2` from due north tapering down to `−11.5` from due south, per the reference table). Skipped when wind is unavailable.
3. **3h trend** — rising (≥ +0.1 hPa/h), falling (≤ −0.1 hPa/h), or steady, each using its own linear formula and lookup table.
4. **Season and hemisphere** — April–September counts as the northern-hemisphere growing season, adding or removing 3.2 hPa depending on trend direction. Hemisphere is hardcoded to north — the station is stationary in Central US and will never move.

The adjusted pressure is run through the trend-specific formula (e.g. rising: `F = 0.1740 * (1031.40 - pressure)`) to get an index `F`, which is rounded and clamped into a lookup table of 14 (rising), 10 (falling), or 17 (steady) letter codes (A-Z), each mapping to one of the 26 classic Zambretti forecast texts (e.g. `Z` = "Stormy, much rain", `A` = "Settled fine"). This replaces an earlier implementation that used only the 3h tendency rate with a 5-bucket table — that version ignored absolute pressure entirely, so a rapid but small wobble near a high baseline (e.g. 1035 to 1033 hPa) could read "Stormy, much rain" even though nothing stormy was actually forecast.

There is no single official digital Zambretti formula — the 1915 device was an analog dial, and several people have independently reverse-engineered it into code with different constants (e.g. the `zambretti-py` package uses an entirely different formula/lookup with no season term at all). This implementation follows the beteljuice/pywws variant, the one most widely deployed across weather-station software.

### Daily anchor time

The algorithm's ~90% accuracy claim was historically measured from a single reading taken once daily around 9 AM local solar time, not from continuous recalculation. Rather than compute true solar time, `zambretti_text()` anchors to a fixed clock time approximating it: **9:12 AM CST / 10:12 AM CDT**, expressed internally as a constant 15:12 UTC (America/Chicago is always UTC-6 or UTC-5, so this lands on the right wall-clock time either way with no DST-awareness needed). It always looks back to the most recent occurrence of that anchor — so the panel shows one stable "forecast for today" no matter what time the dashboard itself regenerates, rather than recomputing off whatever the pressure happens to be doing right now.

## Confidence

Every member here gets a confidence value computed against the shared default
fingerprint (`air_temp`, `dew_point`, `station_pressure`, `wind_avg`), matched
against its own scored history by calendar day. member_id=0's combination now
multiplies each member's weight by its own confidence (floored, defaulted to the
group's own average when unknown) via `models/_confidence.py`'s `combine_pattern`,
which also fixed a pre-existing bug: a member missing a weight used to collapse the
whole group to a plain average, now only that member is dropped. See
[confidence.md](confidence.md) for the full design.
