# multivariate_trend (model 14)

Fits an independent regression to each sensor variable's recent time series and
extrapolates it forward to each lead time. Every variable is its own predictor — no
variable proxies another. Members vary by window length, weighting scheme, and
polynomial degree.

## Motivation

The pressure_tendency model (5) extrapolates a pressure curve and maps the pressure
tendency rate to other variables via a learned transfer function. That approach is strong
when pressure changes drive the other variables, but it breaks down when the relationship
is weak or lagged (e.g., a temperature trend driven by solar heating has little to do
with pressure). multivariate_trend bypasses that coupling and extrapolates each variable
directly from its own recent trajectory.

## Algorithm

1. **Window selection**: For each member, filter `all_obs` to the trailing window (1–3
   hours). Time values are centered at `issued_at` (hours, negative = past).
2. **Polynomial fit**: Fit a degree-1 polynomial via weighted normal equations. The OLS
   member uses uniform weights; the WLS member applies exponential decay (recent obs
   weighted more).
3. **Extrapolation**: Evaluate the fitted polynomial at t = lead_hours for each variable.
   Members are restricted to leads where the extrapolation ratio is defensible (see Lead
   restrictions below).
4. **Precip probability**: Derived from two signals — the linear trend in
   `precip_accum_day` (active precipitation rate) and the current dewpoint depression
   (saturation proximity). See below.
5. **member_id=0**: Inverse-MAE weighted mean + spread across all named members that
   produced values for a given lead. Uses sector-aware weights (night/morning/afternoon/
   evening) once scoring history accumulates.

## Lead restrictions

Short-window members extrapolating far beyond their window produce physically meaningless
results (e.g., a 1h morning warming trend projected 24h). Each member is capped at a
`max_lead_h` based on the extrapolation ratio:

- **Linear members**: capped at 2× their window length

Members produce `None` for leads beyond their cap and are excluded from the ensemble mean
at those leads.

## Members

| ID | Name | Degree | Window | Half-life | Max lead | Notes |
|----|------|--------|--------|-----------|----------|-------|
| 0  | ensemble mean   | — | — | — | — | inverse-MAE weighted mean + spread |
| 1  | linear-1h       | 1 | 1h   | none   | 6h   | very recent micro-trends |
| 2  | linear-3h       | 1 | 3h   | none   | 6h   | medium-term OLS |
| 5  | wls-3h-hl20     | 1 | 3h   | 20 min | 6h   | strongly recent-biased |

### Retired members (2026-09-18)

IDs 3, 4, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16 (`linear-6h` through `wls-24h-hl360`,
including both quadratic members and the ridge member) were consistently the worst
performers in the whole barogram roster — cross-variable z-score analysis showed a
structural mismatch, not a data-maturity gap: the quadratic members overshot on a 3-7h
window regardless of history length, and the rest lost to climatology-style members on
temp/dewpoint because linear trend extrapolation can't track a diurnal cycle no matter
how long it's fit over. Historical forecast rows and their `members`-table entries are
kept; they just no longer run. Full writeup: thornlog message board
"barogram-model-analysis".

This also retired the Hypothesis H window-length sweep, which asked whether there is an
optimal window length for each lead time (MAE decreases as window grows, peaks at some
optimum, then increases as stale data dilutes the recent trend). Members 13
(`linear-36h`) and 14 (`linear-48h`) were deliberately long — the expectation was that a
48h linear trend over two diurnal cycles would be close to zero and converge toward
something resembling persistence of the current anomaly. The dashboard's Learnings
section (which plotted this and the other tracked hypotheses) has since been removed
entirely.

## Precip probability

```
if precip_accum_day slope > 0.2 mm/hr (active precipitation):
    p = min(0.95, rate / (rate + 0.3)) × exp(−lead / 12)
else:
    dp_dep = air_temp − dew_point
    if dp_dep < 14°C:
        p = max(0.0, 0.35 − dp_dep × 0.025)
    else:
        p = 0.0
```

The first branch captures ongoing precipitation and projects it forward with exponential
decay. The second uses saturation proximity as a moisture availability proxy.

## Notes

- Minimum data points: 2 obs (degree-1 fit). Members abstain (value=None) if the window
  is too sparse.
- Uses `NEEDS_ALL_OBS` so `all_obs` is fetched once per run (full history from timestamp
  0) and shared across members.
- The `window_h` for each member is stored in the `members` table.

## Confidence

Every member here gets a confidence value computed against the shared default
fingerprint (`air_temp`, `dew_point`, `station_pressure`, `wind_avg`), matched
against its own scored history by calendar day. member_id=0's combination now
multiplies each member's weight by its own confidence (floored, defaulted to the
group's own average when unknown) via `models/_confidence.py`'s `combine_pattern`,
which also fixed a pre-existing bug: a member missing a weight used to collapse the
whole group to a plain average, now only that member is dropped. See
[confidence.md](confidence.md) for the full design.
