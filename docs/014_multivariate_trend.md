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

1. **Window selection**: For each member, filter `all_obs` to the trailing window (1–48
   hours). Time values are centered at `issued_at` (hours, negative = past).
2. **Polynomial fit**: Fit a degree-1 or degree-2 polynomial via weighted normal
   equations. OLS members use uniform weights; WLS members apply exponential decay
   (recent obs weighted more). The ridge member adds an L2 penalty to all non-intercept
   coefficients, shrinking slopes toward zero.
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
- **Quadratic members**: capped at 1× their window length (tighter because the x² term
  grows faster outside the fitting range)
- Members with window ≥ 12h have no cap — their extrapolation ratio is ≤ 2× at the 24h
  lead

Members produce `None` for leads beyond their cap and are excluded from the ensemble mean
at those leads.

## Members

| ID | Name | Degree | Window | Half-life | Max lead | Notes |
|----|------|--------|--------|-----------|----------|-------|
| 0  | ensemble mean   | — | — | — | — | inverse-MAE weighted mean + spread |
| 1  | linear-1h       | 1 | 1h   | none   | 6h   | very recent micro-trends |
| 2  | linear-3h       | 1 | 3h   | none   | 6h   | medium-term OLS |
| 3  | linear-6h       | 1 | 6h   | none   | 12h  | slower synoptic trends |
| 4  | linear-12h      | 1 | 12h  | none   | none | large-scale trends; most stable |
| 5  | wls-3h-hl20     | 1 | 3h   | 20 min | 6h   | strongly recent-biased |
| 6  | wls-6h-hl45     | 1 | 6h   | 45 min | 12h  | moderate decay |
| 7  | wls-6h-hl120    | 1 | 6h   | 120 min| 12h  | gentle decay |
| 8  | quad-3h         | 2 | 3h   | none   | 6h   | captures acceleration; strict cap |
| 9  | quad-6h         | 2 | 6h   | none   | 6h   | longer-window acceleration; capped at 1× window |
| 10 | ridge-6h        | 1 | 6h   | none   | 12h  | L2-regularized conservative anchor |
| 11 | linear-18h      | 1 | 18h  | none   | none | fills the 18h/24h gap |
| 12 | linear-24h      | 1 | 24h  | none   | none | full-day trend |
| 13 | linear-36h      | 1 | 36h  | none   | none | hypothesis sweep: 1.5 days |
| 14 | linear-48h      | 1 | 48h  | none   | none | hypothesis sweep: 2 days |
| 15 | wls-18h-hl240   | 1 | 18h  | 4h     | none | 18h window, 4h half-life |
| 16 | wls-24h-hl360   | 1 | 24h  | 6h     | none | 24h window, 6h half-life |

## Hypothesis H: trend window vs. skill

Members 11–16 were added to test whether there is an optimal window length for each lead
time. The hypothesis: MAE decreases as window grows (capturing more signal), peaks at
some optimum, then increases as stale data dilutes the recent trend.

Members 13 (`linear-36h`) and 14 (`linear-48h`) are deliberately long — the expectation
is that a 48h linear trend over two diurnal cycles will be close to zero and converge
toward something resembling persistence of the current anomaly. The dashboard tracks this
in Hypothesis H, plotting all-time avg MAE against window length for each lead.

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

## Ridge regularization

The ridge member modifies the normal equations by adding `alpha = 5.0` to the diagonal
element corresponding to each non-intercept coefficient. This penalizes large slopes,
pulling extrapolated values toward the current level rather than following the trend
aggressively. It acts as a conservative anchor in the ensemble when other members diverge.

## Notes

- Minimum data points: degree 1 requires 2 obs, degree 2 requires 3 obs. Members abstain
  (value=None) if the window is too sparse.
- Uses `NEEDS_ALL_OBS` so `all_obs` is fetched once per run (full history from timestamp
  0) and shared across members. All window sizes including 48h have access to the full
  observation history.
- The `window_h` for each member is stored in the `members` table to support the
  hypothesis H dashboard chart.
