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

1. **Window selection**: For each member, filter `all_obs` to the trailing window (1–12
   hours). Time values are centered at `issued_at` (hours, negative = past).
2. **Polynomial fit**: Fit a degree-1 or degree-2 polynomial via weighted normal
   equations. OLS members use uniform weights; WLS members apply exponential decay
   (recent obs weighted more). The ridge member adds an L2 penalty to all non-intercept
   coefficients, shrinking slopes toward zero.
3. **Extrapolation**: Evaluate the fitted polynomial at t = lead_hours for each variable.
4. **Precip probability**: Derived from two signals — the linear trend in
   `precip_accum_day` (active precipitation rate) and the current dewpoint depression
   (saturation proximity). See below.
5. **member_id=0**: Inverse-MAE weighted mean + spread across all 10 members. Uses
   sector-aware weights (night/morning/afternoon/evening) once scoring history
   accumulates.

## Members

| ID | Name | Degree | Window | Half-life | Notes |
|----|------|--------|--------|-----------|-------|
| 0 | ensemble mean | — | — | — | inverse-MAE weighted mean + spread across members 1–10 |
| 1 | linear-1h | 1 | 1h | none | captures very recent micro-trends |
| 2 | linear-3h | 1 | 3h | none | medium-term OLS |
| 3 | linear-6h | 1 | 6h | none | slower synoptic trends |
| 4 | linear-12h | 1 | 12h | none | large-scale trends; most stable |
| 5 | wls-3h-hl20 | 1 | 3h | 20 min | strongly recent-biased |
| 6 | wls-6h-hl45 | 1 | 6h | 45 min | moderate decay |
| 7 | wls-6h-hl120 | 1 | 6h | 120 min | gentle decay; includes most of the window |
| 8 | quad-3h | 2 | 3h | none | captures acceleration; diverges at long leads |
| 9 | quad-6h | 2 | 6h | none | longer-window acceleration; more stable than quad-3h |
| 10 | ridge-6h | 1 | 6h | none | L2-regularized; conservative anchor against trend divergence |

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

- The quadratic members (8, 9) will diverge markedly at 18h and 24h leads. This is
  expected and informative — scoring will naturally downweight them at longer leads.
- Minimum data points: degree 1 requires 2 obs, degree 2 requires 3 obs. Members abstain
  (value=None) if the window is too sparse.
- Uses `NEEDS_ALL_OBS` so `all_obs` is fetched once per run and shared with
  pressure_tendency.
