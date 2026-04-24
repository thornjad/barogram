# analog (model 8)

Finds the K most similar historical days and uses their subsequent weather as a probabilistic forecast.

## Motivation

Analog models exploit the fact that weather is approximately periodic: days with similar current conditions tend to evolve similarly. Rather than fitting a parametric model, this approach retrieves historical precedents and uses their futures directly. The forecast improves monotonically as historical data accumulates — it is intentionally data-starved at first deployment.

## Algorithm

1. **Candidate selection**: For each historical calendar day (up to 365 days back), find the observation closest to the current local time-of-day using circular time-of-day distance. This yields one representative observation per historical day.
2. **Similarity**: Compute weighted Euclidean distance in sigma-normalized feature space across four variables: temperature, dewpoint, pressure, and wind speed. Sigma normalization means each variable contributes on a scale of standard deviations, so no variable dominates due to its units.
3. **Analog selection**: Sort candidates by distance and take the K nearest.
4. **Forecast**: For each of the K analog days, look up the observation at `analog_time + lead_hours` (±30 min window). The forecast value is the mean of those K future observations; spread is the population standard deviation.
5. **Graceful degradation**: When fewer candidates exist than K (always true early on), all available analogs are used. If no valid future observation exists for an analog at a given lead, that analog is excluded from the mean.

## Normalization

Sigma is computed from the candidate pool at each forecast issuance. When sigma is zero or only one candidate exists for a variable, that variable is dropped from the distance calculation entirely — this prevents division-by-zero and avoids inflating distances on features with no variability.

## Members

| ID | Name | K | Feature weights (T, Td, P, wind) | Notes |
|----|------|---|----------------------------------|-------|
| 0 | ensemble mean | — | inverse-MAE weighted | weighted mean + spread across members 1–8 |
| 1 | k3 | 3 | [1, 1, 1, 1] | smallest pool; most sensitive to exact match quality |
| 2 | k5 | 5 | [1, 1, 1, 1] | default small-pool balanced |
| 3 | k10 | 10 | [1, 1, 1, 1] | more robust as data accumulates |
| 4 | k20 | 20 | [1, 1, 1, 1] | large pool; approaches climatology with sparse data |
| 5 | k5-moisture | 5 | [2, 2, 1, 0.5] | temperature and dewpoint emphasized; targets airmass character |
| 6 | k5-synoptic | 5 | [1, 0.5, 2, 1.5] | pressure and wind emphasized; targets synoptic regime |
| 7 | k10-dist-weighted | 10 | [1, 1, 1, 1] | inverse-distance weighted mean; closer analogs count proportionally more |
| 8 | k5-seasonal | 5 | [1, 1, 1, 1] | distances penalized by calendar-month difference (γ=0.2 per month); prefers same-season analogs |

## Limitations

- **Data starvation**: Performance is poor until a year or more of history accumulates. With only weeks of data, K=3 and K=5 may be using the only available candidates.
- **No synoptic context**: The model matches on the current point-in-time observation state. It cannot distinguish a morning reading that will evolve into severe weather from one that will remain benign, unless such evolution is well-represented in the analog pool.
- **Time-of-day alignment**: Analogs are drawn from the same time of day, so the diurnal phase is implicitly preserved. This is appropriate but means the model has no mechanism to correct for diurnal timing errors.
- **Seasonal penalty is weak**: The γ=0.2 per-month penalty in the seasonal member is intentionally gentle. With limited history it mostly behaves like the equal-weight members; the benefit emerges after a full annual cycle.
