# trend_climo_blend (model 30)

Forecasts temperature and dewpoint by blending a trend-persistence "starting basis"
toward the climatological mean, with the blend weight decaying over lead hours. As
lead grows, every member's forecast converges on climatology; near lead=1 it leans
on whatever basis that member trusts.

```
forecast(lead) = weight(lead) * basis(lead) + (1 - weight(lead)) * climo_future(lead)
```

`climo_future(lead)` is the plain (month, hour) climatological bucket mean —
`db.climo_bucket_means`, the same function `climatological_mean` (model 2) uses.

## Motivation

An idea for combining trend persistence with the climatological mean: look back
some number of hours, project that trend forward, and let its influence fade as
lead grows in favor of climatology. Distinct from `climo_deviation` (model 4),
which decays a snapshot *anomaly* at issue time rather than an extrapolated
trend, and from `pressure_tendency`/`dewpoint_tendency`, which mean-revert a
trend toward the station's all-time mean rather than blending toward the
lead-specific climatological bucket.

## Basis groups

| basis | definition |
|-------|------------|
| `trend3` / `trend6` / `trend12` | linear OLS fit (`models.pressure_tendency._ols1`) over the trailing 3h/6h/12h, extrapolated as `intercept + slope * lead` |
| `flat` | current observed value, no slope — the "dumb" basis |
| `analogk5` / `analogk10` / `analogk20` | mean of the K nearest analog days' *current* value (same distance metric as `analog`, model 8), constant across lead |

## Blend families

| family | weight(lead) | mod rate | fast rate | slow rate |
|--------|--------------|----------|-----------|-----------|
| `lin` | `max(0, 1 - lead/horizon)` | horizon=12h | horizon=6h | horizon=24h |
| `exp` | `exp(-k * lead)` | k=0.15 | k=0.35 | k=0.05 |
| `log` | `1 / (1 + k * ln(1 + lead))` | k=0.4 | k=1.0 | k=0.15 |

`lin` reaches exactly zero at its horizon; `exp` and `log` never fully vanish, `log`
decaying slowest of the three at long lead.

## Members

| member_id | name | basis | family | rate |
|-----------|------|-------|--------|------|
| 1-3 | `trend3_{lin,exp,log}` | trend3 | — | mod |
| 4-6 | `trend6_{lin,exp,log}` | trend6 | — | mod |
| 7-9 | `trend12_{lin,exp,log}` | trend12 | — | mod |
| 10-12 | `flat_{lin,exp,log}` | flat | — | mod |
| 13-15 | `analogk5_{lin,exp,log}` | analogk5 | — | mod |
| 16-18 | `analogk10_{lin,exp,log}` | analogk10 | — | mod |
| 19-21 | `analogk20_{lin,exp,log}` | analogk20 | — | mod |
| 22-27 | `flat_{lin,exp,log}_{fast,slow}` | flat | — | fast/slow |
| 28 | `climo_only` | — | weight forced to 0 — pure climatology, a reference point for whether blending ever beats bare climo |
| 29 | `self_correction` | standard self-correction member, see [self_correction.md](self_correction.md) |

Rate exploration (fast/slow variants) is scoped to the `flat` basis only — cheapest
basis to reason about — rather than crossed against all seven bases, to keep the
member count from exploding into a full cartesian grid.

## member_id=0

Weighted mean (skill-score weights when available, else equal) + spread across all
28 members above (1-27 plus `climo_only`).

## Confidence

Same shared-fingerprint confidence machinery as every other model
(`models/_confidence.py`), combined via `combine_pattern` for member_id=0. New
model as of migration 067 — starts at the neutral fallback until it accumulates
scored history of its own.

## Scope

Temperature and dewpoint only, not pressure — `pressure_tendency` (model 5)
already owns trend-based pressure forecasting.
