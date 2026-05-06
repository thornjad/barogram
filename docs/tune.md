# Ensemble Member Tuning

The ensemble models in barogram generate a large field of members per run. Model 3 has 9,
each using a different recency-weighting scheme for historical observations. Model 4 has
36, adding four decay-rate hypotheses on top of those 9. Every forecast run collapses that
field into a single `member_id=0` value that the dashboard shows. How to do that collapsing
well is the question tuning tries to answer.

The naive answer is equal weighting, an arithmetic mean of all members. This is reasonable
before any scoring history exists. Once there is a scoring history, though, some members are
demonstrably better than others for specific variables and lead times, and equal weighting
treats all of them as equally credible witnesses regardless of that track record.

## How real ensemble systems handle this

Real ensemble forecasting systems face the same problem at much larger scale. ECMWF's ENS
runs 50 perturbed members plus a control run, and the raw ensemble mean is not the final
product. Post-processing steps learn statistical relationships between the ensemble's
historical output and observations. The most common approach, Ensemble Model Output
Statistics (EMOS), fits a regression that uses the ensemble mean and spread to correct for
systematic bias and miscalibration. Bayesian Model Averaging (BMA) is similar, assigning a
posterior weight to each member based on how well it has predicted observations over a
training window, then producing a weighted mixture as the forecast.

The core insight is that ensemble members are not interchangeable. Different perturbation
schemes produce members that are reliably better or worse for specific variables and lead
times. Learning that structure is more useful than averaging over it.

## What barogram does

Barogram uses a simplified version of this principle. After enough forecasts have been
scored, `tune` computes a **skill score** for each member: how much better (or worse) it
performs relative to a naive reference model. For temperature, dewpoint, and precipitation
probability the reference is `climatological_mean`; for pressure it is `persistence`.

Skill is defined as `1 - loss_member / loss_reference`. A skill of zero means the member
performs exactly as well as the reference; positive skill means it beats the reference;
negative skill means it is worse. The loss metric is **Huber loss** rather than plain MAE.
Below a per-variable threshold δ, errors are penalized quadratically, making the system
more sensitive to consistent mediocrity than MAE. Above δ, the penalty is linear, limiting
the influence of anomalous events like a polar vortex or a supercell passing at scoring
time. δ is computed automatically as the 80th percentile of historical absolute errors for
each variable, pooled across all non-external models.

Members with positive skill receive weight proportional to their skill (with a floor).
Members whose skill equals zero (the reference model itself) receive a floor weight
proportional to `--floor / n_members`. Members with negative skill receive a subfloor
weight (`--floor * --subfloor-fraction / n_members`). The `bogo` model is unconditionally
pinned to subfloor regardless of its apparent skill, because its sparse scoring history
can produce statistically noisy positive skill estimates. All weights are normalized to sum
to 1 within each (model, variable, lead_hours, sector) cell.

## Sector-aware weighting

Different physical mechanisms dominate at different times of day. A model that infers
daytime heating from solar radiation will accumulate poor Huber scores during nighttime
valid times even if it is genuinely skilful during the afternoon. Pooling those errors
into a single weight would systematically undervalue it.

To address this, `tune` partitions the scoring history by the hour of each forecast's
valid_at time into four sectors:

| Sector | Hours | Label |
|--------|-------|-------|
| 0 | 00:00–05:59 | night |
| 1 | 06:00–11:59 | morning |
| 2 | 12:00–17:59 | afternoon |
| 3 | 18:00–23:59 | evening |

Each (member, variable, lead_hours) cell gets a separate weight per sector. At forecast
time the ensemble looks up the sector that matches each cell's valid_at hour.

## Pooled blending

Sector-specific weights can be noisy when sector data is sparse, particularly early in
the system's history. To smooth this out, `tune` blends each sector-specific Huber loss estimate
with the all-sector (pooled) Huber loss using a fixed `--pool-alpha` fraction (default 0.10):

```
blended_loss = (1 - pool_alpha) * sector_huber + pool_alpha * pooled_huber
```

When a sector has fewer than `--min-runs` scored rows, the blend collapses to the pooled
Huber loss entirely. The pooled fraction is permanent — it does not shrink to zero as data
accumulates — so the all-sector signal always contributes a small stabilizing influence.

One variable-specific note: the `mae` column in the forecasts table stores Brier score
(squared error) for `precip_prob` and absolute error for all other variables. `tune`
uses the `error` column (signed linear difference) for all Huber loss computations, so
the tuning is consistent across variables. The `mae` column is used only by the
dashboard for display.
