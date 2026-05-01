# Model 100: barogram_ensemble

**Type:** ensemble
**Variables:** temperature, dewpoint, pressure, precip_prob (coverage depends on contributing models)
**Lead times:** 6, 12, 18, 24 hours

## Overview

barogram_ensemble is a meta-ensemble that combines the member_id=0 (ensemble mean)
output from each base model into a single unified forecast. It runs last in the dispatch
loop so all base model rows are committed before it reads them.

## Members

| member_id | Name | Source |
|-----------|------|--------|
| 0 | (ensemble mean) | weighted mean of members 1–6 |
| 1 | persistence | model 1 member_id=0 |
| 2 | climatological_mean | model 2 member_id=0 |
| 3 | weighted_climatological_mean | model 3 member_id=0 |
| 4 | climo_deviation | model 4 member_id=0 |
| 5 | pressure_tendency | model 5 member_id=0 |
| 6 | diurnal_curve | model 6 member_id=0 |

As new base models are added, they get a new member row here (member_id == base model_id).

## Weighting

Before enough scoring history exists, all members receive equal weight. Once the `tune`
command has sufficient data (default ≥ 3 scored rows per cell), it computes inverse-MAE
weights per (member_id, variable, lead_hours, sector) and stores them in the weights
table. The ensemble reads these at forecast time, deriving the sector from each cell's
valid_at hour (0=night 00-05, 1=morning 06-11, 2=afternoon 12-17, 3=evening 18-23).
If no weight is found for a given sector, the member falls back to equal weighting.

The `spread` field on member_id=0 rows is the unweighted population standard deviation
across all contributing members for that (variable, lead_hours) cell.

## Missing values

If a base model does not produce a value for a given (variable, lead_hours) — for
example, diurnal_curve does not forecast pressure — that model is excluded from the mean
for that cell. If no base model produces a value, the cell is omitted entirely.

## Dashboard

The "Ensemble Forecast" section at the top of the dashboard shows the barogram_ensemble
member_id=0 forecast as a Now / +6h / +12h / +18h / +24h table. Spread is shown in
small text beneath each forecast value.
