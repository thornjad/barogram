# Model 100: barogram_ensemble

**Type:** ensemble
**Variables:** temperature, dewpoint, pressure (coverage depends on contributing models)
**Lead times:** 6, 12, 18, 24 hours

## Overview

barogram_ensemble is a meta-ensemble that combines the member_id=0 (ensemble mean)
output from each base model into a single unified forecast. It runs last in the dispatch
loop so all base model rows are committed before it reads them.

## Members

| member_id | Name | Source |
|-----------|------|--------|
| 0 | (ensemble mean) | weighted mean of all contributing base models |
| 1 | persistence | model 1 member_id=0 |
| 2 | climatological_mean | model 2 member_id=0 |
| 3 | weighted_climatological_mean | model 3 member_id=0 |
| 4 | climo_deviation | model 4 member_id=0 |
| 5 | pressure_tendency | model 5 member_id=0 |
| 6 | diurnal_curve | model 6 member_id=0 |
| 7 | airmass_diurnal | model 7 member_id=0 |
| 8 | analog | model 8 member_id=0 |
| 9 | surface_signs | model 9 member_id=0 |
| 10 | synoptic_state_machine | model 10 member_id=0 |
| 12 | bogo | model 12 member_id=0 |
| 101 | self_correction | model 100's own self-correction (the former model 101), not a contributing model |

Members are discovered dynamically via `db.sync_ensemble_members` — new base models are added automatically when they first run. member_id equals the contributing model's MODEL_ID.

## Self-correction and the final member 0

Member 0 as `run()` produces it is the raw blend of the contributing base models. Member 101 is the
ensemble's self-correction member: the raw blend minus a bias learned from the `raw_mix` history
(member -1) of this model. `sc_blend.apply` then stores the raw blend as `raw_mix` and replaces member 0
with the raw blend blended with member 101 at its stored tune weight, exactly as for every base model
(see [self_correction.md](self_correction.md)). Model 100's `raw_mix` history starts empty at the cutover,
so member 101 has no value for the first day or two and member 0 is the raw blend meanwhile.

## Weighting

Before enough scoring history exists, all members receive equal weight. Once the `tune`
command has sufficient data (default ≥ 3 scored rows per cell), it computes skill-score
weights per (member_id, variable, lead_hours, sector) and stores them in the weights
table. Weights are set by each member's Huber loss relative to a reference model
(climatological mean for temperature and dewpoint, persistence for pressure). Members that beat
the reference earn proportional weight; those that don't are floored or subfloored.
The ensemble reads these at forecast time, deriving the sector from each cell's
valid_at hour (0=night 00-05, 1=morning 06-11, 2=afternoon 12-17, 3=evening 18-23).
If no weight is found for that sector for any member, the whole group falls back to a
plain equal average (`models/_confidence.py`'s `combine_pattern`); if only some
members lack a weight, only those members are dropped, the rest still combine by
weight, confidence-adjusted.

The `spread` field on member_id=0 rows is the unweighted population standard deviation
across all contributing members for that (variable, lead_hours) cell.

## Confidence

barogram_ensemble is treated as just another weighted model here, combining its
members' confidence via the exact same `combine_pattern` every other multi-member
model uses, no special case for being the meta-ensemble. Each contributing base
model's own confidence, already computed by that model against its own scored
history, flows straight in via `ensemble_inputs`'s `confidence` column; the ensemble
doesn't compute confidence itself, it only combines what its members already report.
See [confidence.md](confidence.md) for the full design.

## Missing values

If a base model does not produce a value for a given (variable, lead_hours) — for
example, diurnal_curve does not forecast pressure — that model is excluded from the mean
for that cell. If no base model produces a value, the cell is omitted entirely.

## Dashboard

The "Ensemble Forecast" section at the top of the dashboard shows the barogram_ensemble
member_id=0 forecast as a Now / +6h / +12h / +18h / +24h table. Spread and confidence
(as a percentage) are both shown in small text beneath each forecast value.
