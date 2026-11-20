# Model 300: meta_forecast

**Type:** meta
**Variables:** temperature, dewpoint, pressure
**Lead times:** 1 through 24 hours

## Overview

meta_forecast is the best forecast barogram can make for this yard from everything it knows. The ensemble (class 100) forecasts from backyard observations only, the external models (class 200) bring in outside forecasts only, and this model reads all of them plus current conditions and combines them as an ordinary multi-member model. It runs last in the dispatch loop, after `barogram_ensemble`, and reads the rows that the ensemble, the three external models and the base models just wrote for this run.

It is a side project. The ensemble stays the headline forecast, and meta_forecast feeds nothing. Its `models.type` is `meta`, so `db.ensemble_inputs`, `db.sync_ensemble_members` and the Huber delta pool (all of which filter on `type = 'base'`) never see it. The dashboard shows it in the usual per-model tables with a `meta` badge.

Weighting, confidence and self-correction work the way they do for every other model, so read [tune.md](tune.md), [confidence.md](confidence.md) and [self_correction.md](self_correction.md) for the shared mechanics. This page covers only what is specific to model 300.

## Members

Static members are registered by `migrations/074_meta_forecast.sql` and are listed in `STATIC_MEMBERS` in `models/meta_forecast.py`. Migration registration is data only, and the migration file is immutable once applied (see [database.md](database.md)), so a change to the static set ships as a new numbered migration.

| id | name | what it holds | variables |
|----|------|---------------|-----------|
| 1 | nws | pass-through of model 200 member 0 | temperature, dewpoint |
| 2 | tempest_forecast | pass-through of model 201 member 0 | temperature, dewpoint |
| 3 | external_corrected | pass-through of model 202 member 0 | temperature, dewpoint |
| 4 | barogram_ensemble | pass-through of model 100 member 0 | temperature, dewpoint |
| 11 | median_sources | median of the pool values present | all |
| 12 | hot_hand | pool mean weighted by inverse recent MAE | all |
| 13 | spread_gated_ensemble | `v202 + 0.5 * g * (v_ens - v202)`, g from the ensemble's spread | temperature, dewpoint |
| 14 | ensemble_residual | `v202 + gamma * (v_ens - v202)`, gamma fitted | temperature, dewpoint |
| 15 | obs_anchor | `v202 - rho * r`, rho fitted, r the recent 202 error | temperature, dewpoint |
| 16 | lagged_consensus | mean of 202's value for this valid time over this run and up to 2 earlier runs | temperature, dewpoint |
| 17 | ensemble_gated | `v202 + w * (v_ens - v202)`, w the ensemble's share of the two confidences | temperature, dewpoint |
| 18 | confidence_argmax | the pool value with the highest stored confidence | all |
| 91 | self_correction | 60-day bias correction of the raw mix | all |
| 92 | self_correction_fast | 14-day bias correction of the raw mix | all |
| -1 | raw_mix | stored by `sc_blend.apply`, never emitted by `run()` | all |
| 0 | (final value) | | all |
| 100 + n | base-model pass-throughs | pass-through of base model `n` member 0 | persistence for all variables, every other base model for pressure only |

The 100-series members are registered by `run()` itself with `insert or ignore into members`, one for each base model that emits a usable row in the current run. A base model that starts emitting pressure therefore joins without a migration. Source rows with a null `value` (model 26's pressure rows are one example) are dropped before anything else uses them, so they create no member, no pass-through row and no pool entry.

### Why these members exist

Members 1 and 2 are the raw outside sources, kept so `tune` can weight them directly and so `median_sources` has at least three pool values. Members 3 and 4 carry 202's corrections and the ensemble's blend.

Persistence (member 101) holds the latest observation itself, which no other member carries, and it is also the `tune` reference for pressure. Member 15 (`obs_anchor`) brings in current conditions a second way, by shifting 202 with how wrong it was over the last two hours. Together these are the model's current-conditions inputs.

The base-model pass-throughs exist because the ensemble stopped combining pressure on 2026-09-09 (commit c52f874) and the external sources carry none. For temperature and dewpoint the base models already reach this model through the ensemble, so repeating them would add nothing. For pressure there is no aggregate source, so each base model that emits pressure becomes one member and `tune` sets their weights. This is the same shape the ensemble had for pressure before it dropped it.

Members 13, 14 and 17 share the form `v202 + k * (v_ens - v202)` and overlap on purpose, since each gates the ensemble's pull on different information (its spread, a fitted coefficient, and the two confidences). Members 3 and 13 to 17 are all anchored on 202, so that family is the largest group in the temperature and dewpoint pool.

### Member 92

Member 92 uses a 14-day correction window although `migrations/066_self_correction_fast_window_members.sql` limited fast windows to two models. An earlier walk-forward backtest of this member design (a session analysis, not stored in the repo) found the 14-day correction beat the 60-day one on dewpoint at every lead from 6 hours out, for example 1.68 against 1.72 degrees F MAE at 6 hours and 1.94 against 1.97 at 24 hours.

## Pools and required inputs

Members 11, 12 and 18 draw from a per-variable pool. For temperature and dewpoint the pool is members 1 to 4. For pressure it is every 100-series member present in the cell. A derived member emits no row when it lacks what it needs.

| member | requires |
|--------|----------|
| 11 | at least 3 pool values |
| 12 | at least 2 pool values |
| 13 | values of members 3 and 4, and model 100's member-0 spread |
| 14 | values of members 3 and 4, and a gamma fit |
| 15 | value of member 3, a recent 202 error, and a rho fit |
| 16 | value of member 3 and at least one qualifying earlier 202 run |
| 17 | values of members 3 and 4 and their stored confidences, not both 0 |
| 18 | at least 2 pool members with a stored confidence (`bogo` is skipped, its confidence is a random draw), and the highest one above 0 |

## Cell valid_at

Every (variable, lead) cell gets one `valid_at`. It is 202's member-0 `valid_at` when present, otherwise the first present of 200, 201 and 100, otherwise the lowest `valid_at` among the remaining source rows. Member 0, every derived member and the self-correction members use it, which satisfies `sc_blend`'s requirement that a self-correction row and member 0 share a time. Pass-through members keep their source row's own `valid_at` and are scored against that time, as the ensemble's members are. The external models sit on the hour while the ensemble anchors to the latest observation, so the two typically differ by a few minutes.

## Derived members in detail

`hot_hand` weights each pool member by the reciprocal of its source model's mean `mae` over the last 5 days in the same cell. If any pool member has fewer than 6 rows in that window it falls back to the plain mean, since a thin window gives a misleading MAE.

`spread_gated_ensemble` compares the ensemble's spread for the cell with the median of its own spread over the last 30 days, using `g = clip(1 - s / (2 * s_ref), 0, 1)`. Fewer than 20 history rows set `g` to 0.5.

`ensemble_gated` weights 202 and the ensemble by their confidences. Confidences are small and close together, particularly at long leads, so the member compares them as a ratio (`w = c_ens / (c_ens + c_202)`), which keeps the signal that an absolute difference would lose.

`confidence_argmax` takes the pool value with the highest stored confidence, ties to the lowest member id. It skips `bogo`, whose confidence comes from a random choice of matches (`STATES_OWN_MEMBER_ZERO_CONFIDENCE` in `models/bogo.py`) and so is not comparable with the evidence-based confidences of other models, and it abstains when the highest confidence is 0.

`lagged_consensus` averages this run's 202 value with up to two earlier runs for the same valid time. Each earlier run contributes the row nearest the valid time (within 600 seconds), and runs must be at least an hour apart from this run and from each other. An earlier run has no row beyond 24 hours, so the member is absent at the longest leads.

### Fitted members

`ensemble_residual` and `obs_anchor` each fit one coefficient per (variable, lead) from the last 90 days of scored errors, and `error` is never pruned so history past the 30-day debugging window stays usable.

- `gamma = -sum(e_c * d) / (sum(d * d) * (1 + 20 / n))`, clipped to [-0.5, 1.0], where `e_c` is 202's error and `d` is the ensemble's error minus 202's.
- `rho = sum(e_c * r) / (sum(r * r) * (1 + 20 / n))`, clipped to [0, 1.5], where `r` is the mean error of 202's rows valid in the two hours before the run.

Each fit needs at least 30 pairs and otherwise the member emits no row. Two sources of noise are accepted. The ensemble and 202 are scored against observations up to 20 minutes apart, so `d` is slightly different from the value gap that gamma is later applied to. And the historical `r` includes rows scored after the fact, while a live run sees only rows already scored (`barogram-run` scores first, a manual `forecast` may not), so a live `r` can be smaller than the fitted one. The shrinkage term and the clips bound both effects.

## Dewpoint clamp

`insert_forecasts` clamps every stored dewpoint to the temperature of the same member, time and lead, and exempts only models 200 and 201 themselves. Model 300's members are all clamped, so a stored pass-through of those two models can differ from its source when the source's dewpoint exceeds its own temperature.

## Success criterion and follow-ups

The goal is for member 0 to score at or below 202's member 0 MAE for temperature and dewpoint at every lead, and at or below persistence's for pressure (persistence is pressure's `tune` reference), measured over matched `issued_at` once `tune` has produced weights for this model. Until then member 0 is an unweighted equal mean of its members, and after the first tune it is a weighted mean over only the members that already have weights, so early comparisons say little. Members 11 to 18 report a confidence of 0.0 until they have scored history of their own. The confidence floor in `combine_pattern` (0.001) still gives such a member about 60% of a typical pass-through's influence once weights exist, since pass-through confidences sit near 0.0016, so the derived members count from the first weighted run. The self-correction members enter member 0 through `sc_blend` by tune weight, and their confidence plays no part.

Two follow-ups need days of scored history and are not part of the build.

- Run the success-criterion comparison once weights exist and report every lead where the model loses.
- Re-time `forecast` against the pre-300 baseline after about 7 days and again after about 60 days. `cmd_forecast` loads 400 days of scored history for every registered member of this model, and most of its members are pass-throughs that never use that history. With about 56 member-variables the load projects to roughly five times the ensemble's per-run cost at saturation, so the projected cost is the measured cost times 400 divided by the days of history elapsed. If that projection matters, skip history loading for pass-through members. The first real-data run added no measurable time over the pre-300 baseline (5:19 against 5:43, both noisy), but that run had no scored history to load.

## Deferred members

These were considered and left out of the first version.

| member idea | reason deferred |
|-------------|-----------------|
| pass-throughs of individual base models for temperature and dewpoint, and of 202's ten internal members | the ensemble and member 3 already carry them |
| affine_correction, revision_momentum | both need forecast value and observed pairs, which exist only inside the 30-day prune window |
| lead_smooth, coherence, nowcast_handoff, trend_match_weight | post-processing ideas that need design work first |
| analog_weighted | needs the shared matches wired through per member |
| self_correction_median | needs a variant of the shared self-correction helper |

Each can be added later by registering the member in a new migration and emitting it from `run()`.
