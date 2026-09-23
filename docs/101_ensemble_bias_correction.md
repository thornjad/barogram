# ensemble_bias_correction (model 101)

Single-member model that learns a per-`(variable, lead_hours)` bias correction from
`barogram_ensemble`'s (model 100) own past scored history, then applies it to a
same-cycle recomputation of model 100's raw blend.

## Motivation

From the 2026-09-22 "Synoptic Signal Ideas" brainstorm, section 12: `barogram_ensemble`
applies zero correction of its own, so its historical `member_id=0` rows already ARE
the uncorrected blend to learn a systematic bias against, at no extra data-collection
cost.

## Mechanics

Runs one slot before `barogram_ensemble` in `barogram.py`'s `_MODELS`, so it cannot
read model 100's output for the current cycle — that row doesn't exist yet. Instead it:

1. Reads the same `db.ensemble_inputs` (every base model's own `member_id=0` output)
   that model 100 itself reads.
2. Recomputes the identical weighted, confidence-adjusted blend model 100 would
   compute, via the shared `models.ensemble.blend_cells` helper — factored out of
   `models/ensemble.py`'s own `run()` specifically so this math lives in one place.
3. For each `(variable, lead_hours)` cell, looks up model 100's own past scored
   history at that cell (`db.model_signed_error_history`) and takes the mean signed
   `error` (`value - observed`, see `score.py`) as the bias.
4. Emits `raw_blend - bias` as its own value.

## Feedback loop

Writes its own `model_id=101`, `member_id=0` row, scored against real observations
like any other model. `db.sync_ensemble_members` (called by `barogram_ensemble` later
the same cycle) is no longer filtered to `id < 100`, so model 101 registers as an
ordinary member of `barogram_ensemble` the next time it runs, and `db.ensemble_inputs`
already includes it via its `type='base'` row — no `id`-range special-casing needed
either.

Its weight in the ensemble then comes from the normal `tune` skill-score machinery
(see [tune.md](tune.md)): if the correction isn't actually good, it gets down-weighted
automatically, same as any other member.
