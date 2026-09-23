# Standard self-correction member

`models/_self_correction.py` is a shared helper, not a model of its own. Any base
model can call it to add one more ordinary member: that model's own member_id=0
value, minus a learned bias from that same model's own historical scored error at
the same (variable, lead_hours) cell.

This is the same idea `models/external_corrected.py` and
`models/ensemble_bias_correction.py` already use — bias is the mean signed `error`
(`value - observed`) over a model's own scored history for a cell, and the
corrected value is `raw_value - bias` — generalized into a template so any model
can add it without re-deriving the bias math.

## Why it's a member, not a change to member_id=0

The correction earns its own weight, confidence, and error through the existing
tune/scoring machinery, exactly like any other member. If a model's own bias isn't
stable enough to be worth correcting for, its self-correction member simply scores
worse and fades toward zero weight — it never overrides member_id=0's own value.

## Gating

Gated behind `_MIN_SAMPLES` (3) scored rows in the trailing `_WINDOW_DAYS` (60): a
cell with too little scored history yet returns `value=None` rather than applying a
correction learned from noise, the same cold-start behavior every newly added
member gets.

## Eligibility

Every base model is a candidate except: `persistence` and `climatological_mean`
(baselines with nothing to self-correct against), `barogram_ensemble` (has its own
dedicated model, `ensemble_bias_correction`), and `nws` / `tempest_forecast` /
`external_corrected` (external sources, `external_corrected` already applies its
own historical-bias correction per source/strategy).

Landed so far: `climo_deviation` (member 55), `dewpoint_tendency` (member 4),
`solar_ramp` (member 3) — migration `058_standard_self_correction_members.sql` —
`bogo` (member 70) — migration `063_bogo_self_correction_member.sql` — and
`weighted_climatological_mean` (member 13), `pressure_tendency` (member 12),
`diurnal_curve` (member 42), `analog` (member 9), `dry_airmass_diurnal`
(member 8), `full_state_analog` (member 18), `multivariate_trend` (member 17),
`surface_signs` (member 5), `pressure_trend_cascade` (member 7),
`pressure_damped_diurnal` (member 4) — migration
`064_self_correction_batch_a.sql`.

Batch B (migration `065_batch_b_self_correction_members.sql`):
`wind_veer_detector` (member 6), `frontal_trigger` (member 5), `regime_stability`
(member 2), `diurnal_rate_anomaly` (member 3), `storm_trajectory` (member 6),
`pressure_trajectory` (member 6), `pressure_consensus_transfer` (member 4),
`inverse_pressure_transfer` (member 4), `radiational_cooling` (member 4),
`moisture_trajectory` (member 6), `synoptic_state_machine` (member 97),
`airmass_diurnal` (member 21).

This closes out every model on the original eligibility list — no remaining
eligible models unwired.

## Adding it to another existing model

1. `import models._self_correction as _self_correction`
2. Add `NEEDS_CONN_OUT = True` and accept `conn_out=None` in `run()`.
3. Pick the model's next open member_id (never an already-scored slot) and add it
   to whichever list feeds that model's per-cell confidence lookup — but NOT to
   the list that feeds member_id=0's own mean, since the correction is derived
   from that mean and must never feed back into it.
4. Right after building a cell's member_id=0 row, call
   `_self_correction.corrected_value(conn_out, MODEL_ID, variable, lead, mean, issued_at)`
   and append a row with the new member_id and that value.
5. Register the member in a new migration:
   `insert or ignore into members (model_id, member_id, name) values (<id>, <mid>, 'self_correction');`

A brand-new model built after this convention lands should add its self-correction
member as its first member (member_id 1) rather than retrofitting one in later.
