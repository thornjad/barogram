# inverse_pressure_transfer (model 19)

Runs the pressure-tendency transfer relationship backwards. Instead of using pressure
to predict temperature/dewpoint (what pressure_tendency and pressure_trend_cascade do),
this model reads other base models' predicted temperature/dewpoint for the current run
(via `conn_out`) and infers what pressure change would be consistent with that
prediction, using a transfer function trained the same way as
[pressure_trend_cascade](016_pressure_trend_cascade.md)'s but with predictor and target
swapped (variable delta -> pressure delta instead of pressure delta -> variable delta).

Outputs pressure only — this model has nothing new to say about temperature or
dewpoint, it consumes them.

Must run after any base model it reads temperature/dewpoint from, in `barogram.py`'s
`_MODELS` order.

## Members

- **temp_only_inverse** — infers pressure delta from the temperature-only inverse
  transfer function, using the mean predicted temperature delta across other base
  models as the signal
- **dewpoint_only_inverse** — same, using the mean predicted dewpoint delta
  (a moisture-led signal, as opposed to temperature-led)
- **joint_inverse** — average of the two single-variable inverse predictions

## member_id=0

Weighted mean (skill-score weights when available, else equal) + spread across the
three members, per lead.

## Confidence

Every member here gets a confidence value computed against the shared default
fingerprint, matched against its own scored history by calendar day, and
member_id=0's combination is confidence-adjusted via `models/_confidence.py`'s
`combine_pattern`, the same as every other model. This model reads other base
models' `member_id=0` values, already confidence-adjusted, as its own input, then
applies its own confidence on top; that compounding is an accepted, documented
property of "no special case," not an oversight. See [confidence.md](confidence.md).

## Self-correction

Member 4 (`self_correction`) is the standard self-correction member (migration
`065_batch_b_self_correction_members.sql`) — member_id=0 minus this model's own
learned historical bias at each (variable, lead_hours) cell. See
[self_correction.md](self_correction.md).
