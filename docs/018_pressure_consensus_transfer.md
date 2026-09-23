# pressure_consensus_transfer (model 18)

Reads other base models' own predicted pressure for the current run (via `conn_out`)
and blends them into a private consensus, then feeds that consensus's predicted delta
through a transfer function (see
[pressure_trend_cascade](016_pressure_trend_cascade.md)) to forecast temperature and
dewpoint. Also emits the consensus itself as this model's own pressure forecast.

Unlike `barogram_ensemble` — which combines every base model's `member_id=0` forecast
per variable independently — this model cross-pollinates: a pressure consensus built
from several sources feeds a prediction for a *different* variable.

Must run after any base model it reads pressure from, in `barogram.py`'s `_MODELS`
order.

## Members

- **simple_mean_consensus** — unweighted mean of source models' predicted pressure at
  each lead
- **spread_aware** — same consensus mean, but the transferred temperature/dewpoint
  delta is damped in proportion to how much the sources disagree
  (`damp = 1 - min(1, spread / 3.0 hPa)`)
- **best_model_only** — passes through a single source's prediction, picked by a
  fixed priority order (synoptic_state_machine, full_state_analog, analog,
  climatological_mean, persistence, weighted_climatological_mean, climo_deviation,
  pressure_tendency, diurnal_curve, bogo) — first one present at that lead wins

## member_id=0

Weighted mean (skill-score weights when available, else equal) + spread across the
three members, per variable and lead.

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
