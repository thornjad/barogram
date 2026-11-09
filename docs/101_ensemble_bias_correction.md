# ensemble_bias_correction (model 101) — retired

Model 101 learned a per-`(variable, lead_hours)` bias correction from
`barogram_ensemble`'s (model 100) scored history and applied it to a same-cycle
recomputation of model 100's raw blend. It re-entered model 100 as an ordinary
member (member 101) with its own weight and confidence.

It is now folded into model 100 and works the same way as every base model's
self-correction (see [self_correction.md](self_correction.md)):

- Model 100's member 0 is the raw blend of the base models.
- Member 101 of model 100 is the ensemble's `self_correction` member: the raw blend
  minus a bias learned from model 100's stored `raw_mix` history (member -1).
- `sc_blend.apply` stores the raw blend as `raw_mix` and replaces member 0 with the
  raw blend blended with member 101 at its stored tune weight.

This also fixes what model 101 got wrong: it learned its bias from model 100's
member-0 history, which already contained its own contribution. It now learns from
`raw_mix`, which never does.

Model 101 stops emitting at the cutover (migration `068_raw_mix_member.sql`). Its
historical rows, its row in `models`, and member 101's history and tuned weights in
model 100 are kept as they were.
