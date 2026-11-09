# Standard self-correction member

`models/_self_correction.py` is a shared helper, not a model of its own. Any model
can call it to add one more ordinary member: that model's own raw mix (the value it
would output without self-correction), minus a learned bias from that same raw
mix's historical scored error at the same (variable, lead_hours) cell.

Bias is the mean signed `error` (`value - observed`) over the raw mix's scored
history for a cell, and the corrected value is `raw_value - bias`.

## How it takes part in the model's output

A model's `run()` produces its ordinary members, a member 0 that is their weighted
mix (the **raw mix**), and its self-correction (SC) member rows. Two things then
make the SC member part of the model's final output, with no model code:

1. **The raw mix is stored as its own scored member**, `raw_mix`, at member id `-1`
   (`db.RAW_MIX_ID`). `corrected_value` learns its bias from that member's history,
   not from member 0, because member 0 holds the *final* value, which already
   contains the correction and would teach it back to itself. A model without a
   `raw_mix` member still has a raw member 0 and learns from that.
2. **`sc_blend.apply` replaces member 0 with the raw mix blended with the SC
   members** (in `cmd_forecast`, between `model.run()` and `db.insert_forecasts`).
   The raw mix already carries every other member's weight, so its weight is the
   sum of those members' stored tune weights; each SC member enters at its own
   stored tune weight. The blend is `_confidence.combine_pattern` over those terms,
   with the raw mix's confidence being the model's own member-0 confidence and the
   SC member's confidence its own. The model's confidence is what the blend returns.

The weights are the ones `tune` already computes for every member, SC included, so
the SC member earns its share like any other member: an SC that doesn't help scores
worse and carries a small weight. Nothing here clamps or floors a weight; a cell with
no weight basis yet (no stored weights for the other members or for any SC member)
is left exactly as the model produced it.

`raw_mix` sits at a non-positive id so everything that already skips aggregate rows
skips it: `tune` ignores `member_id <= 0`, the Huber delta pool and the dashboard's
member lists use `member_id > 0`.

A model takes part when `members` holds `(model_id, -1, 'raw_mix')`; its SC members
are the members named `self_correction` or `self_correction_fast`. If `sc_blend.apply`
raises for a model, `cmd_forecast` inserts the model's unblended rows and reports it
in the "complete with errors" line.

Accepted approximation: a direct blend of every member would give each member
influence `weight * confidence`, while the raw mix enters as one term whose
confidence is its group confidence, which is at least the weighted mean of its
members' confidences. The raw mix is therefore slightly favored over SC when member
confidences differ.

## Gating

Gated behind `_MIN_SAMPLES` (3) scored rows in the trailing window: a cell with too
little scored history yet returns `value=None` rather than applying a correction
learned from noise, the same cold-start behavior every newly added member gets. An
SC member with no value is skipped by the blend.

## Window

`corrected_value` takes an optional `window_days` argument, defaulting to
`_WINDOW_DAYS` (60). A model may add a second member calling it with a shorter
window (e.g. 14 days) to react faster to an active drift than the 60-day default
allows -- the default stays 60 for every existing caller that doesn't pass it.

`climo_deviation` (member 56) and `weighted_climatological_mean` (member 14) each
add a `self_correction_fast` member using `window_days=14`, since this project's
own seasonal-transition findings (models 2/3/4's warm/moist bias) traced to the
default window lagging an active drift, specifically on these two models. Not a
blanket rollout -- other models keep the single 60-day member.

## Eligibility

Every base model is a candidate except: `persistence` and `climatological_mean`
(baselines with nothing to self-correct against), and `nws` / `tempest_forecast` /
`external_corrected` (external sources, `external_corrected` already applies its
own historical-bias correction per source/strategy). The ensemble,
`barogram_ensemble`, follows the same pattern with its own SC member (member 101).

## Adding it to another model

1. `import models._self_correction as _self_correction`
2. Add `NEEDS_CONN_OUT = True` and accept `conn_out=None` in `run()`.
3. Pick the model's next open member_id (never an already-scored slot) and add it
   to whichever list feeds that model's per-cell confidence lookup — but NOT to
   the list that feeds member_id=0's own mean: member 0 is the raw mix, and the
   blend adds the SC member afterwards.
4. Right after building a cell's member_id=0 row, call
   `_self_correction.corrected_value(conn_out, MODEL_ID, variable, lead, mean, issued_at)`
   and append a row with the new member_id and that value. Pass `window_days=N` to
   add a fast-window variant as an additional member instead of (or alongside) the
   default-window one.
5. Register the members in a new migration: the SC member
   (`insert or ignore into members (model_id, member_id, name) values (<id>, <mid>, 'self_correction');`)
   and the raw mix
   (`insert or ignore into members (model_id, member_id, name) values (<id>, -1, 'raw_mix');`).
   Registering `raw_mix` is what turns on the blend for the model.
6. The model must set `NEEDS_WEIGHTS`, `NEEDS_MATCH_HISTORY` and `NEEDS_CONN_OUT`.

A brand-new model built after this convention lands should add its self-correction
member as one of its first members rather than retrofitting one in later.
