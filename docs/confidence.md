# Confidence

## What it is

`weight` (see [tune.md](tune.md)) is a long-run skill score: how good a member has
been on average. Confidence is a second, independent signal computed live at
forecast time: given how similar current conditions are to specific days in a
member's own scored history, and how well that member did on those specific days,
how much should its number right now be trusted.

Every model and every member has a confidence value, 373 members in total against
the live database. The meta-ensemble (`barogram_ensemble`) is just another model
here, combining its members' confidence the same way every other multi-member
model does.

## Computing confidence

Two models with their own existing pattern-matching (`analog`, `full_state_analog`)
reuse the exact analog days they already selected for their own value forecast, per
member. Every other model uses a shared default fingerprint (`air_temp`, `dew_point`,
`station_pressure`, `wind_avg`, unweighted) computed once per forecast run.

For a `(model_id, member_id, variable, lead_hours)` cell, `models/_confidence.py`'s
`confidence_for_cell` buckets that member's scored history by calendar day, then for
each matched analog day pulls the entire day's bucket into the matched-error pool
(not just the nearest single scored run). The matched-day average error, compared
against the cell's own overall average error, maps to a confidence value in `(0, 1]`.
A cell with too little history (5 distinct days or fewer) returns `None`; a group
where every member reports the same confidence value reproduces today's exact
combination regardless of what that value is, since only the *spread* across a
group's members moves a combined value at all.

Per-model feature weighting on the shared fingerprint (letting a model like
`pressure_tendency` weight `station_pressure` higher when matching analog days) was
considered and deferred: none of these models' members are structured around feature
sensitivity the way `analog.py`'s own members are, so there's no verified case for it
yet. `_similarity.distance` already accepts an optional weight vector, so this stays
cheap to add later if a concrete case shows up.

## Injecting confidence into a combination

Every weighted model in this plan (20 of the 25) shares one combination shape:
`models/_confidence.py`'s `combine_pattern` drops only the members missing a weight,
then multiplies each remaining member's weight by its own confidence (floored at 0.1
so no member is ever driven to exactly zero influence, and defaulted to the group's
own average when a member's confidence is unknown), renormalizes, and returns both
the combined value and an influence-weighted group confidence.

`combine_pattern` also fixed a real, pre-existing bug independent of confidence: 16
of these 20 models used to fall back to a plain equal average for the *entire* group
the moment any one member lacked a weight, instead of dropping just that member. The
other 4 (`synoptic_state_machine`, `multivariate_trend`, `barogram_ensemble`, and
`bogo` after gaining real weights for the first time under this work) already worked
the drop-only-the-missing-member way.

The remaining two shapes: `external_corrected` (Pattern C) blends two *sources*
(NWS, Tempest) by inverse-MAE, with each source's inverse-MAE term additionally
multiplied by that source's own confidence; and the four Pattern E models
(`persistence`, `climatological_mean`, `nws`, `tempest_forecast`) have no combination
step at all, confidence is computed and attached to whichever row(s) they emit,
nothing is combined.

## Confidence compounding through model chains

`pressure_consensus_transfer` and `inverse_pressure_transfer` read other base models'
`member_id=0` pressure values, already confidence-adjusted, then apply their own
confidence on top; their output flows into `barogram_ensemble`, applying confidence a
third time. This is an accepted property of "no special case," not an oversight.

This also means their own `member_id > 0` rows carry a small, ongoing share of
confidence's effect into `tune`'s `huber_delta_per_variable` pool (see
[database.md](database.md)), measured at 0.37% of that pool. It isn't filtered out,
since those rows are those two models' own genuine predictions, not mirrored
duplicates the way `barogram_ensemble`'s rows are.

## What this doesn't do

**Calibration** (whether a member's confidence claims actually track its real error
over time) is not built here. It's a second-order question needing real confidence
history to design against, which is exactly why every `forecasts` row stores its own
`confidence` value regardless of whether anything reads it yet.

Confidence quality is expected to improve as scored history accumulates, the same way
every model's own accuracy has. The three newest models
(`wind_veer_detector`/`frontal_trigger`/`dewpoint_tendency`, added in migration 041)
start with confidence at the neutral fallback and their combined output identical to
today's, until they accumulate enough scored history of their own; this is expected
graceful-cold-start behavior, not a shortfall.

## Verifying the mechanism, not just the wiring

A real, separate concern found during review: with only a handful of matched analog
days per cell, a confidence estimate can be statistically indistinguishable from one
computed against random, non-matching days. Before shipping the combination-changing
parts of this work, a merge-time spot-check (not a permanent suite test) confirmed
confidence actually varies across members for a mature cell, and that a member with
visibly worse matched-day error came out with lower confidence, not flat or backwards.
Barogram's history is short everywhere, the same constraint every model in this
ensemble already works under; this is a sanity check that the mechanism points the
right direction, not a statistical proof it already beats noise.
