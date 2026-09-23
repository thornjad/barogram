# Confidence

## What it is

`weight` (see [tune.md](tune.md)) is a long-run skill score: how good a member has
been on average. Confidence is a second, independent signal computed live at
forecast time: given how similar current conditions are to specific days in a
member's own scored history, and how well that member did on those specific days,
how much should its number right now be trusted.

Every model and every member has a confidence value — every row in the `members`
table, across every model. This count grows as models gain members (see
[database.md](database.md)'s `members` table and `migrations/`), so treat any
specific total as a snapshot, not a fixed fact. The meta-ensemble
(`barogram_ensemble`) is just another model here, combining its members'
confidence the same way every other multi-member model does.

## Computing confidence

Two models with their own existing pattern-matching (`analog`, `full_state_analog`)
reuse the exact analog days they already selected for their own value forecast, per
member. Every other model uses a shared default fingerprint (`air_temp`, `dew_point`,
`station_pressure`, `wind_avg`, unweighted) computed once per forecast run.

The fingerprint search itself (`find_default_matches`) only counts a historical day as
a real match if it's within `_MATCH_DISTANCE_THRESHOLD` (sigma-normalized distance,
see `_similarity.distance`) of current conditions, keeping at most the closest
`_MATCH_MAX_CANDIDATES`. Conditions genuinely unlike anything recorded so far (a first
winter cold snap against a spring/summer-only dataset, for example) can and should
return zero matches — this isn't a fallback path, it's the expected outcome the first
time a truly novel pattern shows up.

For a `(model_id, member_id, variable, lead_hours)` cell, `models/_confidence.py`'s
`confidence_for_cell` takes each matched day's own nearest-clock-time analog
timestamp and pulls in only this member's scored runs within
`_MATCH_HOUR_TOLERANCE_SEC` of it — not that day's entire scored history. Real forecast
runs land roughly 3 hours apart, so this grabs the one relevant run per matched day
without diluting the pool with runs from unrelated hours.

`blended_confidence` computes this in two independent steps, not one blend:

```
raw   = 1 / (1 + matched_avg / overall_avg_error)
trust = n / (n + _CONFIDENCE_PSEUDOCOUNT)     # n = matched-and-scored sample count
confidence = trust * raw
```

`raw` is what confidence would say with total trust in the evidence — above 0.5 when
this member does *better* than its own typical error on days like today, below 0.5
when it does *worse*. `trust` is a multiplier on that claim, not a blend toward it: it
scales confidence *down toward zero* as evidence thins, rather than blending it toward
a neutral 0.5 guess the way an earlier version of this design did. This is deliberate
and matches the intended meaning of the number: confidence is a model's own claim about
whether to trust it right now, and a claim backed by only a sliver of evidence deserves
to be muted, in *either* direction — a single lucky match can't manufacture high
confidence any more than a single unlucky one can manufacture certainty of failure.
`trust` has no hard ceiling and keeps climbing as `n` grows, so a well-evidenced signal
can reach real confidence near either extreme, not just hover near the middle.

Zero usable evidence — no analog day matched closely enough, or matched days exist but
this member has no scored run near their clock time; both mean the same thing — returns
exactly `0.0` (`trust` is 0, so the formula already gives this without a special case
needed at the trust step; matched_avg being undefined at n=0 is the only reason an early
return still exists). A cell with too little history overall (5 distinct scored days
or fewer) also returns `0.0`: no baseline exists yet to measure this forecast against,
which is itself zero basis for claiming any confidence at all — not a separate "unknown"
state, just the same "nothing to trust yet" answer arrived at from a different gap in
the evidence. Every forecast row therefore carries a real confidence number; nothing in
this pipeline returns `None`. Confidence lands in `[0, 1)` — it approaches but never
reaches exactly `1.0`, since `trust` never reaches exactly 1 for finite n.

Per-model feature weighting on the shared fingerprint (letting a model like
`pressure_tendency` weight `station_pressure` higher when matching analog days) was
considered and deferred: none of these models' members are structured around feature
sensitivity the way `analog.py`'s own members are, so there's no verified case for it
yet. `_similarity.distance` already accepts an optional weight vector, so this stays
cheap to add later if a concrete case shows up.

## Injecting confidence into a combination

Every weighted model in this plan (20 of the 25) shares one combination shape:
`models/_confidence.py`'s `combine_pattern` drops only the members missing a weight,
then multiplies each remaining member's weight by its own confidence (floored at
`_CONFIDENCE_FLOOR = 0.001`, 0.1% — not 10% — so no member is ever driven to fully zero
influence, but a member reporting genuine 0% confidence still counts for almost
nothing, and defaulted to the group's own average when a member's confidence is
unknown, or `0.0` when *no* member in the group has a known confidence at all — same
"nothing to base trust on" case as the zero-evidence path above, not a neutral guess),
renormalizes, and returns both the combined value and an influence-weighted group
confidence.

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
every model's own accuracy has. A newly added model or member starts at `0.0`
confidence (too little history to judge at all, see the `_MIN_HISTORY_DAYS` gate above)
and is floored to near-zero influence in any combination until it accumulates enough
scored history of its own; this is expected graceful-cold-start behavior, not a
shortfall.

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
