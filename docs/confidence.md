# Confidence

## What it is

`weight` (see [tune.md](tune.md)) is a long-run skill score, how good a member has been on average. Confidence is a second, independent signal computed live at forecast time. It is the member's own claim about itself: *in situations like this one I have a lot of close evidence, and I did well on it.*

- **No evidence means 0.** A member with no matched past moments, or none with a scored run of its own, reports exactly 0.0.
- **One match means low.** A perfect record on a single match is low (about 16%) and well above the 0.1% combination floor.
- **High needs both.** A high confidence needs many close matches and good performance on them. It approaches 1 as evidence piles up, and never reaches it. Members spread across the whole range: some near 1% (unusual weather or a poor record), most mid-range, a few high.
- **No correction layer.** Nothing adjusts the number afterward. If it looks wrong, the formula has a bug and gets fixed, since there is nothing else to blame.

```
confidence = q * e(n_eff)
```

- `q` is how well the member did on its matches, a number from 0 to 1.
- `n_eff` is how much close evidence there is, a similarity-weighted match count.
- `e` turns that count into a number from 0 to 1.

Every value in this design that is marked *experimental* below is a first guess meant to change.

## Matching

Every run fingerprints its own current conditions and finds similar past moments (`find_default_matches` in `models/_confidence.py`, computed once per forecast run and shared by every model).

- **Candidates.** One Tempest snapshot per past day, the one nearest today's clock time. A future member that cares about a different time frame, for example a storm member that cares about what happened at 3 am, will bring its own matching.
- **Fingerprint.** 16 Tempest columns plus a 3 hour change in each, weighted by `FEATURE_WEIGHTS` and `TREND_FEATURE_WEIGHTS` and compared with sigma-normalized distance (`_similarity.distance`) divided by the root of the total weight (`distance_norm`), so distance is a root mean weighted squared z and editing a weight never moves the similarity cutoffs. Temperature, dewpoint and pressure carry the most weight. Noisy columns such as battery, lightning, gusts and UV stay in on purpose at low weight, because some members will want them. Relative humidity is out because it is a function of temperature and dewpoint and double counted them. On live data the noisy columns were about 14% of distance among the closest matches before the weights were lowered, and humidity 13%.
- **Pool.** The closest `match_pool(n)` candidates are kept (experimental: 200, or 25% of all candidate days when larger, so a multi-year history keeps its good matches), with no similarity gate at this stage. The pool exists only to bound computation.
- **Similarity** (`default_similarity`, experimental). A match is worth `exp(-(distance / _SIM_SCALE)²)` with `_SIM_SCALE = 0.8`, a number from 0 to 1, and exactly 0 in two cases. One is a distance beyond `_SIM_MAX_DISTANCE` (2.0). Both were widened from 0.6 and 1.1 on 2026-11-12, because live matches were few and each member's similarity mass (`n_eff`) too small, and the backtest scored the wider kernel best. The other is a difference in temperature, dewpoint or pressure from now of more than `_CRITICAL_LIMITS`, however close the rest of the fingerprint is. A past moment that is that different on a variable that defines the airmass says nothing about today. Zero-similarity matches are dropped, so conditions unlike anything recorded return no matches at all.
- **Same-day matches.** Each match is one snapshot of one calendar day, and a day matched more than once counts once.
- **Own matches.** A model with its own way of choosing and weighing matches passes its own list to `confidence_for_cell`. A match is a bare timestamp (similarity 1, which is what `analog` and `full_state_analog` hand in for the days their own forecasts selected) or a `(timestamp, similarity)` pair.

## The calculation

`confidence_for_cell` computes one `(model_id, member_id, variable, lead_hours)` cell from the member's scored history.

1. **Evidence per match.** For each match, average the member's scored runs within `_MATCH_HOUR_TOLERANCE_SEC` of that moment. Runs near the same moment become one piece of evidence, so back-to-back runs never count twice. Matches with no scored run nearby drop out.
2. **Performance per match.** `exp(-2 z²)` where `z` is the match's error divided by the spread (below). Small error against normal variation gives close to 1.
3. **Quantity.** `n_eff` is the sum of the similarities of the matches that have evidence.
4. **Quality.** `q` is the similarity-weighted average of the performances, so close matches count more and `q` says how good the member was, not how much data there is.
5. **Evidence curve** (experimental). `e(n) = n^p / (n^p + k^p)` with `p = 1.5`, a gentle saturating curve, so a typical member lands mid-range instead of bunched near 0 (the earlier cubic needed about 150 close matches and left every member under 1.1%). `e(0)` is exactly 0, `e(k)` is 50% and `e` stays below 1 forever. `k = _EVIDENCE_K0 · sqrt(candidate days / _EVIDENCE_REF_DAYS)`, with `_EVIDENCE_K0 = 3` against 245 days of history (the live measurement of 2026-11-12: median member `n_eff` about 3.75, 90th percentile about 6). `k` grows with the square root of history because `n_eff` itself grows about linearly with it, so the bar rises more slowly than the evidence and confidence still creeps up as data accumulates. It is about 16% at 1, 50% at 3, 74% at 6 and above 99% at 150 matches.

`q` does not grow with data and `e` does, so volume alone never makes a mediocre member confident and a perfect member still needs lots of close evidence.

**Spread.** `spread` is the natural variability of the value itself, lead hours out, on the matched days (`matched_day_spreads`, the population stdev of what the value actually was at the target time). It is the spread of the raw value and not of the change from now to then, because a change-based spread shrinks at short leads and grows at long ones purely from elapsed time, which made every model look unconfident at short leads and confident at long ones. The steep squared-exponential performance curve beat gentler alternatives in a real-data backtest of the ensemble.

**Zero.** The result is exactly 0.0 for a cell with no scored history, a history spanning 5 distinct days or fewer (`_MIN_HISTORY_DAYS`), a cell with no known spread, or no match that has a scored run near it. Every forecast row carries a real number and nothing returns `None`.

## Filled history

The scheduled runs have always forecast leads 6, 12, 18 and 24. The hours between were added on 2026-09-10, so a cell such as +10 h had months less scored history than +12 h and almost no matched moments to learn from. `history_fill.filled_history` (used by `cmd_forecast` in place of the plain history load) adds the missing rows in memory:

- For each run with a scored forecast at both neighbouring anchors (0, 6, 12, 18, 24 hours), the forecast at an in-between lead is the straight line between them, scored against the observation nearest its own target time, the way `score.py` scores a real row.
- Lead 0 is the Tempest observation at issue time, so leads 1 to 5 lie on the line from now to the +6 h forecast.
- A run that has a real scored row at an hourly lead keeps it, and nothing synthetic is added there. Leads 6, 12, 18 and 24 are never synthesized.
- `prune` clears `value` and `observed` after 30 days but keeps `error`, so an older anchor's value is recovered as `error + observation at its valid time`.
- Nothing is written to the database. It affects confidence only. `tune` weights still read the stored rows.

Checked against 25 days of real hourly rows, the line reproduces the real error within about 0.1 degrees from +5 h up, and is too optimistic at +1 to +3 h for a model whose real short-lead shape is not linear (`diurnal_curve` dewpoint at +1 h: 0.48 on the line, 0.89 real). Filling lifts hourly cells from about 1 to 3% to the level of the 6/12/18/24 cells (dewpoint 12 to 25%, temperature 2 to 20%). Matches from before the first forecast (2026-04-10) stay uncovered, since there is nothing to interpolate.

## Member 0 has its own confidence

A multi-member model's member 0 is the model itself, one more member of whatever ensemble it belongs to, and its confidence is its own. It is never an average of its members' confidences. Members' confidences only weight how their values combine into member 0's value.

After the model runs and any self-correction blend, `_member_zero_confidence` in `barogram.py` gives every member 0 row a confidence by the same method any member uses, from member 0's own scored history (the past final values the model stored as member 0). This holds at every level, so the ensemble's headline is its own member 0 and a future model that has `barogram_ensemble` as a member would compute its own too. For single-member models whose member 0 already computed its own, the number is the same.

A model whose member 0 chooses its own evidence sets `STATES_OWN_MEMBER_ZERO_CONFIDENCE = True` and `cmd_forecast` leaves its member 0 alone. Only BOGO does.

## Models that choose their own evidence

Every model builds its members' confidences itself, and most call the shared `confidence_for_cell` with the shared matches. A model can instead hand in its own matches.

BOGO is the prototype. It picks a random subset of the shared matches, of random size (`_random_matches` in `models/bogo.py`), counts each as a perfect match, and states the confidence that follows from how it actually did on those days. That is the same calculation as every other model, with randomized inputs and no signature analysis. It is not seeded, so every call differs. A test that needs repeatability seeds `random` itself. BOGO's member 0 does the same from its own history.

## Injecting confidence into a combination

Every weighted model shares one combination shape. `combine_pattern` in `models/_confidence.py` drops only the members missing a weight, multiplies each remaining member's weight by its confidence (floored at `_CONFIDENCE_FLOOR = 0.001`, so a member reporting 0% still counts for almost nothing without being fully excluded), uses the group's own average for a member whose confidence is unknown or 0.0 when no member's is known, renormalizes, and returns the combined value. It also returns an influence-weighted group confidence, which nothing stores as a model's confidence.

`external_corrected` blends two sources (NWS, Tempest) by inverse MAE, with each source's term also multiplied by its confidence. The four models with no combination step (`persistence`, `climatological_mean`, `nws`, `tempest_forecast`) compute confidence and attach it to the rows they emit.

## Confidence compounding through model chains

`pressure_consensus_transfer` and `inverse_pressure_transfer` read other base models' member 0 pressure values, already confidence-adjusted, and apply their own confidence on top, and their output flows into `barogram_ensemble`, which applies confidence a third time. This is an accepted consequence of leaving the models uniform. It also means their member rows carry a small share of confidence's effect into `tune`'s `huber_delta_per_variable` pool (see [database.md](database.md)), about 0.37% of that pool, which is not filtered because those rows are the two models' own predictions.

## Reading the report

`uv run barogram calibration` is read-only. For each 10-point band of stated confidence it prints the row count, the mean stated confidence, how often the forecasts landed inside a fixed band (2 °F for temperature and dewpoint, 1.5 hPa for pressure) and the mean error. Nothing in it feeds back into the number. Only rows whose confidence is still stored appear (`prune` clears it after 30 days). The band counts double as the spread histogram of confidence across runs.

## Limits

- Data is the limit today. The station has recorded since March 2026, so a member sees a handful of close matches (`n_eff` median about 3.75). The curve is scaled to that, so a typical good member sits around 30 to 50%. Unusual weather (few matches) or a poor record still gives low confidence, and the bar rises slowly with history (see the evidence curve).
- The fingerprint is still crude. Dewpoint trend alone is about 8 to 30% of the distance among close matches because its sigma is tight, and pressure similarity is weakly captured. The weights and cutoffs are experimental. The backtest (`make tune-similarity`, 245 test days, temperature, dewpoint and pressure at +3 h and +12 h, matches weighted by live similarity and each target scored against the no-matching baseline) shows matching beating the baseline at +12 h and roughly tying it at +3 h. The kernel width mattered more than the weight vector: scale 0.8 with cutoff 2.0 scored 0.984 against 1.023 for the old 0.6 and 1.1 (under 1 beats the baseline), and 1.0 scored 0.990. Weight vectors differ by about 0.01 to 0.02, so the choice between them is within noise. The live vector is the meteorologically tiered one (`guess-3`: pressure level 1.0 and its trend at full weight, temperature trend 0.8x, dewpoint trend 0.6x, wind direction 0.6), applied on meteorological grounds because the backtest scored it no better and no worse than the previous vector (`guess-2`).
- A newly added model or member starts at 0.0 (too little history) and counts for almost nothing in any combination until it has scored history of its own.
- Long-lead and low-evidence confidences are tiny, which narrows the absolute differences between members. The ratios between them still weight the combination.
