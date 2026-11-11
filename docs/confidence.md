# Confidence

## What it is

`weight` (see [tune.md](tune.md)) is a long-run skill score, how good a member has been on average. Confidence is a second, independent signal computed live at forecast time. It is the member's own claim about itself: *in situations like this one I have a lot of close evidence, and I did well on it.*

- **No evidence means 0.** A member with no matched past moments, or none with a scored run of its own, reports exactly 0.0.
- **One match means almost 0.** Even a perfect record on a single match is a tiny number.
- **High needs both.** A high confidence needs many close matches and good performance on them. It approaches 1 as evidence piles up, and never reaches it.
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
- **Pool.** The closest `_MATCH_POOL` (experimental, 200) candidates are kept, with no similarity gate at this stage. The pool exists only to bound computation.
- **Similarity** (`default_similarity`, experimental). A match is worth `exp(-(distance / _SIM_SCALE)²)`, a number from 0 to 1, and exactly 0 in two cases. One is a distance beyond `_SIM_MAX_DISTANCE`. The other is a difference in temperature, dewpoint or pressure from now of more than `_CRITICAL_LIMITS`, however close the rest of the fingerprint is. A past moment that is that different on a variable that defines the airmass says nothing about today. Zero-similarity matches are dropped, so conditions unlike anything recorded return no matches at all.
- **Same-day matches.** Each match is one snapshot of one calendar day, and a day matched more than once counts once.
- **Own matches.** A model with its own way of choosing and weighing matches passes its own list to `confidence_for_cell`. A match is a bare timestamp (similarity 1, which is what `analog` and `full_state_analog` hand in for the days their own forecasts selected) or a `(timestamp, similarity)` pair.

## The calculation

`confidence_for_cell` computes one `(model_id, member_id, variable, lead_hours)` cell from the member's scored history.

1. **Evidence per match.** For each match, average the member's scored runs within `_MATCH_HOUR_TOLERANCE_SEC` of that moment. Runs near the same moment become one piece of evidence, so back-to-back runs never count twice. Matches with no scored run nearby drop out.
2. **Performance per match.** `exp(-2 z²)` where `z` is the match's error divided by the spread (below). Small error against normal variation gives close to 1.
3. **Quantity.** `n_eff` is the sum of the similarities of the matches that have evidence.
4. **Quality.** `q` is the similarity-weighted average of the performances, so close matches count more and `q` says how good the member was, not how much data there is.
5. **Evidence curve** (experimental). `e(n) = 1 − (1 − cubic(n)) · (1 − base(n))`, with `cubic(n) = n³ / (n³ + K)` and `base(n) = 0.02 · n / (n + 1)`. The base term makes one match worth about 1% (well above the 0.1% combination floor) and zero matches exactly 0. `K` is solved so `e` reaches exactly 99% at `_EVIDENCE_FULL_AT` (150) and stays below 1 forever. It is about 1% at 1, 4% at 10, 32% at 25, 79% at 50 and 97% at 100.

`q` does not grow with data and `e` does, so volume alone never makes a mediocre member confident and a perfect member still needs lots of close evidence.

**Spread.** `spread` is the natural variability of the value itself, lead hours out, on the matched days (`matched_day_spreads`, the population stdev of what the value actually was at the target time). It is the spread of the raw value and not of the change from now to then, because a change-based spread shrinks at short leads and grows at long ones purely from elapsed time, which made every model look unconfident at short leads and confident at long ones. The steep squared-exponential performance curve beat gentler alternatives in a real-data backtest of the ensemble.

**Zero.** The result is exactly 0.0 for a cell with no scored history, a history spanning 5 distinct days or fewer (`_MIN_HISTORY_DAYS`), a cell with no known spread, or no match that has a scored run near it. Every forecast row carries a real number and nothing returns `None`.

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

- Data is the limit today. The station has recorded since March 2026, so most members see a handful of close matches and sit at low confidence. That is the intended behavior, since low data means low confidence, and it rises as scored history and recorded weather accumulate.
- The fingerprint is still crude. Dewpoint trend alone is about 8 to 30% of the distance among close matches because its sigma is tight, and pressure similarity is weakly captured. The weights and cutoffs are experimental. The backtest (`make tune-similarity`, 244 test days, temperature, dewpoint and pressure at +3 h and +12 h, each scored against the no-matching baseline) shows matching barely beating the baseline at +12 h (ratios about 0.92 to 0.98) and not beating it at +3 h (about 1.03 to 1.12), and the weight vectors tried differ by about 0.01 to 0.02 overall, so the choice between them is within noise for now. The live vector is the meteorologically tiered one (`guess-3`: pressure level 1.0 and its trend at full weight, temperature trend 0.8x, dewpoint trend 0.6x, wind direction 0.6), applied on meteorological grounds because the backtest scored it no better and no worse than the previous vector (`guess-2`).
- A newly added model or member starts at 0.0 (too little history) and counts for almost nothing in any combination until it has scored history of its own.
- Long-lead and low-evidence confidences are tiny, which narrows the absolute differences between members. The ratios between them still weight the combination.
