# Confidence

## What it is

`weight` (see [tune.md](tune.md)) is a long-run skill score, how good a member has been on average. Confidence is a second, independent signal computed live at forecast time. It answers a narrower question, which is how much to trust this member's number right now, given how it did the last times conditions looked like these.

Confidence is each model's own statement about itself. Most models state it from evidence, using the shared method described below, and BOGO states a seeded random value because random is its algorithm for confidence (see "Models that state their own confidence"). Every row in `members` carries a confidence value on every forecast row. The meta-ensemble (`barogram_ensemble`) is another model here and combines its members' confidence the same way every other multi-member model does.

A confidence of 0.9 means that in situations like this one, forecasts from this kind of evidence landed inside the hit tolerance about 90% of the time, measured over many separate days. Nothing caps the number below 1 and nothing pushes it up. A cell with little history gets a low number or none, and 99% only appears once about 100 or more distinct days of near-perfect hits support it.

## Evidence score

A member's evidence score is how small its errors were on past situations like the current one. It is an internal measurement and is never reported on its own.

**Matching.** Every run fingerprints its own current conditions and finds similar past moments. The shared fingerprint (`find_default_matches`, computed once per forecast run) compares 17 Tempest columns plus a 3 hour trend of each, weighted by `FEATURE_WEIGHTS` and `TREND_FEATURE_WEIGHTS`, using sigma-normalized distance (`_similarity.distance`). A past moment only counts as a match inside `_MATCH_DISTANCE_THRESHOLD`, and at most the closest `_MATCH_MAX_CANDIDATES` are kept, so conditions unlike anything recorded can and should return zero matches. Each match is one snapshot per past calendar day, the one nearest in clock time, which means a 9 am run and a 4 pm run match different moments. `analog` and `full_state_analog` reuse the days their own forecasts already selected instead of the shared set.

**Evidence from a member's history.** For a `(model_id, member_id, variable, lead_hours)` cell, `confidence_for_cell` in `models/_confidence.py` looks at this member's scored runs within `_MATCH_HOUR_TOLERANCE_SEC` of each matched moment. Runs near the same moment are averaged into one piece of evidence, so back-to-back runs never count twice. `n_ev` is the number of matched moments with any scored run near them, and `matched_avg` is the mean of those per-moment errors.

```
z     = matched_avg / spread
score = exp(-2 * z * z)
```

`spread` is the natural variability of the value itself, lead hours out, on the matched days (`matched_day_spreads`, the population stdev of what the value actually was at the target time). It is the spread of the raw value and not of the change from now to then, because a change-based spread shrinks at short leads and grows at long ones purely from elapsed time, which made every model look unconfident at short leads and confident at long ones. `z` near 0 means the member's error is small next to normal variation, and past 1 or 2 means the error is large next to what genuinely different weather looks like. The steep squared-exponential curve beat gentler alternatives (a normal survival function, a logistic) in a real-data backtest of the ensemble.

**Bins.** The score is reduced to three small integers.

- `score_bin` is the score in tenths, 0 to 9.
- `n_bin` records how much evidence stands behind it, 0 for 1 to 3 matched moments, 1 for 4 to 10, 2 for 11 or more.
- `form_bin` records recent form, 0 cold, 1 even, 2 hot. It compares the cell's outcomes that landed in the last 48 hours against its long-run error. Outcomes are averaged within 6 hour valid-time blocks first, so a burst of back-to-back runs counts once, and fewer than 3 blocks reads as even. A ratio under 0.8 is hot and over 1.25 is cold. Only scored outcomes are used, so everything in the window was already observed when the run was issued, and form reads error, never confidence.

`CellHistory.recent_mean` and `form_bin` compute this, cached per cell.

Zero usable evidence returns exactly 0.0, as does a cell with 5 distinct scored days or fewer (`_MIN_HISTORY_DAYS`) or a cell with no known spread. Every forecast row therefore carries a real confidence number and nothing here returns `None`.

## From score to confidence

A forecast is a hit when its absolute error is inside a tolerance, 2 °F (1.11 °C) for temperature and dewpoint and 1.5 hPa for pressure, the same at every lead because an error of 10 degrees is 10 degrees at any lead (`models/_calibration.py`, `tolerance`). Long-lead confidence therefore comes out low, since forecasts really do land inside that band only about 20% of the time at lead 24.

**The tally.** The `calib_counts` table holds, per `(variable, lead_hours, n_bin, form_bin, score_bin, day)`, how many forecasts of that kind were hits and how many there were. `day` is the issue day, so back-to-back runs on one day add to one row. The tally pools every labeled member of every model, because many members share one weather day and a per-member cell would be too thin to trust.

**Lookup.** At each forecast start `db.load_calib_counts` reads the last 365 days and `models/_calibration.py` builds a lookup from it.

- A cell becomes active at 14 distinct days. Its hit rate (`p_hat`) is the mean over days of each day's hit fraction, so a day with thousands of rows weighs the same as a day with one.
- An active cell's value is `(p_hat * days + 1) / (days + 2)`, which makes the effective sample size distinct days. Fourteen days of perfect hits give 0.9375 and 98 days give 0.99.
- For every `(variable, lead_hours, n_bin, form_bin)` a 10 entry vector over score bins is built from the active cells, merged with pool-adjacent-violators so confidence never falls as the score rises (a thin high estimate merges with its neighbors and cannot lift them). Inactive bins take the nearest lower active value.
- A coarse series summed over `n_bin` and `form_bin` fills bins below the lowest active fine cell, never above that cell's value, and stands in entirely when no fine cell is active.
- When nothing is active for a `(variable, lead_hours)`, the lookup returns `None` and `confidence_for_cell` uses the cold-start fallback, `blended_confidence`, which multiplies `score` by `n / (n + _CONFIDENCE_PSEUDOCOUNT)`. The fallback retires cell by cell as the tally fills, and a lookup error also falls back, reported once per run, so a bug cannot take every model down.

Zero evidence is never looked up. A member with a single matched moment gets whatever rate single-moment evidence has actually delivered, which can be well above what the old formula gave it.

## Labeling and counting

The tally needs each forecast's evidence bins at the time it is scored, and the stored `confidence` cannot supply them. It is already the output of the lookup, and `prune` clears it after 30 days. So each stored row keeps `conf_bin`, one packed integer holding the three bins (`models/_calibration.py`, `pack` and `unpack`). `prune` leaves it alone, which means the tally can be rebuilt from rows if the tolerance or the bins ever change.

`confidence_for_cell` logs `(conf_bin, confidence)` for histories that know their model and member ids (`CellHistory` carries them, set in `cmd_forecast`). A leaf with nothing to judge (every zero-evidence return) logs no bin, so its 0.0 is never mistaken for an aggregate and recalibrated. After the model runs and any self-correction blend, `_stamp_calibration` in `barogram.py` copies `conf_bin` onto a row only if its confidence still equals what was logged. A row some later step rewrote, or one that never went through `confidence_for_cell`, stays unlabeled and never enters the tally. `cmd_forecast` prints `labeled N, aggregate M` per model, so a model that quietly stops labeling stands out.

When the scorer scores a labeled row it adds the hit or miss to `calib_counts` in the same transaction, and only if its own update was the one that scored the row, so two overlapping scorers cannot count a row twice. Any migration that resets `scored_at` to null must also clear `conf_bin` on those rows for the same reason.

## Aggregate rows

Member 0 of a multi-member model is a blend, and a weighted mean of its members' rates is not the rate of the blend. After labeling, every member 0 row with a value and a confidence that is not a matching leaf (this covers the self-correction blend and `barogram_ensemble`'s headline) is recalibrated. Its group confidence in tenths is the score bin, `conf_bin` is `100 + score_bin`, and the lookup uses the reserved `n_bin` 3 and `form_bin` 1 cell (fine series only). At scoring time the blend's own error decides hit or miss, so the tally learns how often the blend lands inside tolerance at each group confidence. Until that cell has 14 days, the row keeps its group confidence.

Because base models store recalibrated member 0 confidence and the ensemble reads those stored values, the ensemble's inputs and its own headline are both on the measured scale.

## Models that state their own confidence

Every model builds its members' confidences itself and uses them to weight its own member 0 blend. Most call the shared `confidence_for_cell`. BOGO's members instead each state a seeded random value from 0 to 1 inclusive (`_random_confidence` in `models/bogo.py`, seeded from the run, member, variable and lead so a rerun gives the same numbers), and those values weight BOGO's own blend like any other model's. BOGO never calls `confidence_for_cell`, so its member rows are never labeled or counted, while its member 0 is an aggregate and is recalibrated through the pooled aggregate cell like every other model's.

## Injecting confidence into a combination

Every weighted model shares one combination shape. `combine_pattern` in `models/_confidence.py` drops only the members missing a weight, multiplies each remaining member's weight by its confidence (floored at `_CONFIDENCE_FLOOR = 0.001`, so a member reporting 0% still counts for almost nothing without being fully excluded), uses the group's own average for a member whose confidence is unknown or 0.0 when no member's is known, renormalizes, and returns the combined value with an influence-weighted group confidence.

`external_corrected` blends two sources (NWS, Tempest) by inverse MAE, with each source's term also multiplied by its confidence. The four models with no combination step (`persistence`, `climatological_mean`, `nws`, `tempest_forecast`) compute confidence and attach it to the rows they emit.

## Confidence compounding through model chains

`pressure_consensus_transfer` and `inverse_pressure_transfer` read other base models' member 0 pressure values, already confidence-adjusted, and apply their own confidence on top, and their output flows into `barogram_ensemble`, which applies confidence a third time. This is an accepted consequence of leaving the models uniform. It also means their member rows carry a small share of confidence's effect into `tune`'s `huber_delta_per_variable` pool (see [database.md](database.md)), about 0.37% of that pool, which is not filtered because those rows are the two models' own predictions.

## Reading the calibration report

`uv run barogram calibration` prints, for labeled rows, each 10-point band of stated confidence with its row count, mean stated confidence and observed hit rate. A calibrated band has a hit rate close to its mean stated confidence. A second table counts unlabeled rows per band (BOGO's members, `raw_mix`, copied ensemble rows, rows from before the calibration migration), which are not checkable against the tally. The band counts also serve as the spread histogram of confidence across runs. The report states what happened and aims at no particular shape.

## Limits

- The tolerance is fixed. Raising it for longer leads would keep long-lead rates more discriminating but changes what a stated percentage means, and it is one function (`tolerance`) to change.
- Calibration is pooled across models, so a model that overstates itself is only corrected through its own evidence score, not by a per-model offset. A per-model dimension would plug into the same table.
- The tally covers the last 365 days. The score is relative to the season's variability while a hit is an absolute band, so a season very different from the tally's recent past will read slightly off until it fills in.
- Long-lead confidences are low and compressed together, which narrows the ratio between members' confidences that weights the ensemble blend at those leads.
- A newly added model or member starts at 0.0 (too little history) and counts for almost nothing in any combination until it has scored history of its own.
