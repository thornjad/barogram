# frontal_trigger (model 21)

Joint pressure-tendency + wind-veer trigger. Combines the two earliest precursors
found in the 2026-09-12 dry-airmass intrusion into a single signal.

## Motivation

Pressure tendency and wind veer were both visible 2+ hours before the 06:00 run that
missed the 2026-09-12 dry-airmass intrusion, well before dewpoint itself moved. Neither
signal alone was fully convincing that early; this model tests whether combining them
gives more confidence and an earlier usable trigger than either alone.

This stays a separate model rather than adding wind-rotation members to
synoptic_state_machine — see that model's own 2026-08-20 finding that wind rotation
hurts specifically as a *joint* dimension there (six wind-joint members all rank in the
bottom half). frontal_trigger is a narrow, two-signal joint state instead of one more
dimension folded into a much larger combinatorial space.

## Members

| member_id | name | logic |
|-----------|------|-------|
| 0 | — | weighted mean of 1–3 |
| 1 | ptend_veer_strict | joint (pressure-tendency direction, wind-veer category) state, only when **both** are actively signaling (non-"steady") — high precision, fires rarely |
| 2 | ptend_veer_loose | same joint state, fires when **either** signal is actively signaling (the other may be steady or unavailable, tagged "unknown") — earlier and more frequent than member 1, lower precision; direct comparison point |
| 3 | ptend_veer_weighted | continuous blend of the two signals' own *marginal* (not joint) conditional-mean deltas, weighted 1.0 when actively signaling vs 0.3 when steady — never abstains for a novel joint combination the way members 1–2 can |

## Signals

- **Pressure tendency**: 3h delta classified with pressure_tendency's own
  `_zambretti_category` (rapid_rise/slow_rise/steady/slow_fall/rapid_fall), collapsed to
  rising/falling/steady for this model — magnitude doesn't matter here, only direction.
- **Wind veer**: `wind_veer_detector`'s ungated `_rotation_category` (veering/backing/
  steady) over the same 3h window.

## member_id=0

Weighted mean (skill-score weights when available, else equal) + spread across the
three members, per variable and lead — same pattern as the other pressure-family models.

## Confidence

Every member here gets a confidence value computed against the shared default
fingerprint, matched against its own scored history by calendar day, and
member_id=0's combination is confidence-adjusted via `models/_confidence.py`'s
`combine_pattern`, the same as every other model. This model is new enough
(migration 041) that its confidence starts at the neutral fallback and its combined
output stays identical to today's until it accumulates enough scored history of its
own, the same graceful-cold-start behavior every newly added member gets. See
[confidence.md](confidence.md) for the full design.
