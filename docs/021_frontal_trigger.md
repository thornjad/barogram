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
| 0 | — | weighted mean of 1–4 |
| 1 | ptend_veer_strict | joint (pressure-tendency direction, wind-veer category) state, only when **both** are actively signaling (non-"steady") — high precision, fires rarely |
| 2 | ptend_veer_loose | same joint state, fires when **either** signal is actively signaling (the other may be steady or unavailable, tagged "unknown") — earlier and more frequent than member 1, lower precision; direct comparison point |
| 3 | ptend_veer_weighted | continuous blend of the two signals' own *marginal* (not joint) conditional-mean deltas, weighted 1.0 when actively signaling vs 0.3 when steady — never abstains for a novel joint combination the way members 1–2 can |
| 4 | front_type_archetype | classifies pressure rate, wind veer, air-temp trend, and precip duration jointly into one `cold_frontal` / `warm_frontal` archetype state (or `None`) over the same 3h window — richer than 1–3's plain pressure-tendency+veer joint |

## Signals

- **Pressure tendency**: 3h delta classified with pressure_tendency's own
  `_zambretti_category` (rapid_rise/slow_rise/steady/slow_fall/rapid_fall), collapsed to
  rising/falling/steady for members 1–3 — magnitude doesn't matter there, only direction.
  Member 4 uses the uncollapsed 5-bucket category instead, since it needs rapid vs slow
  to distinguish a cold-frontal fall from a warm-frontal one.
- **Wind veer**: `wind_veer_detector`'s ungated `_rotation_category` (veering/backing/
  steady) over the same 3h window.
- **Air temp trend** (member 4 only): 3h `air_temp` delta, classified `sudden_drop`
  (≤ -2.0°C), `gradual_rise` (≥ +0.5°C), or `steady`.
- **Precip duration** (member 4 only): fraction of the 3h window with `precip` above
  zero, classified `none`, `brief` (< 34% of the window), or `steady` (≥ 34%).

## Member 4: front-type archetype

Cold-frontal signature: `rapid_fall` pressure + active veer (veering or backing) +
`sudden_drop` temp + precip not `steady` (none or brief only — a cold front's precip,
if any, is a short burst, not sustained rain). Warm-frontal signature: `slow_fall`
pressure + `steady` veer (minimal direction change) + `gradual_rise` temp + precip not
`brief` (none or steady only — a warm front's precip, if any, is steady light rain, not
a burst). Any other combination, or any missing signal, resolves to `None` and the
member abstains for that observation. Best leads are roughly 3–18h, matching how long a
frontal passage's temperature/dewpoint signature typically takes to play out; leads
outside that range simply thin out under `_MIN_SAMPLES` rather than being hard-excluded
in code.

## member_id=0

Weighted mean (skill-score weights when available, else equal) + spread across the
four members, per variable and lead — same pattern as the other pressure-family models.

## Confidence

Every member here gets a confidence value computed against the shared default
fingerprint, matched against its own scored history by calendar day, and
member_id=0's combination is confidence-adjusted via `models/_confidence.py`'s
`combine_pattern`, the same as every other model. Members 1–3 date to migration 041;
member 4 (migration 049) starts at the neutral fallback and its contribution to
member_id=0 stays negligible until it accumulates enough scored history of its own,
the same graceful-cold-start behavior every newly added member gets. See
[confidence.md](confidence.md) for the full design.

## Self-correction

Member 5 (`self_correction`) is the standard self-correction member (migration
`065_batch_b_self_correction_members.sql`) — member_id=0 minus this model's own
learned historical bias at each (variable, lead_hours) cell. See
[self_correction.md](self_correction.md).
