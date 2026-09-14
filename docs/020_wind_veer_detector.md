# wind_veer_detector (model 20)

Classifies wind-direction change over a trailing 3h window into veering / backing /
steady — the same classification surface_signs's wind_rotation signal uses — but
without (or with a much lower) minimum wind-speed floor.

## Motivation

The 2026-09-12 dry-airmass intrusion: the wind veered roughly 120° (SW to NW) between
03:00 and 06:00 local, a clean 2+ hour lead on the airmass change that showed up before
the dewpoint itself turned over. It happened entirely at wind speeds of 0.2–1.8 mph
(0.1–0.8 m/s) — well under surface_signs's and synoptic_state_machine's shared 1.5 m/s
floor for trusting wind direction (both call the same `_wind_rotation_category` in
surface_signs.py). That floor meant neither existing model could have used this signal
at all for this event. This model tests whether direction is still informative below it.

## Members

| member_id | name | wind-speed floor |
|-----------|------|-------------------|
| 0 | — | weighted mean of 1–3 |
| 1 | veer_nogate | none (only true zero-wind excluded) |
| 2 | veer_lowgate | 0.3 m/s |
| 3 | veer_gust_confirmed | none, but a veer/back call is discarded unless a coincident gust/avg ratio uptick (> 1.8) also occurs in the same window |

## Algorithm

Each member classifies the net direction change between the oldest and newest valid
observation in a trailing 3h window (`_angular_diff`, reused from surface_signs), then
looks up the historical conditional mean delta for that category, same pattern as
surface_signs's own signal members. member_id=0 is a weighted mean (skill-score weights
when available, else equal) + spread across the three.

## Open question this model is testing

Whether wind direction below the 1.5 m/s floor is signal or noise. `veer_gust_confirmed`
exists specifically to guard against the failure mode where near-calm direction readings
flip randomly — if it scores meaningfully better than `veer_nogate`, that's evidence the
floor exists for a reason and light-wind direction needs a confirming signal, not just a
lower threshold.

## Confidence

Every member here gets a confidence value computed against the shared default
fingerprint, matched against its own scored history by calendar day, and
member_id=0's combination is confidence-adjusted via `models/_confidence.py`'s
`combine_pattern`, the same as every other model. This model is new enough
(migration 041) that its confidence starts at the neutral fallback and its combined
output stays identical to today's until it accumulates enough scored history of its
own, the same graceful-cold-start behavior every newly added member gets. See
[confidence.md](confidence.md) for the full design.
