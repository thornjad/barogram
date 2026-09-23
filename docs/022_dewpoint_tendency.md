# dewpoint_tendency (model 22)

Mirrors pressure_tendency's regression approach, but applied to dewpoint's own
trailing slope instead of pressure. Forecasts dewpoint only.

## Motivation

During the 2026-09-12 dry-airmass intrusion, the earliest the dewpoint itself showed
anything was a peak-and-turn at 05:15 local — about 45 minutes before the 06:00 run,
later than the pressure and wind-veer precursors (see pressure_trend_cascade and
wind_veer_detector), but still nothing in the ensemble picked up on it, because no
member actually extrapolated dewpoint's own recent trend. Every existing dewpoint model
either holds it flat (persistence-like) or anchors to a diurnal/climatological curve.
This model's whole job is: what did the last hour or three actually do, and where is
that headed — nothing more.

Reuses pressure_tendency's polynomial-fit, exponential-weighting, and OU mean-reversion
functions directly — none of that numerics is pressure-specific.

## Members

| member_id | name | window | recency weighting |
|-----------|------|--------|--------------------|
| 0 | — | weighted mean of 1–3 | — |
| 1 | linear_1h | 1h | none |
| 2 | linear_3h | 3h | none |
| 3 | linear_3h_hl45 | 3h | 45-minute half-life |
| 4 | self_correction | — | standard self-correction member, see [self_correction.md](self_correction.md) |

## Algorithm

Degree-1 (linear) fit of dewpoint against time over the member's window, extrapolated to
each lead, then OU mean-reverted (λ=0.10/h, same as pressure_tendency) toward the
station's all-time mean dewpoint — so a strong short-term slope taper toward
climatology rather than running away indefinitely at 24h lead.

## member_id=0

Weighted mean (skill-score weights when available, else equal) + spread across the
three members.

## Confidence

Every member here gets a confidence value computed against the shared default
fingerprint, matched against its own scored history by calendar day, and
member_id=0's combination is confidence-adjusted via `models/_confidence.py`'s
`combine_pattern`, the same as every other model. This model is new enough
(migration 041) that its confidence starts at the neutral fallback and its combined
output stays identical to today's until it accumulates enough scored history of its
own, the same graceful-cold-start behavior every newly added member gets. See
[confidence.md](confidence.md) for the full design.
