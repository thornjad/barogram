# pressure_damped_diurnal (model 17)

Fork of [airmass_diurnal](007_airmass_diurnal.md) that makes the 3h pressure tendency
the primary signal damping (or boosting) the diurnal temperature curve, on the theory
that a falling barometer usually means increasing cloud cover suppressing daytime
heating, and a rising one means clearer skies allowing the full diurnal swing.

Like airmass_diurnal, pressure is intentionally not an output here — only temperature
and dewpoint, and only temperature actually gets adjusted (dewpoint gets base
climatology + anchor, matching airmass_diurnal's own convention).

## Members

- **linear_damp** — amplitude multiplier scaled linearly by the 3h pressure tendency
  rate (`1 + 0.12 * dp_dt`, clamped to [0.3, 1.7])
- **threshold_damp** — binary: if the 24h-projected pressure change (`dp_dt * 24`)
  exceeds ±3 hPa, apply a fixed multiplier (0.7 falling / 1.2 rising); otherwise
  amplitude is untouched
- **airmass_pressure_joint** — clearness index (clear/cloudy) crossed with a
  pressure-trend bucket (rising/steady/falling) into six joint states, each with its
  own fixed multiplier

## member_id=0

Weighted mean (skill-score weights when available, else equal) + spread across the three
members, per variable and lead.

## Confidence

Every member here gets a confidence value computed against the shared default
fingerprint (`air_temp`, `dew_point`, `station_pressure`, `wind_avg`), matched
against its own scored history by calendar day. member_id=0's combination now
multiplies each member's weight by its own confidence (floored, defaulted to the
group's own average when unknown) via `models/_confidence.py`'s `combine_pattern`,
which also fixed a pre-existing bug: a member missing a weight used to collapse the
whole group to a plain average, now only that member is dropped. See
[confidence.md](confidence.md) for the full design.
