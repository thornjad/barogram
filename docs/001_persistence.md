# 001 Persistence Model

## Overview

The persistence model is the ultimate naive forecast. It rejects the idea that the climate changes, that weather is transient, and confidently asserts that whatever the conditions are now, they will continue to be the conditions forever. Specifically, it checks the latest observed conditions and forecasts that the same conditions will be present at all future times.

## Lead Times

6, 12, 18, 24 hours from the most recent Tempest observation.

## Members

| member_id | name | description |
|-----------|------|--------------|
| 0 | — | flat carry-forward of the latest observed value (the original model) |
| 1 | trend_persistence | added 2026-09-14: extends the latest value with the last hour's rate of change, decayed toward zero over the lead time (`exp(-λ·lead)`, λ=0.231/h, ~3h half-life) rather than extrapolated forever. Motivated by the 2026-09-12 dry-airmass intrusion, where plain flat persistence (member 0) already scored far better than the ensemble that run simply by not fighting the cooling/drying trend already underway at issue time — this member tests whether leaning into that trend briefly, before fading back to flat, does even better |

## Implementation Notes

The persistence model serves as the baseline against which all other models
are scored. A model with no skill relative to persistence is not useful.

The output database schema stores `issued_at` (when the forecast was
generated) and `valid_at` (when the forecast is for) as Unix epochs.
All rows produced by a single `barogram.py forecast` run share the same
`issued_at`, making it the effective run identifier.

See `migrations/001_baseline.sql` for the full forecasts table schema.

## Confidence

Both member_id=0 (the pressure skill-score reference) and member_id=1
(`trend_persistence`) get their own confidence, computed independently against each
one's own scored history against the shared default fingerprint. Nothing is combined
here, so confidence has no effect on either forecast value; member_id=0 remains
tune's own pressure skill-score reference, unchanged. See
[confidence.md](confidence.md) for the full design.
