# 001 Persistence Model

## Overview

## How It Works

The persistence model assumes conditions do not change: the forecast value
for any future time equals the most recently observed value.

## Brief History

## Strengths

## Known Failure Modes

## Variables Forecast

| Variable    | Unit | Source column      |
|-------------|------|--------------------|
| temperature | °C   | air_temp           |
| humidity    | %    | relative_humidity  |
| pressure    | mb   | station_pressure   |
| wind_speed  | m/s  | wind_avg           |

## Lead Times

6, 12, 18, 24 hours from the most recent Tempest observation.

## Implementation Notes

The persistence model serves as the baseline against which all other models
are scored. A model with no skill relative to persistence is not useful.

The output database schema stores `issued_at` (when the forecast was
generated) and `valid_at` (when the forecast is for) as Unix epochs.
All rows produced by a single `barogram.py forecast` run share the same
`issued_at`, making it the effective run identifier.

See `migrations/001_init.sql` for the full forecasts table schema.
