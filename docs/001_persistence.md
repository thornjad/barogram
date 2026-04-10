# 001 Persistence Model

## Overview

The persistence model is the ultimate naive forecast. It rejects the idea that the climate changes, that weather is transient, and confidently asserts that whatever the conditions are now, they will continue to be the conditions forever. Specifically, it checks the latest observed conditions and forecasts that the same conditions will be present at all future times.

## Brief History

https://journals.ametsoc.org/view/journals/wefo/23/5/2008waf2007037_1.xml

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
