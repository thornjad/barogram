# airmass_diurnal (model 7)

Scales the climatological diurnal temperature curve by solar clearness index and airmass signals derived entirely from Tempest PWS observations.

## Motivation

The existing `diurnal_curve` model fits a good average daily temperature cycle but has no way to distinguish a clear-sky day from an overcast one. On a sunny spring day with dry southerly flow, the afternoon high can run 10–15°F above the average diurnal peak. `airmass_diurnal` attempts to capture this using:

- **Solar clearness index (k)**: ratio of observed solar radiation to theoretical clear-sky irradiance. k ≈ 1 means clear; k ≈ 0 means overcast.
- **Dewpoint depression (T − Td)**: large gap means dry air, which heats faster under solar radiation.
- **Wind direction sector**: rough proxy for airmass origin (southerly = warm, northerly = cold, etc.).
- **Morning warmup rate**: how fast temperature is rising before forecast issuance relative to climatological expectation.

## Core mechanism

1. Compute 30-day hourly climatology from Tempest observations.
2. Evaluate the climatological temperature at each `valid_at` hour.
3. Anchor to current observations: `T_base = T_climo_valid + (T_obs − T_climo_now)`.
4. Add a member-specific amplitude adjustment (affects temperature only).

The clearness-based adjustment amplifies or dampens the deviation of the forecast hour from the daily mean:

```
dev = T_climo_valid − T_daily_mean
T_adj = dev × (k − K_MEAN) × K_SENSITIVITY
```

Positive `dev` (afternoon peak): clear sky boosts the forecast upward. Negative `dev` (overnight trough): clear sky pulls the forecast downward. This preserves the daily mean while stretching the amplitude on clear days.

## Members

| ID | Name | Signal |
|----|------|--------|
| 0 | ensemble mean | weighted average of members 1–16 |
| 1 | clearness-only | k persisted at issued time scales diurnal amplitude |
| 2 | clearness+dewpoint | k × normalized dewpoint depression factor |
| 3 | clearness-pressure-projected | k adjusted forward via pressure tendency (dP/dt) |
| 4 | wind-sector-only | 8-sector wind direction constant temperature offset |
| 5 | wind+clearness | sector offset + clearness scaling combined |
| 6 | morning-warmup-rate | recent T rise rate scales afternoon amplitude |
| 7 | dewpoint-only | dewpoint depression anomaly, afternoon hours only |
| 8 | combined-full | k × dewpoint factor + sector offset |
| 9 | clearness-trend | dk/dt projected k (slope over 3h window) |
| 10 | clearness-trend+dewpoint | projected k × dewpoint depression factor |
| 11 | clearness-trend+pressure-proj | projected k further adjusted by dP/dt |
| 12 | pressure-departure | station pressure departure from 30d mean → T offset |
| 13 | pressure-dep+clearness-trend | pressure departure + projected-k clearness |
| 14 | wind-veer | net veering/backing rate (°/hour) from 3h direction history → advection offset |
| 15 | clearness-stability | k dampened by solar radiation CV — broken cumulus reduces amplitude |
| 16 | veer+clearness | members 14 + 15 combined |

## Limitations

- Clearness signal is unavailable when the sun is below the horizon at issued time; members 1–3, 5, 15, and 16 fall back to the anchored diurnal climatology (no amplitude boost).
- Sector offsets (members 4, 5, 8) are static empirical constants, not derived from local data. They will likely have systematic bias until enough data accumulates for tuning.
- The veering/backing signal (member 14, 16) uses the net direction change over 3 hours. A single 180° wind shift will appear the same as a gradual 3°/hour drift; rapid synoptic changes may alias the signal.
- Solar CV (members 15, 16) requires at least 4 daytime observations (> 10 W/m²) in the 3h window; pre-dawn or deeply overcast runs fall back to the raw clearness index.
