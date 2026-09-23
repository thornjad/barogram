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
| 17 | clearsky-envelope-trend | trend of solar_radiation vs the station's own 30d hourly max envelope, not the astronomical clear-sky formula |
| 18 | uv-solar-divergence | uv_index vs solar_radiation ratio, divergence from own history — speculative haze/smoke-aloft proxy |
| 19 | early-ramp-steepness | solar_radiation ramp rate in the first 2h after sunrise vs the theoretical clear-sky ramp |
| 20 | snow-cover-proxy | sub-freezing run-length + any precip in that run infers snow-covered ground, deliberately conservative |

## Guardrail: continental diurnal-swing ceiling

Not a member. Every member's temperature value (including member_id=0's blend) is
clamped to within half of a locally-learned seasonal ceiling around the day's
climatological mean:

```
ceiling = 95th percentile of daily (max − min air_temp) for this calendar month,
          across up to 400 days of history
value = clamp(value, T_daily_mean − ceiling/2, T_daily_mean + ceiling/2)
```

Inland, non-lake-moderated stations see larger clear/calm diurnal swings than a
coastal or lake-adjacent one — the ceiling is learned from this station's own
history rather than a fixed constant, and stays inactive (no clamping) until at
least 5 days of same-month history exist. See `_seasonal_swing_ceiling` in
`models/airmass_diurnal.py`.

## Limitations

- Clearness signal is unavailable when the sun is below the horizon at issued time; members 1–3, 5, 15, and 16 fall back to the anchored diurnal climatology (no amplitude boost).
- Sector offsets (members 4, 5, 8) are static empirical constants, not derived from local data. They will likely have systematic bias until enough data accumulates for tuning.
- The veering/backing signal (member 14, 16) uses the net direction change over 3 hours. A single 180° wind shift will appear the same as a gradual 3°/hour drift; rapid synoptic changes may alias the signal.
- Solar CV (members 15, 16) requires at least 4 daytime observations (> 10 W/m²) in the 3h window; pre-dawn or deeply overcast runs fall back to the raw clearness index.
- The envelope trend (member 17) needs 12+ populated hourly buckets in the trailing 30 days and 2+ qualifying daytime points in the 3h window; otherwise it falls back to no amplitude adjustment, same as the astronomical-clearness members.
- UV-solar divergence (member 18) is explicitly speculative — cheap to test, no strong physical calibration yet, sensitivity may need retuning once it has scored history.
- Early-ramp steepness (member 19) only fires 4-10h out on the same calendar day, and only once 2h have actually elapsed since today's sunrise; it's silent (no adjustment) the rest of the time, including every overnight/next-day lead.
- Snow-cover proxy (member 20) is inference, not measurement — no snow-depth sensor exists. It requires 3+ consecutive sub-freezing days with precip recorded in that stretch, and its suppression is deliberately modest and capped so a wrong guess costs little.

## Confidence

Every member here gets a confidence value computed against the shared default
fingerprint (`air_temp`, `dew_point`, `station_pressure`, `wind_avg`), matched
against its own scored history by calendar day. member_id=0's combination now
multiplies each member's weight by its own confidence (floored, defaulted to the
group's own average when unknown) via `models/_confidence.py`'s `combine_pattern`,
which also fixed a pre-existing bug: a member missing a weight used to collapse the
whole group to a plain average, now only that member is dropped. See
[confidence.md](confidence.md) for the full design.

## Self-correction

Member 21 (`self_correction`) is the standard self-correction member (migration
`065_batch_b_self_correction_members.sql`) — member_id=0 minus this model's own
learned historical bias at each (variable, lead_hours) cell. See
[self_correction.md](self_correction.md).
