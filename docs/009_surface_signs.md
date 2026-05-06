# surface_signs (model 9)

A forecast model that reads observable atmospheric signals from the Tempest station —
wind rotation, moisture tendency, cloud cover, and convective activity — and applies
historically-learned conditional mean deltas for each signal state.

Inspired by Theophrastus's *Book of Signs* (c. 300 BCE), which catalogued observable
physical cues — wind shifts, humidity, sky appearance — as precursors to weather change.
This is the first barogram model to use `wind_direction`, `lightning_count`, `wind_gust`,
and `precip_accum_day`.

## Motivation

`pressure_tendency` extrapolates a single time series forward. `analog` finds similar
historical days in feature space. `surface_signs` does something different: it classifies
the *current observational state pattern* and maps it to historical outcomes. Each member
isolates one physical signal, so the ensemble can discover which signals carry the most
information at each lead time and for each variable.

## Algorithm

For each signal member, the training phase scans all historical obs and at each timestamp:

1. Computes the signal category (e.g., "backing", "veering", "steady")
2. Finds the observed values `lead_hours` later (±15 min window)
3. Records the per-variable delta

At forecast time, the live category is matched to the historical conditional mean delta:

```
forecast_value = obs_val + mean_delta(live_category, variable, lead_hours)
```

Cells with fewer than 3 historical pairs are absent — the model abstains (`value = None`)
for those combinations rather than guessing.

## Members

| ID | Name | Signal | Categories |
|----|------|--------|------------|
| 0 | ensemble mean | skill-score weighted mean of members 1–4 | — |
| 1 | wind-rotation | net wind direction change over 3h | veering, backing, steady |
| 2 | dp-trend | dewpoint spread (temp − dp) change over 3h | narrowing, steady, widening |
| 3 | solar-cloud | solar radiation deficit vs climatological mean | clear, partial_cloud, heavy_cloud |
| 4 | convective | lightning count (3h) + precip rate (1h) | lightning, precip, dry |

## Signal details

### wind-rotation (member 1)

Filters the 3h observation window to readings where `wind_avg > 1.5 m/s` (below this,
direction is unreliable). Computes the net angular change from oldest to newest qualifying
observation using circular arithmetic (`diff = (d2 − d1 + 360) % 360`, then normalized to
(−180, 180]).

- **veering** (> +15°, clockwise): in the Northern Hemisphere, veering indicates the warm
  sector of a cyclone or post-frontal clearing
- **backing** (< −15°, counterclockwise): indicates approaching cold or warm front
- **steady**: < 15° net change

Returns `None` (member abstains) when fewer than 2 qualifying obs are available.

### dp-trend (member 2)

Computes the dewpoint spread (`air_temp − dew_point`) at the current observation and
3 hours prior.

- **narrowing** (delta < −1°C): moisture increasing, boundary layer saturating, front approaching
- **widening** (delta > +1°C): drying, post-frontal clearing
- **steady**: change within ±1°C

Returns `None` when the 3h prior observation is unavailable or temperature/dewpoint values
are missing.

### solar-cloud (member 3)

Compares the current `solar_radiation` reading against the climatological mean for the
same calendar month and hour of day, computed from all historical obs with
`solar_radiation > 5 W/m²`.

`deficit = 1 − actual / climo_mean`

- **heavy_cloud** (deficit > 0.7): substantial cloud cover, likely overcast
- **partial_cloud** (0.3 < deficit ≤ 0.7): scattered clouds
- **clear** (deficit ≤ 0.3): near-clear sky

Returns `None` when: solar_radiation is None or ≤ 5 W/m² (nighttime or sensor gap),
fewer than 10 historical samples exist for the (month, hour) bucket, or climo mean is
unavailable (common early in deployment).

### convective (member 4)

Lightning takes priority over precipitation.

1. Sums `lightning_count` (treating `NULL` as 0) over the 3h window. Any strikes → **lightning**.
2. Computes the 1h precipitation rate: `max(0, precip_accum_day_now − precip_accum_day_1h_ago)`.
   The `max(0, …)` clamp handles midnight resets. Rate > 0.5 mm/h → **precip**.
3. Otherwise → **dry**.

Always returns a non-None category, so this member always produces a forecast (even when
"dry" has few historical samples early in deployment).

## Limitations

- **wind-rotation** is data-starved during calm periods and may abstain frequently in
  Minnesota winters when sustained winds are common but variable.
- **solar-cloud** is silent from sunset to sunrise and requires ~10 obs per (month, hour)
  bucket before it can classify at all. Expect many `None` values during the first few
  weeks of deployment.
- **dp-trend** and **convective** are robust from day one but may show high variance in
  early conditional means when sample counts are low.
- All members degrade gracefully to `None` rather than producing unreliable forecasts.

## Data requirements

- Tempest observations at ≥5-minute cadence (standard)
- At least 3 historical (category, variable, lead) pairs for any member to produce a
  non-None forecast
- Solar climo requires ≥10 daytime obs per (month, hour) bucket; this accumulates over
  the first few weeks
