# synoptic_state_machine (model 10)

A forecast model that classifies current atmospheric conditions using the same four
observable signals as `surface_signs` — wind rotation, dewpoint spread trend, solar cloud
cover, and convective state — but treats them as a single combined state rather than four
independent ones.

## Motivation

`surface_signs` isolates each physical signal and learns how that signal alone predicts
future weather. Each member is blind to the others. The problem is that signals interact:
veering winds while moisture is increasing points toward a different atmospheric evolution
than veering winds while the air is drying out, even though both share the same wind
rotation category. The synoptic state machine learns conditional mean deltas for the full
joint state, so those interactions become visible.

The trade-off is data density. A four-signal state with three categories per signal has
81 possible combinations, and not all of them appear frequently enough to build reliable
statistics. The model handles this through member design: simpler members use fewer
signals and have fewer, denser cells, while the full-state members are more expressive
but take longer to accumulate reliable history.

## Algorithm

The model scans all historical observations in a single pass, computing all five member
state tuples simultaneously per timestamp. For each historical moment, it records the
observed change in each variable at each lead time and accumulates those deltas by
(member, state tuple, variable, lead). Cells with fewer than 3 historical pairs are
excluded; the model abstains for those combinations rather than guessing.

At forecast time, the four live signal categories are computed from the current
observation window, assembled into each member's state tuple, and matched against the
accumulated conditional mean delta table:

```
forecast_value = obs_val + mean_delta(member, live_state, variable, lead_hours)
```

If the live state has never appeared with enough history for a given (variable, lead)
pair, that member returns `None` for that slot. The ensemble mean uses whichever members
produced non-None values.

## Members

| ID | Name | Signals | State space | Notes |
|----|------|---------|-------------|-------|
| 0 | ensemble mean | all members | — | sector-weighted mean + spread |
| 1 | full-4 | wind, dp, cloud, convective | 3×3×3×3 = 81 | abstains at night (cloud = None) |
| 2 | no-cloud | wind, dp, convective | 3×3×3 = 27 | works at night; drops cloud signal |
| 3 | wind-moisture | wind, dp | 3×3 = 9 | the two most synoptically stable signals |
| 4 | moisture-convective | dp, convective | 3×3 = 9 | moisture trend and active precip |
| 5 | coarse-4 | coarsened wind, dp, cloud, convective | 2×2×2×3 = 24 | abstains at night; more data per cell |

Member 5 coarsens the first three signals to binary categories to increase sample counts:

- wind rotation: **rotating** (veering or backing) or **steady**
- dewpoint trend: **moistening** (narrowing) or **drying** (steady or widening)
- cloud cover: **cloudy** (partial or heavy) or **clear**
- convective: unchanged (dry / precip / lightning)

## Signal definitions

All four signals use the same classification logic as `surface_signs` (model 9), imported
directly to ensure consistency.

### wind rotation

Net wind direction change over the 3h observation window, filtered to readings where
`wind_avg > 1.5 m/s`. A change greater than +15° (clockwise) is **veering**, less than
−15° (counterclockwise) is **backing**, otherwise **steady**. Returns None when fewer
than 2 qualifying observations are available.

### dewpoint spread trend

Change in the temperature-dewpoint spread (`air_temp − dew_point`) between now and 3h
ago. A decrease of more than 1°C is **narrowing** (air moistening), an increase of more
than 1°C is **widening** (air drying), otherwise **steady**. Returns None when the 3h
prior observation is unavailable.

### solar cloud cover

Solar radiation compared against the climatological mean for the same calendar month and
hour, built from all historical daytime observations. A deficit above 70% is
**heavy_cloud**, above 30% is **partial_cloud**, otherwise **clear**. Returns None at
night (solar ≤ 5 W/m²) and when fewer than 10 historical samples exist for the
(month, hour) bucket.

### convective state

Lightning takes priority. If the 3h observation window contains any lightning strikes,
the state is **lightning**. If the 1h precipitation accumulation rate exceeds 0.5 mm/h,
the state is **precip**. Otherwise **dry**. Always returns a non-None category.

## Limitations

- **full-4 and coarse-4** are blind at night because the cloud signal is unavailable.
  Members 2, 3, and 4 provide coverage during overnight hours.
- **full-4** has 81 possible states. In the first year of data, many cells will have
  fewer than 3 samples and the member will abstain frequently. Performance improves as
  history accumulates; member 5 provides a denser alternative in the interim.
- The model cannot distinguish between states that have identical signal categories but
  different magnitudes. Two slow-rise pressure events both map to the same state
  regardless of their rates.
- All members degrade gracefully to `None` rather than guessing. The ensemble mean
  reflects only the members with sufficient historical backing for each (variable, lead)
  combination.

## Data requirements

- Tempest observations at ≥5-minute cadence (standard)
- At least 3 historical (state, variable, lead) pairs for any member to produce a
  non-None forecast; denser members (3, 4, 5) accumulate this threshold faster
- Solar climo requires ≥10 daytime obs per (month, hour) bucket before the cloud signal
  activates; this accumulates over the first few weeks of deployment
