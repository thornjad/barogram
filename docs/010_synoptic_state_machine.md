# synoptic_state_machine (model 10)

A forecast model that classifies current atmospheric conditions using up to five
observable signals — wind rotation, dewpoint spread trend, solar cloud cover, convective
state, and pressure tendency — and treats them as a single combined state rather than
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

The model scans all historical observations in a single pass, computing all seven member
state tuples simultaneously per timestamp. For each historical moment, it records the
observed change in each variable at each lead time and accumulates those deltas by
(member, state tuple, variable, lead). Cells with fewer than 3 historical pairs are
excluded; the model abstains for those combinations rather than guessing.

At forecast time, the five live signal categories are computed from the current
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
| 1 | full-4 | wind, dp, cloud, convective | 3×3×3×4 = 108 | abstains at night (cloud = None) |
| 2 | no-cloud | wind, dp, convective | 3×3×4 = 36 | works at night; drops cloud signal |
| 3 | wind-moisture | wind, dp | 3×3 = 9 | the two most synoptically stable signals |
| 4 | moisture-convective | dp, convective | 3×4 = 12 | moisture trend and active precip |
| 5 | coarse-4 | coarsened wind, dp, cloud, convective | 2×2×2×4 = 32 | abstains at night; more data per cell |
| 6 | full-4+ptend | wind, dp, cloud, convective, pressure tendency | 3×3×3×4×3 = 324 | abstains at night; adds pressure trend |
| 7 | no-cloud+ptend | wind, dp, convective, pressure tendency | 3×3×4×3 = 108 | works at night; adds pressure trend |
| 8 | moisture-only | dp | 3 | dp trend alone |
| 9 | convective-only | convective | 4 | convective alone; never abstains for a missing signal |
| 10 | moisture-ptend | dp, pressure tendency | 3×3 = 9 | dp paired with ptend instead of convective |
| 11 | moisture-conv-ptend | dp, convective, pressure tendency | 3×4×3 = 36 | the three signals that don't require cloud or wind |
| 12 | moisture-cloud | dp, cloud | 3×3 = 9 | abstains at night; dp paired with cloud instead of convective |
| 13 | no-wind | dp, cloud, convective, pressure tendency | 3×3×4×3 = 108 | full-4+ptend minus wind rotation |
| 14 | wind-only | wind | 3 | wind rotation alone; weak contrast baseline |
| 15 | convective-cloud | convective, cloud | 4×3 = 12 | abstains at night; sky-condition pair, no moisture signal |

Convective gained a 4th category ("hail") on 2026-09-18 when `surface_signs`'s shared
`_convective_category` was edited in place to read two new Tempest fields — every
member above that includes the convective signal inherits the larger state space
automatically, since nothing here enumerates a fixed category count. "hail" starts
with zero historical samples and abstains the same as any new category would.

Members 8–15 riff on member 4 (moisture-convective), the smallest, most consistently
available member of the original seven: they explore why it holds up (few cells, no
wind-rotation dependency, always-defined convective signal) by isolating and recombining
signals, minus the wind-rotation dependency where possible. 9 and 14 are deliberately
weak — single-signal contrast baselines to see how far a single category goes.

Member 5 coarsens the first three signals to binary categories to increase sample counts:

- wind rotation: **rotating** (veering or backing) or **steady**
- dewpoint trend: **moistening** (narrowing) or **drying** (steady or widening)
- cloud cover: **cloudy** (partial or heavy) or **clear**
- convective: unchanged (dry / precip / lightning)

Members 6 and 7 extend their base members (1 and 2) with a pressure tendency signal,
allowing the model to distinguish, for example, rising-pressure veering winds from
falling-pressure veering winds. Both require pressure data from 3h ago and abstain when
that observation is unavailable.

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

Same shared `_convective_category` as `surface_signs` member 4 — see
[009_surface_signs.md](009_surface_signs.md) for the full hail/lightning/precip/dry
priority order and the 2026-09-18 fields (`precip_type`,
`lightning_strike_count_last_3hr`) that feed it, both tolerant of being absent on any
given observation. Always returns a non-None category.

### pressure tendency

Change in `station_pressure` between now and 3h ago. A rise greater than 0.5 hPa is
**rising**, a fall greater than 0.5 hPa is **falling**, otherwise **steady**. Returns
None when the 3h prior observation is unavailable. Used by members 6 and 7 only.

## Expansion members (16-94)

Added 2026-08-20 after per-member weight analysis showed the productive signal core is
moisture trend, convective state, and pressure tendency, with wind rotation and cloud
cover contributing little as joint dimensions (see the members 8-15 finding above). Four
parametric families, 79 members total. Exact `(member_id, name)` assignment lives in
`migrations/039_synoptic_state_machine_expansion.sql`; the generation order in
`models/synoptic_state_machine.py`'s `_build_expansion_states` must match it exactly.

- **ptend sweep (16-63, 48 members)**: pressure tendency at windows
  {1,2,4,5,6,12,18,24}h (3h is the original signal, untouched) × granularity
  (3-category, matching the original rising/steady/falling; or 5-bucket "graded",
  adding strong_rising/strong_falling splits) × pairing (+dp_trend, +dp_trend+convective,
  or isolated alone). Windows 7-10h were considered and dropped as redundant with the
  1-6h cluster; 12/18/24h were kept since they capture full frontal-passage-scale
  pressure change rather than hourly noise.
- **gust (64-75, 12 members)**: `wind_gust / wind_avg` ratio at windows {1,3,6}h,
  categorized smooth/breezy/gusty. Turbulence proxy, orthogonal to wind_rotation
  (direction) and unused anywhere else in the model. Paired with +dp_trend, +convective,
  +pressure_tendency (3h), or +dp_trend+convective.
- **temptrend (76-87, 12 members)**: raw `air_temp` trend at windows {1,3,6,12}h,
  distinct from the dp_trend spread signal — captures warm/cold air advection
  independent of moisture. Paired with +pressure_tendency(3h), +dp_trend+pressure_tendency(3h),
  or +convective.
- **preciprate (88-93, 6 members)**: precip accumulation rate trend
  (accelerating/steady/decelerating) at windows {30min,1h,3h}, comparing the rate over
  the last window to the window before it. Paired with +convective or +dp_trend.
- **moisture-convective-cloud (94, 1 member)**: dp_trend + convective + cloud, three-way.
  Denser than member 12's dp_trend+cloud pairing, which scored near-zero weight; tests
  whether cloud has any marginal value once paired with convective instead of alone.

None of these change any existing member's signal definition or state tuple. Existing
members 1-15 are only ever modified for a confirmed bug, never for weight/performance
reasons.

## New members (95-96)

Added 2026-09-22, standalone hand-designed additions rather than another parametric
sweep. Registered in `migrations/052_synoptic_state_machine_gust_and_rh_members.sql`.

- **gust-ratio-trend (95)**: trend of the `wind_gust / wind_avg` ratio itself, comparing
  two adjacent 3h trailing windows (`_GUST_TREND_WINDOW_HOURS`). A rise greater than 0.3
  in the ratio is **rising**, a fall greater than 0.3 is **falling**, otherwise
  **steady**. Distinct from the existing gust members (64-75), which read the ratio's
  level at a single window rather than whether it's itself climbing or falling — a
  rising ratio is a leading indicator of approaching mechanical mixing (a wind shift),
  a falling one reads as a calming trend. Single-signal member, 3 cells.
- **rh-wind-pressure (96)**: joint state of relative humidity, wind rotation, and
  pressure tendency (3h). Relative humidity is bucketed by proximity to saturation —
  `relative_humidity` ≥ 90% is **saturated**, ≥ 60% is **moist**, otherwise **dry** —
  and reuses the model's existing wind-rotation and pressure-tendency signals. No
  other member here reads `relative_humidity` directly, only dewpoint spread, so this
  is a genuinely orthogonal axis rather than a restatement of the moisture members.
  3×3×3 = 27 cells.

## Limitations

- **full-4, coarse-4, and full-4+ptend** are blind at night because the cloud signal is
  unavailable. Members 2, 3, 4, and no-cloud+ptend provide coverage during overnight
  hours.
- **full-4** has 108 possible states and **full-4+ptend** has 324. Both will abstain
  frequently in the first year of data. Performance improves as history accumulates;
  members 3, 4, and 5 provide denser alternatives in the interim.
- The model cannot distinguish between states that have identical signal categories but
  different magnitudes. Two slow-rise pressure events both map to **rising** regardless
  of their rates.
- All members degrade gracefully to `None` rather than guessing. The ensemble mean
  reflects only the members with sufficient historical backing for each (variable, lead)
  combination.

## Data requirements

- Tempest observations at ≥5-minute cadence (standard)
- At least 3 historical (state, variable, lead) pairs for any member to produce a
  non-None forecast; denser members (3, 4, 5) accumulate this threshold faster
- Solar climo requires ≥10 daytime obs per (month, hour) bucket before the cloud signal
  activates; this accumulates over the first few weeks of deployment
- Members 6 and 7 require a 3h-prior observation with valid `station_pressure`; they
  abstain whenever that observation is missing

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

Member 97 (`self_correction`) is the standard self-correction member (migration
`065_batch_b_self_correction_members.sql`) — member_id=0 minus this model's own
learned historical bias at each (variable, lead_hours) cell. See
[self_correction.md](self_correction.md).
