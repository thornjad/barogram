# airmass_precip (model 11)

Estimates precipitation probability by classifying the current airmass state and looking up the historical fraction of similar states that were followed by measurable precipitation.

## Motivation

Every other precipitation model in barogram either pulls from an external forecast API or uses the full observation sequence to find analogs. `airmass_precip` takes a different angle: it asks "historically, when conditions looked like this, how often did it rain in the next N hours?" This is a non-parametric conditional climatology — no regression, no parameters to fit, just counting.

The model intentionally starts with near-zero skill (no history) and improves monotonically as the obs record grows. With a full year or more of data, the cells become well-populated and the model learns local precipitation tendencies that are specific to this station.

## Algorithm

1. For each historical observation, classify it into a discrete state using one of eight signal classifiers (see members).
2. For each `(state, lead_hours)` pair, check whether measurable precipitation (>0.1 mm delta in `precip_accum_day`) occurred by the target time.
3. The fraction of observations in that state that were followed by precipitation is the forecast probability.
4. Cells with fewer than 3 historical samples abstain (return None).

Midnight boundary is handled consistently with scoring and analog: if the target time crosses midnight, `precip_accum_day` is compared against 0 (accumulator resets at midnight).

## Members

| ID | Name | Signal | State space |
|----|------|--------|-------------|
| 0 | ensemble mean | — | weighted mean of members 1–8 |
| 1 | dewpoint-moisture | T−Td spread | moist (<3°C) / moderate (3–8°C) / dry (>8°C) |
| 2 | pressure-tendency | 3h ΔP | falling (<−1 hPa) / steady / rising (>+1 hPa) |
| 3 | cloud-cover | solar radiation vs climo mean | clear / partial_cloud / heavy_cloud; abstains at night |
| 4 | wind-sector | wind direction (4-sector) | N (315–44°) / E (45–134°) / S (135–224°) / W (225–314°) |
| 5 | active-precip | current precip rate (last hour) | raining (>0.5 mm/h) / dry |
| 6 | moisture+pressure | joint T−Td × ΔP | 9-state; combines the two strongest signals |
| 7 | wind-rotation | 3h veering/backing rate | veering / backing / steady |
| 8 | cloud+moisture | joint cloud cover × T−Td | 9-state, abstains at night |

Member 5 (`active-precip`) embodies the oldest forecasting heuristic: if it is currently raining, it is likely still raining in the near term. Member 7 (`wind-rotation`) comes from maritime tradition: a backing wind (counterclockwise) signals frontal approach; a veering wind (clockwise) signals clearing.

## Limitations

- **Data starvation**: Each (state, lead) cell needs at least 3 paired observations before it can produce a forecast. With sparse history, most members will abstain entirely.
- **State sparsity for joint members**: Members 6 and 8 have 9 states each. These cells fill slower and will abstain more often than single-signal members.
- **No synoptic context**: The model classifies the current point-in-time state but has no awareness of large-scale patterns (troughs, ridges, Gulf moisture plumes). The analog model captures some of this indirectly through pressure and moisture correlation.
- **Wind sector is coarse**: Four sectors cannot distinguish a southwesterly flow from a southeasterly one, both of which can be associated with very different moisture regimes depending on the local geography.
