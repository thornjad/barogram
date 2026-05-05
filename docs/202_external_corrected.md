# external_corrected (model 202)

A bias-corrected version of the NWS and Tempest external forecasts. Both external models
carry systematic errors — NWS dewpoint runs ~1.1–1.3°C too dry at all leads, while
Tempest dewpoint runs ~1.1–1.4°C too dry. This model learns those error patterns from
scored history and applies corrections conditioned on time of day, season, and current
airmass state.

This model is type `external` and is **not part of the barogram ensemble**. It exists
for comparison and interest: to see how much can be gained by correcting the systematic
biases that the external models carry.

## Motivation

The barogram ensemble derives forecasts from Tempest station observations only. External
models (NWS, Tempest) serve as comparison baselines, not ensemble members. But a bias
correction model sits in a different category — it does not produce an original forecast,
it refines an existing one. Tracking its performance over time answers a practical
question: if you knew the typical error patterns of a professional forecast, how much
better could you do by applying that correction?

## Algorithm

At each forecast run, the model:

1. Queries all scored NWS and Tempest forecast rows from the database.
2. For each scored row, looks up the Tempest observation nearest to `issued_at` (within
   ±10 min) to determine the airmass state (T-Td spread) at forecast issuance.
3. Builds correction tables: mean historical error grouped by conditioning factors.
4. Fetches today's NWS and Tempest forecasts via the same API calls as models 200/201.
5. For each member, applies the correction: `corrected = raw − mean_historical_error`.

The correction sign: because `error = forecast − observed`, subtracting the mean error
from the forecast moves it toward the historical observed mean, reducing systematic bias.

Cells with fewer than 3 historical samples fall back through the chain to flatter
corrections. Members always produce a value as long as the source API returned one —
they never abstain due to sparse conditioning data.

## Members

| ID | Name | Source | Conditioning | Fallback chain |
|----|------|--------|--------------|----------------|
| 0  | ensemble mean | both | — | mean of all non-None members |
| 1  | nws-flat | NWS | none (overall mean per variable/lead) | zero correction |
| 2  | nws-diurnal | NWS | hour of valid_at (0-5 / 6-11 / 12-17 / 18-23) | → nws-flat |
| 3  | nws-seasonal | NWS | season of valid_at (DJF / MAM / JJA / SON) | → nws-flat |
| 4  | nws-airmass | NWS | T-Td spread at issued_at (moist <3°C / moderate / dry >8°C) | → nws-flat |
| 5  | nws-joint | NWS | hour bucket × season | → nws-diurnal → nws-flat |
| 6  | tempest-flat | Tempest | none | zero correction |
| 7  | tempest-diurnal | Tempest | hour of valid_at | → tempest-flat |
| 8  | tempest-seasonal | Tempest | season of valid_at | → tempest-flat |
| 9  | tempest-airmass | Tempest | T-Td spread at issued_at | → tempest-flat |
| 10 | tempest-joint | Tempest | hour bucket × season | → tempest-diurnal → tempest-flat |

**Member 0** is the arithmetic mean of all members that produced non-None values. When
both NWS and Tempest APIs are available, this blends five corrected NWS values and five
corrected Tempest values. If one source is unavailable, the mean reflects the other alone.

## Conditioning factors

**Time of day (diurnal)**: Valid-at hour split into four 6-hour buckets. NWS temperature
errors differ between daytime and overnight: the model may be more accurate at certain
times due to boundary layer evolution.

**Season (seasonal)**: Valid-at month mapped to DJF/MAM/JJA/SON. Errors that are
systematic in summer (hot humid airmasses) may differ from winter (cold dry air).

**Airmass state (airmass)**: T-Td spread at issued_at, classified as moist (<3°C),
moderate (3–8°C), or dry (>8°C). NWS dewpoint bias is suspected to vary with moisture
regime — this member directly tests that hypothesis.

**Joint (joint)**: Diurnal × season (up to 16 cells). The most specific conditioning,
slowest to accumulate reliable statistics. Falls back through diurnal → flat.

## Limitations

- **Data starvation early on**: With ~100 scored NWS rows at current, flat corrections
  are reliable. Diurnal and seasonal members have ~25 samples per bucket — borderline.
  Airmass members similarly. Joint members have ~6 per cell and will fall back frequently
  until more history accumulates.
- **Precip probability**: Bias correction of a probability can in principle produce
  values outside [0, 1]. With typical mean errors of 0.02–0.08, this is unlikely in
  practice.
- **Not a barogram forecast**: Results depend entirely on external API availability and
  accuracy. When NWS or Tempest APIs are unreachable, the corresponding five members
  produce no rows and member 0 reflects only the available source.

## Data requirements

- Scored NWS (model 200) and Tempest forecast (model 201) rows in barogram.db
- Tempest observations in wxlog for airmass conditioning (nearest obs within ±10 min of
  each historical `issued_at`)
- Active NWS API access for current forecast fetch
- Tempest API credentials (`[tempest]` section in barogram.toml) for current forecast
