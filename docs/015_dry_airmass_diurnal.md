# dry_airmass_diurnal (model 15)

Uses the persistence of a dewpoint-depression anomaly to scale the diurnal temperature
amplitude. In a sustained dry airmass (large positive DD anomaly over the past 24–72
hours), afternoon highs run above the climatological diurnal curve and overnight lows
run below it — dry air heats fast and radiates efficiently. This model quantifies that
effect from local Tempest observations alone, with no NWP input required.

## Motivation

Standard base models anchor to the current or climatological temperature without
accounting for how atmospheric dryness amplifies the diurnal cycle. The `climo_deviation`
model handles instantaneous dewpoint anomalies well but does not scale temperature
amplitude. `airmass_diurnal` uses the instantaneous dewpoint depression as one signal
among many. This model isolates the persistent moisture anomaly as its primary driver
and applies it directly to the diurnal deviation from the daily mean.

## Members

| member_id | name | window | pressure boost |
|-----------|------|--------|----------------|
| 0 | — | weighted mean of 1–6 | — |
| 1 | 24h-amp | 24h | no |
| 2 | 48h-amp | 48h | no |
| 3 | 72h-amp | 72h | no |
| 4 | 24h-amp-ridge | 24h | yes |
| 5 | 48h-amp-ridge | 48h | yes |
| 6 | 72h-amp-ridge | 72h | yes |

## Algorithm

**Setup:** fetch 30d obs for hourly climatology, 72h obs for anomaly windows.

**Temperature forecast** for each member:

```
T_base    = climo hour mean interpolated to valid time
dev       = T_base − daily_T_mean          # diurnal deviation; + afternoon, − night
anchor    = T_obs − climo at issue hour    # locks to current obs
dd_anom   = mean(T−Td over window) − climo_DD_now
amp_adj   = dd_anom × AMP_SENSITIVITY × dev

forecast  = T_base + anchor + amp_adj
          [+ p_dep × P_SENSITIVITY × max(0, dev)  for ridge members]
```

The `dev` term ensures the amplitude adjustment is directionally correct: positive in
the afternoon (pushes temps higher in dry air), negative overnight (pushes temps lower).

**Dewpoint forecast:** persistent window-averaged Td anomaly decays exponentially toward
climatology at the valid time.

```
td_anom  = mean(Td over window) − climo_Td_now
forecast = climo_Td_valid + td_anom × exp(−TD_DECAY_K × lead_hours)
```

## Constants

| Constant | Value | Meaning |
|----------|-------|---------|
| AMP_SENSITIVITY | 0.07 | diurnal-dev fraction per 1°C of DD anomaly |
| P_SENSITIVITY | 0.015 | °C per hPa pressure departure (afternoon only) |
| TD_DECAY_K | 0.04 | dewpoint anomaly e-folding per lead hour |

## Behavior

- **Normal conditions** (dd_anom ≈ 0): amp_adj ≈ 0, forecast ≈ anchored climo curve
- **Dry airmass** (dd_anom > 0): afternoon highs pushed up, overnight lows pushed down
- **Humid anomaly** (dd_anom < 0): afternoon highs damped, overnight lows raised
- **Pressure ridge members** add a daytime-only boost when under a surface high
