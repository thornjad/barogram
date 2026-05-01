# BOGO Model

**Model ID:** 12
**Type:** base
**Variables:** temperature, dewpoint, pressure, precip_prob

## Concept

Named after [bogosort](https://en.wikipedia.org/wiki/Bogosort), the famously terrible sorting algorithm that shuffles a list randomly and checks if it's sorted. Like bogosort, this model is correct only by dumb luck.

BOGO is a 27-member ensemble where each member applies a different flavor of wrongness. The member_id=0 row is the ensemble mean and spread across all members. Because the members' errors are largely uncorrelated and directionally random, the mean tends to converge back toward climatology — but with maximum absurdity along the way.

## Members

| ID | Name | Concept |
|----|------|---------|
| 1 | drunkard | random walk chained from climo anchor; each step drifts from prior |
| 2 | blind-drunkard | same walk but re-anchors to climo independently at each lead |
| 3 | chaos | random walk at 3× step size |
| 4 | vibes | independent uniform draw from observed seasonal min/max; pure dart |
| 5 | contrarian | mirrors current obs deviation from climo in the opposite direction |
| 6 | hype-train | extrapolates current 6h obs trend forward with jitter |
| 7 | mercury-retrograde | mild walk normally; 10× step size during actual Mercury retrograde windows |
| 8 | weatherperson | always climo for all variables; always exactly 30% precip probability |
| 9 | crowd-sourced | pulls a random historical observation and uses it as the forecast |
| 10 | groundhog-day | reports actual observations from 24h prior as the forecast for all leads |
| 11 | CG | if any lightning detected, locks precip_prob to 1.0 for all leads |
| 12 | climate-anxiety | always +3°C above climo temperature; slightly elevated precip |
| 13 | too-early | reports actual observations from 6h prior as the forecast for all leads |
| 14 | monday | systematically worse weather on Mondays, better on Fridays |
| 15 | grant-funded | mild climo walk, but ~20% of variables return None each lead |
| 16 | the-algorithm | amplifies current obs deviation from climo across all variables |
| 17 | peer-review | mean of all other members plus small noise |
| 18 | dew-denier | always reports temperature value as dewpoint (100% RH forever) |
| 19 | breaking-news | always predicts the historical seasonal extreme for each variable |
| 20 | engagement-bait | precip always 51%; temperature rounded to nearest integer |
| 21 | both-sides | alternates hot/cold seasonal extremes at each successive lead time |
| 22 | sponsored-content | always predicts perfect mild outdoor weather: 22°C, 0% precip |
| 23 | influencer | randomly chooses "golden hour aesthetic" or "dramatic storm content" |
| 24 | panic | catastrophically overreacts to any pressure change |
| 25 | nostalgia | reports actual observations from this exact date one year ago |
| 26 | astroturfed | climo with a hidden warming drift of 0.1°C/month since 2024-01-01 |
| 27 | record-breaker | predicts world records (all variables) whenever obs deviates from climo |

## Step bounds (drunkard, blind-drunkard, chaos, mercury-retrograde, grant-funded)

| Variable | Base step bound |
|----------|----------------|
| temperature | ±5.0 °C |
| dewpoint | ±3.0 °C |
| pressure | ±3.0 hPa |
| precip_prob | ±0.20, clamped [0, 1] |

chaos uses 3× base; mercury-retrograde uses 10× during retrograde, 0.5× otherwise.

## Physical constraints

All members clamp output to world-record extremes and enforce dewpoint ≤ temperature. The ensemble mean also enforces the dewpoint constraint.

## Expected performance

Poor. The members have zero collective skill. The ensemble mean will sink toward climatology as their random errors cancel, but it will never beat a real model. Its purpose is entertainment, calibration of a lower skill bound, and occasionally making the dashboard charts look like abstract art.
