# BOGO Model

**Model ID:** 12
**Type:** base
**Variables:** temperature, dewpoint, pressure

## Concept

Named after [bogosort](https://en.wikipedia.org/wiki/Bogosort), the famously terrible sorting algorithm that shuffles a list randomly and checks if it's sorted. Like bogosort, this model is correct only by dumb luck.

BOGO is a 69-member ensemble where each member applies a different flavor of wrongness, plus a 70th standard self-correction member. The member_id=0 row is the ensemble mean and spread across the 69 wrongness members only. Because the members' errors are largely uncorrelated and directionally random, the mean tends to converge back toward climatology — but with maximum absurdity along the way.

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
| 8 | weatherperson | always climo for all variables |
| 9 | crowd-sourced | pulls a random historical observation and uses it as the forecast |
| 10 | groundhog-day | reports actual observations from 24h prior as the forecast for all leads |
| 11 | CG | locks all variables to extreme values when lightning is detected |
| 12 | climate-anxiety | always +3°C above climo temperature |
| 13 | too-early | reports actual observations from 6h prior as the forecast for all leads |
| 14 | monday | systematically worse weather on Mondays, better on Fridays |
| 15 | grant-funded | mild climo walk, but ~20% of variables return None each lead |
| 16 | the-algorithm | amplifies current obs deviation from climo across all variables |
| 17 | peer-review | mean of all other members plus small noise |
| 18 | dew-denier | always reports temperature value as dewpoint (100% RH forever) |
| 19 | breaking-news | always predicts the historical seasonal extreme for each variable |
| 20 | engagement-bait | temperature rounded to nearest integer |
| 21 | both-sides | alternates hot/cold seasonal extremes at each successive lead time |
| 22 | sponsored-content | always predicts perfect mild outdoor weather: 22°C |
| 23 | influencer | randomly chooses "golden hour aesthetic" or "dramatic storm content" |
| 24 | panic | catastrophically overreacts to any pressure change |
| 25 | nostalgia | reports actual observations from this exact date one year ago |
| 26 | astroturfed | climo with a hidden warming drift of 0.1°C/month since 2024-01-01 |
| 27 | record-breaker | predicts world records (all variables) whenever obs deviates from climo |
| 28 | record-roulette | fresh independent random draw within all-time observed min/max, per lead hour |
| 29 | record-climb | random walk that only rises, bounded above by all-time observed max |
| 30 | record-drop | random walk that only falls, bounded below by all-time observed min |
| 31 | climo-roulette | fresh independent random draw within today's climo diurnal min/max, per lead hour |
| 32 | climo-climb | random walk that only rises, bounded above by today's climo diurnal max |
| 33 | climo-drop | random walk that only falls, bounded below by today's climo diurnal min |
| 34 | mirror-time | forecast for +N hours = actual conditions N hours ago |
| 35-39 | random-aggregate-1..5 | each independently averages a random n (5-100) historical obs from the matching (month, hour) climo bucket |
| 40 | unit-confusion-fahrenheit | climo temp/dewpoint numeral reinterpreted as °F, converted back to °C |
| 41 | unit-confusion-kelvin | climo temp/dewpoint numeral reinterpreted as K, converted back to °C |
| 42 | unit-confusion-rankine | climo temp/dewpoint numeral reinterpreted as °R, converted back to °C |
| 43 | play-cards | each variable random-walks by a poker-hand-driven delta (5 cards, sum/10, negated if majority red) |
| 44 | head-to-head | 50 random historical obs run a single-elimination bracket against current conditions; the winner is the forecast |
| 45 | lookback-sine-24h | 24h-period sine wave per variable, amplitude from the last 24h's high/low, starts at current conditions |
| 46 | lookback-sine-24h-high | same, but starts (and ends) at the last-24h high |
| 47 | lookback-sine-24h-low | same, but starts (and ends) at the last-24h low |
| 48-50 | lookback-sine-7d(-high/-low) | same three shapes, high/low from the last 7 days |
| 51-53 | lookback-sine-7d-random(-high/-low) | same three shapes, high/low from one random historical 7-day window |
| 54-56 | lookback-sine-100d(-high/-low) | same three shapes, high/low from the last 100 days |
| 57-59 | lookback-sine-alltime(-high/-low) | same three shapes, high/low from the full observed history |
| 60 | fast-sine-24h | alternates high/low at each successive lead (last 24h window), not a real sine |
| 61 | fast-sine-7d | same, last 7 days |
| 62 | fast-sine-100d | same, last 100 days |
| 63 | fast-sine-alltime | same, full history |
| 64 | true-random | uniform random between absolute zero and 60,000°F, converted to °C (pressure reuses the raw numeric range, unitless) |
| 65 | persistence-flicker | climo, but each run picks one chance and every lead independently rolls persistence-instead-of-climo against it |
| 66 | angry-peer-review | mean of all other members plus much wider noise than peer-review |
| 67 | og-only | mean of the original 26 members (1-26), excluding peer-review (17) |
| 68 | sine-reviewer | mean of every lookback-sine and fast-sine member (45-63) |
| 69 | sine-integrator | arithmetic sum (not mean) of every sine member (45-63), treating them as superposable waves |
| 70 | self_correction | standard self-correction member, see [self_correction.md](self_correction.md) -- member_id=0's own mean minus bogo's learned bias; excluded from the mean it's derived from |

## Step bounds (drunkard, blind-drunkard, chaos, mercury-retrograde, grant-funded)

| Variable | Base step bound |
|----------|----------------|
| temperature | ±5.0 °C |
| dewpoint | ±3.0 °C |
| pressure | ±3.0 hPa |

chaos uses 3× base; mercury-retrograde uses 10× during retrograde, 0.5× otherwise.

## Extremes sources

- **seasonal extremes** (breaking-news, both-sides): observed min/max for the current calendar month.
- **all-time extremes** (record-roulette, record-climb, record-drop, lookback-sine-alltime family, fast-sine-alltime): observed min/max across the full tempest_obs history, no month filter.
- **climo diurnal extremes** (climo-roulette, climo-climb, climo-drop): min/max of the climo (month, hour) bucket means across all 24 hours of the current month — the typical diurnal swing for this time of year, not a raw observed extreme.
- **rolling windows** (lookback-sine and fast-sine families): observed min/max over the last 24h, last 7 days, one random historical 7-day window, or the last 100 days, each a plain min/max query bounded by timestamp with no month/hour bucketing.

Each dewpoint draw is capped at the paired temperature draw for that step.

## Sine members

lookback-sine and fast-sine both derive amplitude from a variable's high/low over some lookback window (see above), applied independently to temperature, dewpoint, and pressure using each variable's own current observed value as the wave's reference point. lookback-sine samples a genuine 24h-period sine (zero-start: `current + amp*sin(phase)`) or cosine (high/low-start: `mid ± amp*cos(phase)`) at the four lead hours; fast-sine ignores the wave shape and just alternates high/low at each successive lead. Neither variant was specified for pressure in the original idea (only temperature and dewpoint were) — pressure gets the same treatment for consistency, since every bogo member must return all three variables.

sine-reviewer and sine-integrator both read every lookback-sine and fast-sine member (ids 45-63). sine-reviewer takes their plain mean. sine-integrator adds their sampled values instead of averaging: since every sine member samples the same fixed lead hours off a curve sharing the same 24h period, summing the sampled values at each lead is equivalent to superposing the underlying waves and sampling the result — no continuous-wave math needed.

## Physical constraints

All members clamp output to world-record extremes and enforce dewpoint ≤ temperature. The ensemble mean also enforces the dewpoint constraint.

## Expected performance

Poor. The members have zero collective skill. The ensemble mean will sink toward climatology as their random errors cancel, but it will never beat a real model. Its purpose is entertainment, calibration of a lower skill bound, and occasionally making the dashboard charts look like abstract art.

## Confidence

Every member here gets a confidence value computed against the shared default
fingerprint, matched against its own scored history by calendar day. This is also
the first time bogo has had real per-member skill weights at all (`NEEDS_WEIGHTS`
newly declared); member_id=0's combination multiplies each member's weight by its
own confidence via `models/_confidence.py`'s `combine_pattern`, so a member's actual
track record finally matters here instead of every member counting equally
regardless of quality. See [confidence.md](confidence.md) for the full design.
