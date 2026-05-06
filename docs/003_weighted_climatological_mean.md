# 003 Weighted Climatological Mean Model

## Overview

This model is a direct evolution of the climatological mean (002). Rather than treating all historical observations in a (month, hour) bucket equally, it assigns higher weight to more recent observations, on the premise that recent conditions are a stronger signal of what the near future will look like than older observations.

The 30-observation minimum gate from model 002 is removed here. Weighting handles the thin-data problem implicitly: a bucket with very few observations will be dominated by whatever observations exist. At extremely low observation counts the model may still produce a poor forecast, but seeing how poor it is is part of the point.

## Members

For each lead time, all observations in the (month, hour) bucket are fetched with their timestamps. Each member applies a different weight function `w(age_days)` to compute a weighted mean. Member 0 is the skill-score weighted mean across all members (equal-weighted when no weights are available), with spread equal to the population standard deviation of member forecasts.

### Static tier members

Each member isolates a specific recency tier or combination to test out how the different time windows perform.

| ID | Name              | Weight rule |
|----|-------------------|-------------|
| 1  | today-only        | age < 1d → 20, else 1 |
| 2  | week-only         | age < 7d → 7, else 1 |
| 3  | month-only        | age < 30d → 7, else 1 |
| 4  | week+month        | age < 7d → 20, age < 30d → 7, else 1 |
| 5  | today+week+month  | age < 1d → 50, age < 7d → 20, age < 30d → 7, else 1 |

The tier boundaries correspond to the timescales of distinct weather regimes, where synoptic systems last 3–7 days, persistent patterns (blocking highs, anomalous ridges) last 2–4 weeks, and beyond that conditions are largely climatological.

### Exponential decay members

Continuous decay avoids arbitrary tier boundaries. `weight = e^(-k * age_days)`.

| ID | Name         | k    | Half-life |
|----|--------------|------|-----------|
| 6  | exp-steep    | 0.50 | ~1.4d     |
| 7  | exp-fast     | 0.20 | ~3.5d     |
| 8  | exp-moderate | 0.10 | ~7d       |
| 9  | exp-gentle   | 0.03 | ~23d      |

The four constants span roughly an order of magnitude: `exp-steep` nearly ignores anything older than a week and might be more similar to model 001. `exp-gentle` still gives meaningful weight to observations from a month ago.

### Model ensemble

Member 0 uses skill-score weights from `tune` when available; equal weighting is the fallback before sufficient scoring history exists. The spread is the standard deviation of the member forecasts, showing how much the members disagree in a run.
