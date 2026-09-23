# _pressure_tide: shared preprocessing helper, not a member of any model. Learns
# the station's own small twice-daily solar pressure tide from its long-run
# hourly mean residual, then lets a caller detide (and re-tide) a raw
# station_pressure reading before reading it as a signal.
#
# Real synoptic weather passes through every hour of the day across a long
# enough history and averages out; what's left in the by-hour mean is the
# systematic clock-time-locked component -- the semidiurnal (and any diurnal)
# solar pressure tide -- learned directly from this station rather than a
# textbook formula. See the 2026-09-22 "Synoptic Signal Ideas" brainstorm,
# pressure_trajectory section.

import datetime as dt

_MIN_SAMPLES_PER_HOUR = 5  # below this, an hour's own mean isn't trustworthy yet


def build_profile(all_obs: list[dict]) -> dict[int, float]:
    """{hour_of_day: residual_hpa}, only for hours with enough history. Empty on
    a fresh database -- callers must treat a missing hour as "no correction
    available yet", not fall back to some default."""
    buckets: dict[int, list[float]] = {}
    for row in all_obs:
        v = row.get("station_pressure")
        if v is None:
            continue
        h = dt.datetime.fromtimestamp(row["timestamp"]).hour
        buckets.setdefault(h, []).append(v)
    all_vals = [v for vals in buckets.values() for v in vals]
    if not all_vals:
        return {}
    overall_mean = sum(all_vals) / len(all_vals)
    return {
        h: (sum(vals) / len(vals)) - overall_mean
        for h, vals in buckets.items()
        if len(vals) >= _MIN_SAMPLES_PER_HOUR
    }


def detided(value: float | None, ts: int, profile: dict[int, float]) -> float | None:
    """Subtract this timestamp's local-hour tide residual from a raw
    station_pressure reading. Returns value unchanged if value is None or the
    profile has no entry for that hour yet."""
    if value is None:
        return None
    residual = profile.get(dt.datetime.fromtimestamp(ts).hour)
    return value if residual is None else value - residual


def retided(value: float | None, ts: int, profile: dict[int, float]) -> float | None:
    """Inverse of detided: add the tide residual for ts's local hour back onto
    a detided value, e.g. to turn a decay model's detided prediction back into
    a real forecast pressure. Returns value unchanged if value is None or the
    profile has no entry for that hour yet."""
    if value is None:
        return None
    residual = profile.get(dt.datetime.fromtimestamp(ts).hour)
    return value if residual is None else value + residual
