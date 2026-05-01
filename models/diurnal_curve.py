# diurnal_curve: fits a daily temperature cycle from recent observations and
# projects it forward, anchored to current or midnight conditions.
# three curve types: sine (np.linalg.lstsq), piecewise (circular lerp),
# asymmetric (two half-cosine segments between trough and peak).
# solar members use physics-derived phase (solar noon + 2h) with
# amplitude/baseline from 30d data.
# member_id=0 is the performance-weighted mean of all members when weights are
# available, otherwise equal-weighted.

import datetime as dt
import math
import statistics

import numpy as np

import db

MODEL_ID = 6
MODEL_NAME = "diurnal_curve"
NEEDS_CONN_IN = True
NEEDS_WEIGHTS = True
NEEDS_LOCATION = True

LEAD_HOURS = [6, 12, 18, 24]

# pressure intentionally omitted
VAR_COL = {
    "temperature": "air_temp",
    "dewpoint":    "dew_point",
}

_CURVES    = ["sine", "piecewise", "asymmetric"]
_LOOKBACKS = [(7, "7d"), (14, "14d"), (30, "30d"), (None, "yr")]
_ANCHORS   = ["current", "midnight", "none"]

# flat member list: (member_id, curve, lookback_label, lookback_days, anchor)
_MEMBERS = []
for _c_idx, _curve in enumerate(_CURVES):
    for _l_idx, (_days, _label) in enumerate(_LOOKBACKS):
        for _a_idx, _anchor in enumerate(_ANCHORS):
            _mid = _c_idx * 12 + _l_idx * 3 + _a_idx + 1
            _MEMBERS.append((_mid, _curve, _label, _days, _anchor))
for _a_idx, _anchor in enumerate(_ANCHORS):
    _MEMBERS.append((37 + _a_idx, "solar", "30d", 30, _anchor))

_ALL_MEMBER_IDS = [m[0] for m in _MEMBERS]


def _local_midnight_ts(ts: int) -> int:
    d = dt.datetime.fromtimestamp(ts)
    return int(d.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())


def _local_hour_float(ts: int) -> float:
    d = dt.datetime.fromtimestamp(ts)
    return d.hour + d.minute / 60.0 + d.second / 3600.0


def _hour_means(
    obs_rows: list,
    col: str,
    min_obs_per_bucket: int = 3,
    min_buckets: int = 12,
) -> dict[int, float] | None:
    buckets: dict[int, list[float]] = {}
    for row in obs_rows:
        v = row[col]
        if v is None:
            continue
        h = dt.datetime.fromtimestamp(row["timestamp"]).hour
        buckets.setdefault(h, []).append(v)
    populated = {h: vals for h, vals in buckets.items() if len(vals) >= min_obs_per_bucket}
    if len(populated) < min_buckets:
        return None
    return {h: sum(vals) / len(vals) for h, vals in populated.items()}


def _fit_sine(hm: dict[int, float]) -> tuple[float, float, float] | None:
    hours = sorted(hm.keys())
    if len(hours) < 3:
        return None
    TWO_PI = 2 * math.pi
    X = np.array([[math.sin(TWO_PI * h / 24), math.cos(TWO_PI * h / 24), 1.0]
                  for h in hours])
    y = np.array([hm[h] for h in hours])
    result = np.linalg.lstsq(X, y, rcond=None)
    coeffs = result[0]
    if not np.all(np.isfinite(coeffs)):
        return None
    return (float(coeffs[0]), float(coeffs[1]), float(coeffs[2]))


def _eval_sine(t: float, A: float, B: float, C: float) -> float:
    TWO_PI = 2 * math.pi
    return A * math.sin(TWO_PI * t / 24) + B * math.cos(TWO_PI * t / 24) + C


def _eval_piecewise(t: float, hm: dict[int, float]) -> float | None:
    if len(hm) < 2:
        return None
    hours = sorted(hm.keys())
    t = t % 24
    # build pairs, wrapping the first point at h+24 to handle midnight crossover
    for i in range(len(hours)):
        h0 = hours[i]
        h1 = hours[(i + 1) % len(hours)]
        v0 = hm[h0]
        v1 = hm[h1]
        if i == len(hours) - 1:
            h1 += 24  # wrap sentinel
        if h0 <= t < h1:
            frac = (t - h0) / (h1 - h0)
            return v0 + frac * (v1 - v0)
    # t is >= last hour but < hours[0]+24 (covered by wrap above on last iteration)
    return hm[hours[-1]]


def _eval_asymmetric(t: float, hm: dict[int, float]) -> float | None:
    if len(hm) < 2:
        return None
    t_min = min(hm, key=hm.__getitem__)
    t_max = max(hm, key=hm.__getitem__)
    v_min = hm[t_min]
    v_max = hm[t_max]
    rise_len = (t_max - t_min) % 24
    if rise_len == 0:
        return v_min
    fall_len = 24 - rise_len
    t_rel = (t - t_min) % 24
    if t_rel < rise_len:
        return v_min + (v_max - v_min) * (1 - math.cos(math.pi * t_rel / rise_len)) / 2
    else:
        t_fall = t_rel - rise_len
        return v_max + (v_min - v_max) * (1 - math.cos(math.pi * t_fall / fall_len)) / 2


def _solar_peak_hour(lat_deg: float, ts: int) -> float | None:
    doy = dt.datetime.fromtimestamp(ts).timetuple().tm_yday
    decl = math.radians(-23.45 * math.cos(math.radians(360 / 365 * (doy + 10))))
    lat = math.radians(lat_deg)
    cos_ha = -math.tan(lat) * math.tan(decl)
    if cos_ha >= 1.0:
        return None  # polar night
    # peak temperature ≈ solar noon + 2 hours (solar noon ≈ 12:00 local time)
    return 14.0


def _eval(curve: str, label: str, variable: str, t: float,
          hm_cache: dict, sine_cache: dict,
          solar_peak: float | None = None) -> float | None:
    if curve == "solar":
        hm = hm_cache.get(("30d", variable))
        if hm is None or solar_peak is None:
            return None
        values = list(hm.values())
        amp = (max(values) - min(values)) / 2
        base = (max(values) + min(values)) / 2
        return base + amp * math.cos(2 * math.pi * (t - solar_peak) / 24)

    hm = hm_cache.get((label, variable))
    if hm is None:
        return None

    if curve == "sine":
        params = sine_cache.get((label, variable))
        if params is None:
            return None
        return _eval_sine(t, *params)
    elif curve == "piecewise":
        return _eval_piecewise(t, hm)
    elif curve == "asymmetric":
        return _eval_asymmetric(t, hm)
    return None


def run(obs, issued_at: int, *, conn_in, weights=None, location=None) -> list[dict]:
    if location is None:
        location = db.tempest_station_location(conn_in)

    t_now = _local_hour_float(obs["timestamp"])
    midnight_ts = _local_midnight_ts(obs["timestamp"])
    midnight_obs = db.nearest_tempest_obs(conn_in, midnight_ts, window_sec=3600)

    # precompute solar peak_hour once (doesn't change per lead/variable)
    solar_peak = _solar_peak_hour(location[0], issued_at) if location else None

    # fetch observations per lookback label (4 queries max)
    raw_cache: dict[str, list] = {}
    for days, label in _LOOKBACKS:
        if days is None:
            # year-ago: ±15 days around same calendar date 1 year ago
            start = issued_at - 380 * 86400
            end = issued_at - 350 * 86400
        else:
            start = issued_at - days * 86400
            end = issued_at
        raw_cache[label] = db.tempest_obs_in_range(conn_in, start, end)

    # compute hour means per (label, variable)
    hm_cache: dict[tuple, dict | None] = {}
    for _, label in _LOOKBACKS:
        obs_rows = raw_cache[label]
        for variable, col in VAR_COL.items():
            hm_cache[(label, variable)] = _hour_means(obs_rows, col)

    # fit sine parameters per (label, variable)
    sine_cache: dict[tuple, tuple | None] = {}
    for _, label in _LOOKBACKS:
        for variable in VAR_COL:
            hm = hm_cache.get((label, variable))
            sine_cache[(label, variable)] = _fit_sine(hm) if hm is not None else None

    rows = []
    for lead in LEAD_HOURS:
        valid_at = obs["timestamp"] + lead * 3600
        t_valid = _local_hour_float(valid_at)

        member_vals: dict[int, dict[str, float | None]] = {}

        for mid, curve, label, days, anchor in _MEMBERS:
            member_vals[mid] = {}

            for variable, col in VAR_COL.items():
                raw = _eval(curve, label, variable, t_valid,
                            hm_cache, sine_cache, solar_peak)

                if raw is None:
                    value = None
                elif anchor == "none":
                    value = raw
                elif anchor == "current":
                    curve_now = _eval(curve, label, variable, t_now,
                                      hm_cache, sine_cache, solar_peak)
                    obs_val = obs[col]
                    if curve_now is None or obs_val is None:
                        value = None
                    else:
                        value = raw + (obs_val - curve_now)
                elif anchor == "midnight":
                    curve_midnight = _eval(curve, label, variable, 0.0,
                                           hm_cache, sine_cache, solar_peak)
                    obs_midnight = midnight_obs[col] if midnight_obs is not None else None
                    if curve_midnight is None or obs_midnight is None:
                        value = None
                    else:
                        value = raw + (obs_midnight - curve_midnight)
                else:
                    value = None

                member_vals[mid][variable] = value
                rows.append({
                    "model_id": MODEL_ID,
                    "model": MODEL_NAME,
                    "member_id": mid,
                    "issued_at": issued_at,
                    "valid_at": valid_at,
                    "lead_hours": lead,
                    "variable": variable,
                    "value": value,
                })

        # member_id=0: weighted mean + spread across all members
        for variable in VAR_COL:
            valid_pairs = [
                (mid, member_vals[mid][variable])
                for mid in _ALL_MEMBER_IDS
                if member_vals[mid][variable] is not None
            ]
            if not valid_pairs:
                mean = None
            elif weights:
                w_pairs = [
                    (weights.get((mid, variable, lead), None), v)
                    for mid, v in valid_pairs
                ]
                if any(w is None for w, _ in w_pairs):
                    mean = sum(v for _, v in valid_pairs) / len(valid_pairs)
                else:
                    total_w = sum(w for w, _ in w_pairs)
                    mean = sum(w * v for w, v in w_pairs) / total_w
            else:
                mean = sum(v for _, v in valid_pairs) / len(valid_pairs)
            spread = (
                statistics.pstdev([v for _, v in valid_pairs])
                if len(valid_pairs) > 1
                else None
            )
            rows.append({
                "model_id": MODEL_ID,
                "model": MODEL_NAME,
                "member_id": 0,
                "issued_at": issued_at,
                "valid_at": valid_at,
                "lead_hours": lead,
                "variable": variable,
                "value": mean,
                "spread": spread,
            })

    return rows
