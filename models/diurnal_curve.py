# diurnal_curve: fits a daily temperature cycle from recent observations and
# projects it forward, anchored to current or midnight conditions.
# three curve types: sine (np.linalg.lstsq), piecewise (circular lerp),
# asymmetric (two half-cosine segments between trough and peak).
# solar members use physics-derived phase (solar noon + 2h) with
# amplitude/baseline from 30d data.
# member 40 (range_scaled) rescales the 7d piecewise curve's amplitude by how
# today's morning trajectory compares to the same-window climatological rate.
# member 41 (wind_sector_conditioned) fits the piecewise curve only against 30d
# obs sharing the current prevailing 8-point wind sector.
# member 42 (self_correction): standard self-correction member
# (models/_self_correction.py) -- member_id=0 minus this model's own learned
# historical bias.
# member_id=0 is the performance-weighted mean of all members when weights are
# available, otherwise equal-weighted.

import datetime as dt
import math
import statistics
import time

import numpy as np

import db
import models._confidence as _confidence
import models._self_correction as _self_correction
from models._utils import _sector

MODEL_ID = 6
MODEL_NAME = "diurnal_curve"
NEEDS_CONN_IN = True
NEEDS_CONN_OUT = True
NEEDS_WEIGHTS = True
NEEDS_LOCATION = True
NEEDS_MATCH_HISTORY = True

_SELF_CORRECTION_MEMBER = 42

from models._climo_weights import LEAD_HOURS

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

_ALL_MEMBER_IDS = [m[0] for m in _MEMBERS] + [40, 41]
_CONFIDENCE_MEMBER_IDS = _ALL_MEMBER_IDS + [_SELF_CORRECTION_MEMBER]

# member 40 (range_scaled): trailing-24h range needs enough obs to trust the
# actual high/low rather than a partial-day slice
_RANGE_MIN_OBS = 20
_RANGE_RATIO_MIN = 0.3
_RANGE_RATIO_MAX = 2.5

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
    # compute cos(hour angle) to detect polar night only
    doy = dt.datetime.fromtimestamp(ts).timetuple().tm_yday
    decl = math.radians(-23.45 * math.cos(math.radians(360 / 365 * (doy + 10))))
    lat = math.radians(lat_deg)
    cos_ha = -math.tan(lat) * math.tan(decl)
    if cos_ha >= 1.0:
        return None  # polar night — solar curve is undefined
    # constant approximation: peak temperature ≈ solar noon + 2h, solar noon ≈ 12:00 local
    # does not account for longitude offset or seasonal solar noon shift
    return 14.0

def _wind_sector8(degrees: float) -> int:
    """8-point compass sector for a wind direction in degrees (0=N, clockwise)."""
    return int((degrees + 22.5) / 45) % 8

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

def run(obs, issued_at: int, *, conn_in, conn_out=None, weights=None, location=None,
        member_history=None, default_matches=None) -> list[dict]:
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

    # member 40 (range_scaled): scale the 7d piecewise curve's amplitude by how
    # today's morning trajectory (obs since local midnight) compares to the same
    # window's climatological rate from that curve, then current-anchor it.
    # Faster-than-climo warming (clear/calm) expands the range; slower (cloudy/
    # windy) compresses it.
    range_state: dict[str, tuple[float, float, float] | None] = {}
    for variable, col in VAR_COL.items():
        hm7d = hm_cache.get(("7d", variable))
        if hm7d is None:
            range_state[variable] = None
            continue

        curve_vals = list(hm7d.values())
        curve_mean = sum(curve_vals) / len(curve_vals)
        curve_range = max(curve_vals) - min(curve_vals)

        trailing_vals = [
            row[col] for row in raw_cache["7d"]
            if row["timestamp"] >= issued_at - 86400 and row[col] is not None
        ]
        trailing_range = (
            max(trailing_vals) - min(trailing_vals)
            if len(trailing_vals) >= _RANGE_MIN_OBS else None
        )
        baseline_range = trailing_range if trailing_range is not None else curve_range

        obs_val = obs[col]
        midnight_val = midnight_obs[col] if midnight_obs is not None else None
        climo_now = _eval_piecewise(t_now, hm7d)
        climo_midnight = _eval_piecewise(0.0, hm7d)
        if (obs_val is not None and midnight_val is not None and t_now > 0.5
                and climo_now is not None and climo_midnight is not None):
            actual_rate = (obs_val - midnight_val) / t_now
            climo_rate = (climo_now - climo_midnight) / t_now
        else:
            actual_rate = climo_rate = None

        if actual_rate is not None and climo_rate is not None and abs(climo_rate) > 1e-6:
            ratio = max(_RANGE_RATIO_MIN, min(_RANGE_RATIO_MAX, actual_rate / climo_rate))
        else:
            ratio = 1.0

        predicted_range = baseline_range * ratio if baseline_range else None
        range_scale = (
            predicted_range / curve_range
            if predicted_range is not None and curve_range > 1e-6
            else 1.0
        )
        curve_now = (
            curve_mean + (climo_now - curve_mean) * range_scale
            if climo_now is not None else None
        )
        shift = obs_val - curve_now if (obs_val is not None and curve_now is not None) else 0.0
        range_state[variable] = (curve_mean, range_scale, shift)

    # member 41 (wind_sector_conditioned): hour-of-day means from the last 30d,
    # filtered to obs sharing the current prevailing 8-point wind sector, so an
    # NW day and a S day get their own curve shape instead of one pooled fit.
    prevailing_sector = (
        _wind_sector8(obs["wind_direction"]) if obs["wind_direction"] is not None else None
    )
    sector_hm: dict[str, dict[int, float] | None] = {}
    sector_shift: dict[str, float] = {}
    if prevailing_sector is not None:
        sector_rows = [
            row for row in raw_cache["30d"]
            if row["wind_direction"] is not None
            and _wind_sector8(row["wind_direction"]) == prevailing_sector
        ]
        for variable, col in VAR_COL.items():
            hm_sector = _hour_means(sector_rows, col)
            sector_hm[variable] = hm_sector
            if hm_sector is not None:
                obs_val = obs[col]
                curve_now = _eval_piecewise(t_now, hm_sector)
                sector_shift[variable] = (
                    obs_val - curve_now if (obs_val is not None and curve_now is not None) else 0.0
                )
    else:
        for variable in VAR_COL:
            sector_hm[variable] = None

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

        member_vals[40] = {}
        for variable, col in VAR_COL.items():
            state = range_state.get(variable)
            hm7d = hm_cache.get(("7d", variable))
            if state is None or hm7d is None:
                member_vals[40][variable] = None
            else:
                curve_mean, range_scale, shift = state
                raw = _eval_piecewise(t_valid, hm7d)
                member_vals[40][variable] = (
                    curve_mean + (raw - curve_mean) * range_scale + shift
                    if raw is not None else None
                )

        member_vals[41] = {}
        for variable in VAR_COL:
            hm_sector = sector_hm.get(variable)
            if hm_sector is None:
                member_vals[41][variable] = None
            else:
                raw = _eval_piecewise(t_valid, hm_sector)
                member_vals[41][variable] = (
                    raw + sector_shift.get(variable, 0.0) if raw is not None else None
                )

        # member rows + member_id=0: weighted mean + spread across all members
        for variable in VAR_COL:
            cell_confidences = _confidence.member_confidences(
                member_history, default_matches, _CONFIDENCE_MEMBER_IDS, variable, lead
            )
            for mid in _ALL_MEMBER_IDS:
                rows.append({
                    "model_id": MODEL_ID,
                    "model": MODEL_NAME,
                    "member_id": mid,
                    "issued_at": issued_at,
                    "valid_at": valid_at,
                    "lead_hours": lead,
                    "variable": variable,
                    "value": member_vals[mid][variable],
                    "confidence": cell_confidences.get(mid),
                })

            valid_pairs = [
                (mid, member_vals[mid][variable])
                for mid in _ALL_MEMBER_IDS
                if member_vals[mid][variable] is not None
            ]
            if not valid_pairs:
                mean, group_confidence = None, None
            elif weights:
                member_weights = {
                    mid: weights.get((mid, variable, lead, _sector(valid_at)))
                    for mid, _ in valid_pairs
                }
                confidences = {mid: cell_confidences.get(mid) for mid, _ in valid_pairs}
                mean, group_confidence = _confidence.combine_pattern(
                    valid_pairs, member_weights, confidences
                )
            else:
                mean = sum(v for _, v in valid_pairs) / len(valid_pairs)
                group_confidence = _confidence.average_confidence(
                    [cell_confidences.get(mid) for mid, _ in valid_pairs]
                )
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
                "confidence": group_confidence,
            })

            corrected = _self_correction.corrected_value(
                conn_out, MODEL_ID, variable, lead, mean, issued_at
            )
            rows.append({
                "model_id": MODEL_ID,
                "model": MODEL_NAME,
                "member_id": _SELF_CORRECTION_MEMBER,
                "issued_at": issued_at,
                "valid_at": valid_at,
                "lead_hours": lead,
                "variable": variable,
                "value": corrected,
                "confidence": cell_confidences.get(_SELF_CORRECTION_MEMBER),
            })

    return rows
