# diurnal_rate_anomaly: is a signal changing faster or slower than its own
# climatological rate/level for this specific (month, hour) right now, rather than
# whether its current level is high or low (climo_deviation's domain). One
# mechanism, two variables: z-score the live signal against its own (month, hour)
# bucket's historical mean and stdev, bucket the z-score into above/normal/below,
# then look up the learned conditional-mean delta for that category -- the same
# bucket-history/average-future-delta/look-it-up-live pattern as wind_veer_detector,
# frontal_trigger, and regime_stability.
#
# From the 2026-09-22 "Synoptic Signal Ideas" brainstorm, diurnal_rate_anomaly
# section.
#
# members:
#   1  temp_slope_anomaly  air_temp's trailing-1h rate of change, z-scored against
#                          the historical rate at this same (month, hour) -- is it
#                          warming/cooling faster or slower than usual for this
#                          hour, not whether the level itself is anomalous. Best 1-6h.
#   2  wind_rate_anomaly   wind_avg's current level, z-scored against its own
#                          (month, hour) climatology. Anomalously windy at an hour
#                          that's normally calm usually means synoptic forcing is
#                          overriding the local diurnal wind cycle. Best 3-12h.
#   3  self_correction     standard self-correction member (models/_self_correction.py)
#                          -- member_id=0 minus this model's own learned bias

import datetime
import statistics

import db
import models._confidence as _confidence
import models._self_correction as _self_correction
from models._climo_weights import LEAD_HOURS, VARIABLES
from models._utils import _sector
from models.surface_signs import _find_nearest_ts

MODEL_ID = 25
MODEL_NAME = "diurnal_rate_anomaly"
NEEDS_CONN_IN = True
NEEDS_CONN_OUT = True
NEEDS_WEIGHTS = True
NEEDS_ALL_OBS = True
NEEDS_MATCH_HISTORY = True

_SELF_CORRECTION_MEMBER = 3

_RATE_WINDOW_SEC = 3600     # trailing 1h, matches solar_ramp/pressure_trend_cascade's ramp window
_LOOKUP_SEC = 600           # +/- 10 min, finding the trailing-window anchor obs
_FUTURE_LOOKUP_SEC = 900    # +/- 15 min
_MIN_SAMPLES = 3            # minimum historical pairs per (cat, col, lead), and per climo bucket
_Z_THRESHOLD = 0.75         # |z| below this: normal; above/below it otherwise

_MEMBERS = [
    (1, "temp_slope_anomaly"),
    (2, "wind_rate_anomaly"),
]
_ALL_MEMBER_IDS = [mid for mid, _ in _MEMBERS]


def _rate(now_ts, now_val, by_ts, sorted_ts):
    """air_temp's trailing-_RATE_WINDOW_SEC rate of change ending at now_ts, in
    degrees/hour. None when now_val is missing, no anchor obs falls within
    _LOOKUP_SEC of now_ts - _RATE_WINDOW_SEC, or that anchor's own air_temp is
    missing."""
    if now_val is None:
        return None
    ts_past = _find_nearest_ts(sorted_ts, now_ts - _RATE_WINDOW_SEC, _LOOKUP_SEC)
    if ts_past is None:
        return None
    past_val = by_ts[ts_past]["air_temp"]
    if past_val is None:
        return None
    hours = (now_ts - ts_past) / 3600.0
    if hours <= 0:
        return None
    return (now_val - past_val) / hours


def _bucket_stats(values_by_key):
    """{key: [values]} -> {key: (mean, stdev)}, dropped when a bucket has fewer
    than _MIN_SAMPLES values or zero variance (can't z-score against it)."""
    stats = {}
    for key, vals in values_by_key.items():
        if len(vals) < _MIN_SAMPLES:
            continue
        sd = statistics.pstdev(vals)
        if sd <= 0:
            continue
        stats[key] = (statistics.mean(vals), sd)
    return stats


def _build_climo(all_obs, rate_by_ts):
    """Buckets air_temp's trailing rate and wind_avg's raw level by (month, hour)
    of each observation's own local time, returning a (mean, stdev) pair for each
    bucket that clears _bucket_stats's sample/variance gate."""
    rate_vals: dict[tuple[int, int], list] = {}
    wind_vals: dict[tuple[int, int], list] = {}
    for row in all_obs:
        t = datetime.datetime.fromtimestamp(row["timestamp"])
        key = (t.month, t.hour)
        rate = rate_by_ts.get(row["timestamp"])
        if rate is not None:
            rate_vals.setdefault(key, []).append(rate)
        if row["wind_avg"] is not None:
            wind_vals.setdefault(key, []).append(row["wind_avg"])
    return _bucket_stats(rate_vals), _bucket_stats(wind_vals)


def _zscore(value, stats, key):
    if value is None:
        return None
    cell = stats.get(key)
    if cell is None:
        return None
    mean, sd = cell
    return (value - mean) / sd


def _category(z):
    if z is None:
        return None
    if z > _Z_THRESHOLD:
        return "above"
    if z < -_Z_THRESHOLD:
        return "below"
    return "normal"


def _categories(ts, rate_val, wind_val, climo_rate, climo_wind):
    t = datetime.datetime.fromtimestamp(ts)
    key = (t.month, t.hour)
    return {
        1: _category(_zscore(rate_val, climo_rate, key)),
        2: _category(_zscore(wind_val, climo_wind, key)),
    }


def _build_conditionals(all_obs, rate_by_ts, climo_rate, climo_wind):
    """Conditional mean delta per (member_id, category, column, lead), learned
    from history -- the standard bucket/average pattern."""
    by_ts = {row["timestamp"]: row for row in all_obs}
    sorted_ts = sorted(by_ts)
    all_cols = list(VARIABLES.values())
    accum = {mid: {} for mid, _ in _MEMBERS}

    for ts in sorted_ts:
        row_now = by_ts[ts]
        cats = _categories(ts, rate_by_ts.get(ts), row_now["wind_avg"], climo_rate, climo_wind)
        for mid, cat in cats.items():
            if cat is None:
                continue
            for lead in LEAD_HOURS:
                ts_fut = _find_nearest_ts(sorted_ts, ts + lead * 3600, _FUTURE_LOOKUP_SEC)
                if ts_fut is None:
                    continue
                row_fut = by_ts[ts_fut]
                for col in all_cols:
                    v_now = row_now[col]
                    v_fut = row_fut[col]
                    if v_now is not None and v_fut is not None:
                        accum[mid].setdefault((cat, col, lead), []).append(v_fut - v_now)

    return {
        mid: {k: sum(v) / len(v) for k, v in cell.items() if len(v) >= _MIN_SAMPLES}
        for mid, cell in accum.items()
    }


def run(obs, issued_at, *, conn_in, conn_out=None, weights=None, all_obs=None,
        member_history=None, default_matches=None):
    if all_obs is None:
        all_obs = db.tempest_obs_in_range(conn_in, 0, issued_at)

    by_ts = {row["timestamp"]: row for row in all_obs}
    sorted_ts = sorted(by_ts)
    rate_by_ts = {
        ts: r for ts in sorted_ts
        if (r := _rate(ts, by_ts[ts]["air_temp"], by_ts, sorted_ts)) is not None
    }
    climo_rate, climo_wind = _build_climo(all_obs, rate_by_ts)
    conds = _build_conditionals(all_obs, rate_by_ts, climo_rate, climo_wind)

    live_rate = _rate(obs["timestamp"], obs["air_temp"], by_ts, sorted_ts)
    live_cat = _categories(obs["timestamp"], live_rate, obs["wind_avg"], climo_rate, climo_wind)

    cell_confidences_by_var_lead = {
        (variable, lead): _confidence.member_confidences(
            member_history, default_matches, _ALL_MEMBER_IDS + [_SELF_CORRECTION_MEMBER], variable, lead
        )
        for variable in VARIABLES
        for lead in LEAD_HOURS
    }

    rows = []
    for mid, _ in _MEMBERS:
        cat = live_cat[mid]
        for variable, col in VARIABLES.items():
            obs_val = obs[col]
            for lead in LEAD_HOURS:
                if cat is None or obs_val is None:
                    value = None
                else:
                    mean_delta = conds[mid].get((cat, col, lead))
                    value = obs_val + mean_delta if mean_delta is not None else None
                rows.append({
                    "model_id": MODEL_ID,
                    "model": MODEL_NAME,
                    "member_id": mid,
                    "issued_at": issued_at,
                    "valid_at": obs["timestamp"] + lead * 3600,
                    "lead_hours": lead,
                    "variable": variable,
                    "value": value,
                    "confidence": cell_confidences_by_var_lead[(variable, lead)].get(mid),
                })

    for variable, col in VARIABLES.items():
        for lead in LEAD_HOURS:
            valid_at = obs["timestamp"] + lead * 3600
            cell_confidences = cell_confidences_by_var_lead[(variable, lead)]
            valid_pairs = []
            for mid, _ in _MEMBERS:
                cat = live_cat[mid]
                obs_val = obs[col]
                if cat is None or obs_val is None:
                    continue
                mean_delta = conds[mid].get((cat, col, lead))
                if mean_delta is not None:
                    valid_pairs.append((mid, obs_val + mean_delta))

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
                if len(valid_pairs) > 1 else None
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
