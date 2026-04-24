# surface_signs: forecast from observable atmospheric signals at the Tempest station.
#
# inspired by Theophrastus's "Book of Signs" — reading physical cues (wind rotation,
# moisture tendency, cloud cover, convective activity) to anticipate weather change.
# each member isolates one signal type and applies conditional mean deltas learned
# from historical obs, following the same pattern as the zambretti member in
# pressure_tendency.py.

import bisect
import datetime
import statistics

import db
from models._climo_weights import LEAD_HOURS, VARIABLES

MODEL_ID = 9
MODEL_NAME = "surface_signs"
NEEDS_CONN_IN = True
NEEDS_WEIGHTS = True

_SIGNAL_WINDOW_SEC  = 3 * 3600  # 3h lookback for all signals
_LOOKUP_SEC         = 600       # ±10 min for historical ts matching
_FUTURE_LOOKUP_SEC  = 900       # ±15 min for future obs
_MIN_WIND_MS        = 1.5       # below this, wind direction is unreliable
_MIN_SAMPLES        = 3         # minimum historical pairs per (cat, col, lead)
_PRECIP_1H_MM       = 0.5       # mm/h threshold for active precipitation
_SOLAR_FLOOR_W      = 5.0       # W/m² floor to distinguish day from night
_SOLAR_MIN_SAMPLES  = 10        # minimum samples to compute climo solar mean
_VEER_THRESHOLD_DEG = 15.0      # degrees threshold for veering/backing classification


def _find_nearest_ts(sorted_ts, target, max_delta=600):
    """Binary search for the nearest timestamp within max_delta seconds of target."""
    if not sorted_ts:
        return None
    idx = bisect.bisect_left(sorted_ts, target)
    best = None
    best_d = max_delta + 1
    for i in (idx - 1, idx):
        if 0 <= i < len(sorted_ts):
            d = abs(sorted_ts[i] - target)
            if d <= max_delta and d < best_d:
                best_d = d
                best = sorted_ts[i]
    return best


def _obs_in_window(sorted_ts, by_ts, ts_start, ts_end):
    """Return obs rows for timestamps in [ts_start, ts_end] using bisect slicing."""
    lo = bisect.bisect_left(sorted_ts, ts_start)
    hi = bisect.bisect_right(sorted_ts, ts_end)
    return [by_ts[t] for t in sorted_ts[lo:hi]]


def _angular_diff(d1, d2):
    """Signed angular difference d2 - d1 in (-180, 180]. Positive = clockwise = veering."""
    diff = (d2 - d1 + 360) % 360
    if diff > 180:
        diff -= 360
    return diff


def _wind_rotation_category(window_obs):
    """
    Classify net wind direction change over the observation window.
    Returns "veering", "backing", "steady", or None if insufficient data.
    In the Northern Hemisphere, veering (clockwise) indicates post-frontal clearing;
    backing (counterclockwise) indicates frontal approach.
    """
    valid = [
        r for r in window_obs
        if r["wind_direction"] is not None
        and r["wind_avg"] is not None
        and r["wind_avg"] > _MIN_WIND_MS
    ]
    if len(valid) < 2:
        return None
    oldest = min(valid, key=lambda r: r["timestamp"])
    newest = max(valid, key=lambda r: r["timestamp"])
    net = _angular_diff(oldest["wind_direction"], newest["wind_direction"])
    if net > _VEER_THRESHOLD_DEG:
        return "veering"
    if net < -_VEER_THRESHOLD_DEG:
        return "backing"
    return "steady"


def _dp_trend_category(obs_now, obs_3h_ago):
    """
    Classify dewpoint spread (temp - dewpoint) trend over 3h.
    Narrowing spread means moisture is rising (frontal approach or boundary layer
    saturation). Widening means drying air (post-frontal clearing).
    Returns "narrowing", "steady", "widening", or None if data is missing.
    """
    if obs_now is None or obs_3h_ago is None:
        return None
    t_now = obs_now["air_temp"]
    d_now = obs_now["dew_point"]
    t_past = obs_3h_ago["air_temp"]
    d_past = obs_3h_ago["dew_point"]
    if any(v is None for v in (t_now, d_now, t_past, d_past)):
        return None
    spread_now = t_now - d_now
    spread_past = t_past - d_past
    delta = spread_now - spread_past
    if delta < -1.0:
        return "narrowing"
    if delta > 1.0:
        return "widening"
    return "steady"


def _build_solar_climo(all_obs):
    """
    Compute mean solar radiation by (calendar_month, hour_of_day) from obs history.
    Only considers observations with solar_radiation > _SOLAR_FLOOR_W (daytime only).
    Returns dict keyed by (month, hour); cells with < _SOLAR_MIN_SAMPLES are excluded.
    """
    buckets = {}
    for row in all_obs:
        sr = row["solar_radiation"]
        if sr is None or sr <= _SOLAR_FLOOR_W:
            continue
        dt = datetime.datetime.fromtimestamp(row["timestamp"])
        key = (dt.month, dt.hour)
        buckets.setdefault(key, []).append(sr)
    return {k: sum(v) / len(v) for k, v in buckets.items() if len(v) >= _SOLAR_MIN_SAMPLES}


def _solar_cloud_category(obs, solar_climo):
    """
    Classify cloud cover based on solar radiation deficit vs climatological mean.
    Returns "clear", "partial_cloud", "heavy_cloud", or None (nighttime / no data).
    """
    sr = obs["solar_radiation"]
    if sr is None or sr <= _SOLAR_FLOOR_W:
        return None
    dt = datetime.datetime.fromtimestamp(obs["timestamp"])
    climo_mean = solar_climo.get((dt.month, dt.hour))
    if climo_mean is None:
        return None
    deficit = 1.0 - sr / climo_mean
    if deficit > 0.7:
        return "heavy_cloud"
    if deficit > 0.3:
        return "partial_cloud"
    return "clear"


def _convective_category(window_obs, obs_1h_ago, obs_now):
    """
    Classify convective / precipitation state.
    Lightning takes priority; then active precipitation; then dry.
    Always returns a non-None string.
    precip_accum_day resets at midnight, so deltas are clamped to >= 0.
    """
    lightning = sum((r["lightning_count"] or 0) for r in window_obs)
    if lightning > 0:
        return "lightning"
    if obs_1h_ago is not None and obs_now is not None:
        p_now = obs_now["precip_accum_day"] or 0.0
        p_1h = obs_1h_ago["precip_accum_day"] or 0.0
        if max(0.0, p_now - p_1h) > _PRECIP_1H_MM:
            return "precip"
    return "dry"


def _build_signal_conditionals(signal_fn, sorted_ts, by_ts):
    """
    Learn conditional mean variable deltas for each (category, col, lead_hours) cell.
    signal_fn(ts) returns the category string at that timestamp, or None to skip.
    Cells with fewer than _MIN_SAMPLES pairs are excluded (returned as absent).
    """
    all_cols = list(VARIABLES.values())
    accum = {}
    for ts in sorted_ts:
        cat = signal_fn(ts)
        if cat is None:
            continue
        row_now = by_ts[ts]
        for lead in LEAD_HOURS:
            ts_fut = _find_nearest_ts(sorted_ts, ts + lead * 3600, _FUTURE_LOOKUP_SEC)
            if ts_fut is None:
                continue
            row_fut = by_ts[ts_fut]
            for col in all_cols:
                v_now = row_now[col]
                v_fut = row_fut[col]
                if v_now is not None and v_fut is not None:
                    accum.setdefault((cat, col, lead), []).append(v_fut - v_now)
    return {k: sum(v) / len(v) for k, v in accum.items() if len(v) >= _MIN_SAMPLES}


def run(obs, issued_at, *, conn_in, weights=None):
    all_obs = db.tempest_obs_in_range(conn_in, 0, issued_at)
    by_ts = {row["timestamp"]: row for row in all_obs}
    sorted_ts = sorted(by_ts)
    solar_climo = _build_solar_climo(all_obs)

    # signal closures for conditional learning — each captures what it needs
    def sig1(ts):
        window = _obs_in_window(sorted_ts, by_ts, ts - _SIGNAL_WINDOW_SEC, ts)
        return _wind_rotation_category(window)

    def sig2(ts):
        ts_past = _find_nearest_ts(sorted_ts, ts - _SIGNAL_WINDOW_SEC, _LOOKUP_SEC)
        if ts_past is None:
            return None
        return _dp_trend_category(by_ts[ts], by_ts[ts_past])

    def sig3(ts):
        return _solar_cloud_category(by_ts[ts], solar_climo)

    def sig4(ts):
        window = _obs_in_window(sorted_ts, by_ts, ts - _SIGNAL_WINDOW_SEC, ts)
        ts_1h = _find_nearest_ts(sorted_ts, ts - 3600, _LOOKUP_SEC)
        obs_1h = by_ts.get(ts_1h) if ts_1h is not None else None
        return _convective_category(window, obs_1h, by_ts[ts])

    conds = {
        1: _build_signal_conditionals(sig1, sorted_ts, by_ts),
        2: _build_signal_conditionals(sig2, sorted_ts, by_ts),
        3: _build_signal_conditionals(sig3, sorted_ts, by_ts),
        4: _build_signal_conditionals(sig4, sorted_ts, by_ts),
    }

    # live signal categories from current observation
    window_obs = _obs_in_window(
        sorted_ts, by_ts, obs["timestamp"] - _SIGNAL_WINDOW_SEC, obs["timestamp"]
    )
    obs_3h_ago = db.nearest_tempest_obs(
        conn_in, obs["timestamp"] - _SIGNAL_WINDOW_SEC, window_sec=_LOOKUP_SEC
    )
    obs_1h_ago = db.nearest_tempest_obs(
        conn_in, obs["timestamp"] - 3600, window_sec=_LOOKUP_SEC
    )
    live = {
        1: _wind_rotation_category(window_obs),
        2: _dp_trend_category(obs, obs_3h_ago) if obs_3h_ago is not None else None,
        3: _solar_cloud_category(obs, solar_climo),
        4: _convective_category(window_obs, obs_1h_ago, obs),
    }

    rows = []
    all_member_ids = [1, 2, 3, 4]

    # member rows
    for mid in all_member_ids:
        cat = live[mid]
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
                })

    # ensemble mean (member_id=0)
    for variable, col in VARIABLES.items():
        for lead in LEAD_HOURS:
            valid_pairs = []
            for mid in all_member_ids:
                cat = live[mid]
                if cat is None:
                    continue
                obs_val = obs[col]
                if obs_val is None:
                    continue
                mean_delta = conds[mid].get((cat, col, lead))
                if mean_delta is not None:
                    valid_pairs.append((mid, obs_val + mean_delta))

            if not valid_pairs:
                mean = None
            elif weights:
                w_pairs = [(weights.get((mid, variable, lead)), v) for mid, v in valid_pairs]
                if any(wt is None for wt, _ in w_pairs):
                    mean = sum(v for _, v in valid_pairs) / len(valid_pairs)
                else:
                    total_w = sum(wt for wt, _ in w_pairs)
                    mean = sum(wt * v for wt, v in w_pairs) / total_w
            else:
                mean = sum(v for _, v in valid_pairs) / len(valid_pairs)
            spread = (
                statistics.pstdev([v for _, v in valid_pairs])
                if len(valid_pairs) > 1 else None
            )
            rows.append({
                "model_id": MODEL_ID,
                "model": MODEL_NAME,
                "member_id": 0,
                "issued_at": issued_at,
                "valid_at": obs["timestamp"] + lead * 3600,
                "lead_hours": lead,
                "variable": variable,
                "value": mean,
                "spread": spread,
            })

    return rows
