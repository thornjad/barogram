import datetime
import statistics

import db
from models.surface_signs import (
    _FUTURE_LOOKUP_SEC,
    _LOOKUP_SEC,
    _SIGNAL_WINDOW_SEC,
    _build_solar_climo,
    _dp_trend_category,
    _find_nearest_ts,
    _obs_in_window,
    _solar_cloud_category,
    _wind_rotation_category,
)

MODEL_ID = 11
MODEL_NAME = "airmass_precip"
NEEDS_CONN_IN = True
NEEDS_WEIGHTS = True
NEEDS_ALL_OBS = True

LEAD_HOURS = [6, 12, 18, 24]

_TD_MOIST = 3.0              # °C spread below which air is near-saturated
_TD_DRY = 8.0                # °C spread above which air is clearly dry
_PTEND_THRESHOLD = 1.0       # hPa / 3h threshold for falling/rising
_PRECIP_1H_MM = 0.5          # mm/h threshold for "currently raining"
_PRESSURE_ANOM_THRESHOLD = 3.0  # hPa above/below climo mean for anomaly classification
_UV_CLIMO_MIN = 2.0          # min climo mean UV below which sky classification is unreliable
_MIN_SAMPLES = 3             # minimum historical pairs per (state, lead) cell

_MEMBER_NAMES = [
    (1, "dewpoint-moisture"),
    (2, "pressure-tendency"),
    (3, "cloud-cover"),
    (4, "wind-sector"),
    (5, "active-precip"),
    (6, "moisture+pressure"),
    (7, "wind-rotation"),
    (8, "cloud+moisture"),
    (9, "dry-airmass-gate"),
    (10, "moisture-trend"),
    (11, "diurnal-moisture"),
    (12, "pressure-anomaly"),
    (13, "uv-clear-sky"),
]
_ALL_MEMBER_IDS = [mid for mid, _ in _MEMBER_NAMES]


def _moisture_cat(obs):
    t = obs["air_temp"]
    td = obs["dew_point"]
    if t is None or td is None:
        return None
    spread = t - td
    if spread < _TD_MOIST:
        return "moist"
    if spread < _TD_DRY:
        return "moderate"
    return "dry"


def _ptend_cat(obs_now, obs_3h):
    if obs_now is None or obs_3h is None:
        return None
    p0 = obs_now["station_pressure"]
    p3 = obs_3h["station_pressure"]
    if p0 is None or p3 is None:
        return None
    d = p0 - p3
    if d < -_PTEND_THRESHOLD:
        return "falling"
    if d > _PTEND_THRESHOLD:
        return "rising"
    return "steady"


def _wind_sector_4(obs):
    wd = obs["wind_direction"]
    if wd is None:
        return None
    wa = obs["wind_avg"]
    if wa is not None and wa < 1.0:
        return None  # calm; direction unreliable
    if wd >= 315 or wd < 45:
        return "north"
    if wd < 135:
        return "east"
    if wd < 225:
        return "south"
    return "west"


def _active_precip_cat(obs_now, obs_1h):
    p_now = obs_now["precip_accum_day"] or 0.0
    p_1h = obs_1h["precip_accum_day"] or 0.0 if obs_1h is not None else 0.0
    return "raining" if max(0.0, p_now - p_1h) > _PRECIP_1H_MM else "dry"


def _diurnal_phase(ts):
    h = datetime.datetime.fromtimestamp(ts).hour
    if 6 <= h < 12:
        return "morning"
    if 12 <= h < 18:
        return "afternoon"
    if 18 <= h < 24:
        return "evening"
    return "night"


def _build_pressure_climo(all_obs):
    buckets = {}
    for row in all_obs:
        p = row["station_pressure"]
        if p is None:
            continue
        dt = datetime.datetime.fromtimestamp(row["timestamp"])
        buckets.setdefault((dt.month, dt.hour), []).append(p)
    return {k: sum(v) / len(v) for k, v in buckets.items() if len(v) >= _MIN_SAMPLES}


def _pressure_anomaly_cat(obs, pressure_climo):
    p = obs["station_pressure"]
    if p is None:
        return None
    dt = datetime.datetime.fromtimestamp(obs["timestamp"])
    climo = pressure_climo.get((dt.month, dt.hour))
    if climo is None:
        return None
    d = p - climo
    if d > _PRESSURE_ANOM_THRESHOLD:
        return "high"
    if d < -_PRESSURE_ANOM_THRESHOLD:
        return "low"
    return "normal"


def _build_uv_climo(all_obs):
    buckets = {}
    for row in all_obs:
        uv = row["uv_index"]
        if uv is None or uv <= 0:
            continue
        dt = datetime.datetime.fromtimestamp(row["timestamp"])
        buckets.setdefault((dt.month, dt.hour), []).append(uv)
    return {k: sum(v) / len(v) for k, v in buckets.items() if len(v) >= _MIN_SAMPLES}


def _uv_clear_category(obs, uv_climo):
    uv = obs["uv_index"]
    if uv is None or uv <= 0:
        return None
    dt = datetime.datetime.fromtimestamp(obs["timestamp"])
    climo_mean = uv_climo.get((dt.month, dt.hour))
    if climo_mean is None or climo_mean < _UV_CLIMO_MIN:
        return None
    deficit = 1.0 - uv / climo_mean
    if deficit > 0.7:
        return "heavy_cloud"
    if deficit > 0.3:
        return "partial_cloud"
    return "clear"


def _precip_occurred(obs_now, obs_fut):
    p0 = obs_now["precip_accum_day"]
    p1 = obs_fut["precip_accum_day"]
    if p0 is None or p1 is None:
        return None
    d0 = datetime.date.fromtimestamp(obs_now["timestamp"])
    d1 = datetime.date.fromtimestamp(obs_fut["timestamp"])
    if d0 != d1:
        return 1.0 if p1 > 0.1 else 0.0
    return 1.0 if max(0.0, p1 - p0) > 0.1 else 0.0


def _build_cond(signal_fn, sorted_ts, by_ts):
    accum = {}
    for ts in sorted_ts:
        cat = signal_fn(ts)
        if cat is None:
            continue
        for lead in LEAD_HOURS:
            ts_fut = _find_nearest_ts(sorted_ts, ts + lead * 3600, _FUTURE_LOOKUP_SEC)
            if ts_fut is None:
                continue
            occurred = _precip_occurred(by_ts[ts], by_ts[ts_fut])
            if occurred is None:
                continue
            accum.setdefault((cat, lead), []).append(occurred)
    return {k: sum(v) / len(v) for k, v in accum.items() if len(v) >= _MIN_SAMPLES}


def run(obs, issued_at: int, *, conn_in, weights=None, all_obs=None) -> list[dict]:
    if all_obs is None:
        all_obs = db.tempest_obs_in_range(conn_in, 0, issued_at)
    by_ts = {r["timestamp"]: r for r in all_obs}
    sorted_ts = sorted(by_ts)
    solar_climo = _build_solar_climo(all_obs)
    pressure_climo = _build_pressure_climo(all_obs)
    uv_climo = _build_uv_climo(all_obs)

    def sig1(ts):
        return _moisture_cat(by_ts[ts])

    def sig2(ts):
        ts3 = _find_nearest_ts(sorted_ts, ts - 3 * 3600, _LOOKUP_SEC)
        return _ptend_cat(by_ts[ts], by_ts[ts3]) if ts3 else None

    def sig3(ts):
        return _solar_cloud_category(by_ts[ts], solar_climo)

    def sig4(ts):
        return _wind_sector_4(by_ts[ts])

    def sig5(ts):
        ts1 = _find_nearest_ts(sorted_ts, ts - 3600, _LOOKUP_SEC)
        return _active_precip_cat(by_ts[ts], by_ts[ts1] if ts1 else None)

    def sig6(ts):
        m = sig1(ts)
        p = sig2(ts)
        return (m, p) if m is not None and p is not None else None

    def sig7(ts):
        window = _obs_in_window(sorted_ts, by_ts, ts - _SIGNAL_WINDOW_SEC, ts)
        return _wind_rotation_category(window)

    def sig8(ts):
        c = sig3(ts)
        m = sig1(ts)
        return (c, m) if c is not None and m is not None else None

    def sig9(ts):
        m = sig1(ts)
        p = sig2(ts)
        ts1 = _find_nearest_ts(sorted_ts, ts - 3600, _LOOKUP_SEC)
        a = _active_precip_cat(by_ts[ts], by_ts[ts1] if ts1 else None)
        return (m, p, a) if m is not None and p is not None else None

    def sig10(ts):
        ts3 = _find_nearest_ts(sorted_ts, ts - 3 * 3600, _LOOKUP_SEC)
        return _dp_trend_category(by_ts[ts], by_ts[ts3]) if ts3 else None

    def sig11(ts):
        m = sig1(ts)
        return (m, _diurnal_phase(ts)) if m is not None else None

    def sig12(ts):
        return _pressure_anomaly_cat(by_ts[ts], pressure_climo)

    def sig13(ts):
        return _uv_clear_category(by_ts[ts], uv_climo)

    conds = {
        mid: _build_cond(sig, sorted_ts, by_ts)
        for mid, sig in zip(
            _ALL_MEMBER_IDS,
            [sig1, sig2, sig3, sig4, sig5, sig6, sig7, sig8,
             sig9, sig10, sig11, sig12, sig13],
        )
    }

    obs_3h = db.nearest_tempest_obs(conn_in, obs["timestamp"] - 3 * 3600, window_sec=_LOOKUP_SEC)
    obs_1h = db.nearest_tempest_obs(conn_in, obs["timestamp"] - 3600, window_sec=_LOOKUP_SEC)
    window_obs = _obs_in_window(
        sorted_ts, by_ts, obs["timestamp"] - _SIGNAL_WINDOW_SEC, obs["timestamp"]
    )
    m_live = _moisture_cat(obs)
    p_live = _ptend_cat(obs, obs_3h)
    cloud_live = _solar_cloud_category(obs, solar_climo)
    dp_trend_live = _dp_trend_category(obs, obs_3h)
    p_anom_live = _pressure_anomaly_cat(obs, pressure_climo)
    uv_live = _uv_clear_category(obs, uv_climo)

    live = {
        1: m_live,
        2: p_live,
        3: cloud_live,
        4: _wind_sector_4(obs),
        5: _active_precip_cat(obs, obs_1h),
        6: (m_live, p_live) if m_live is not None and p_live is not None else None,
        7: _wind_rotation_category(window_obs),
        8: (cloud_live, m_live) if cloud_live is not None and m_live is not None else None,
    }
    live[9] = (m_live, p_live, live[5]) if m_live is not None and p_live is not None else None
    live[10] = dp_trend_live
    live[11] = (m_live, _diurnal_phase(obs["timestamp"])) if m_live is not None else None
    live[12] = p_anom_live
    live[13] = uv_live

    rows = []
    for lead in LEAD_HOURS:
        valid_at = obs["timestamp"] + lead * 3600
        member_vals = {}
        for mid in _ALL_MEMBER_IDS:
            cat = live[mid]
            value = conds[mid].get((cat, lead)) if cat is not None else None
            member_vals[mid] = value
            rows.append({
                "model_id": MODEL_ID, "model": MODEL_NAME, "member_id": mid,
                "issued_at": issued_at, "valid_at": valid_at,
                "lead_hours": lead, "variable": "precip_prob", "value": value,
            })

        valid_vals = [(mid, v) for mid, v in member_vals.items() if v is not None]
        if not valid_vals:
            mean = None
        elif weights:
            wp = [(weights.get((mid, "precip_prob", lead), None), v) for mid, v in valid_vals]
            if any(w is None for w, _ in wp):
                mean = sum(v for _, v in valid_vals) / len(valid_vals)
            else:
                tw = sum(w for w, _ in wp)
                mean = sum(w * v for w, v in wp) / tw
        else:
            mean = sum(v for _, v in valid_vals) / len(valid_vals)

        spread = (
            statistics.pstdev([v for _, v in valid_vals])
            if len(valid_vals) > 1 else None
        )
        rows.append({
            "model_id": MODEL_ID, "model": MODEL_NAME, "member_id": 0,
            "issued_at": issued_at, "valid_at": valid_at,
            "lead_hours": lead, "variable": "precip_prob",
            "value": mean, "spread": spread,
        })

    return rows
