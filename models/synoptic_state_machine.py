# synoptic_state_machine: joint synoptic state forecast from combined atmospheric signals.
# classifies current conditions using the same four signals as surface_signs — wind
# rotation, dewpoint spread trend, solar cloud cover, convective state — but treats them
# as a single combined state tuple so signal interactions influence the learned deltas.
#
# members:
#   1  full-4               (wind_rot, dp_trend, cloud, conv) — 81 cells; abstains at night
#   2  no-cloud             (wind_rot, dp_trend, conv) — 27 cells; works at night
#   3  wind-moisture        (wind_rot, dp_trend) — 9 cells
#   4  moisture-convective  (dp_trend, conv) — 9 cells
#   5  coarse-4             coarsened binary × conv — 24 cells; more data per cell
#   6  full-4+ptend         (wind_rot, dp_trend, cloud, conv, p_tend) — 243 cells; abstains at night
#   7  no-cloud+ptend       (wind_rot, dp_trend, conv, p_tend) — 81 cells; works at night

import statistics
import time

import db
from models.surface_signs import (
    _FUTURE_LOOKUP_SEC,
    _LOOKUP_SEC,
    _SIGNAL_WINDOW_SEC,
    _build_solar_climo,
    _convective_category,
    _dp_trend_category,
    _find_nearest_ts,
    _obs_in_window,
    _solar_cloud_category,
    _wind_rotation_category,
)

MODEL_ID = 10
MODEL_NAME = "synoptic_state_machine"
NEEDS_CONN_IN = True
NEEDS_WEIGHTS = True
NEEDS_ALL_OBS = True

LEAD_HOURS = [6, 12, 18, 24]
VARIABLES = {
    "temperature": "air_temp",
    "dewpoint":    "dew_point",
    "pressure":    "station_pressure",
    "wind_speed":  "wind_avg",
}

_MIN_SAMPLES = 3
_ALL_MEMBER_IDS = [1, 2, 3, 4, 5, 6, 7]
_PTEND_THRESHOLD = 0.5


def _sector(ts: int) -> int:
    h = time.localtime(ts).tm_hour
    if h < 6:  return 0
    if h < 12: return 1
    if h < 18: return 2
    return 3


def _cw(cat: str) -> str:
    return "rotating" if cat in ("veering", "backing") else "steady"


def _cdp(cat: str) -> str:
    return "moistening" if cat == "narrowing" else "drying"


def _cc(cat: str) -> str:
    return "cloudy" if cat in ("partial_cloud", "heavy_cloud") else "clear"


def _pressure_tendency_cat(obs_now, obs_3h) -> str | None:
    if obs_now is None or obs_3h is None:
        return None
    p_now = obs_now["station_pressure"]
    p_3h  = obs_3h["station_pressure"]
    if p_now is None or p_3h is None:
        return None
    delta = p_now - p_3h
    if delta > _PTEND_THRESHOLD:   return "rising"
    if delta < -_PTEND_THRESHOLD:  return "falling"
    return "steady"


def _member_states(rot, dp, cloud, conv, p_tend) -> dict:
    """Build {member_id: state_tuple_or_None} from the five raw signal categories."""
    return {
        1: (rot, dp, cloud, conv) if None not in (rot, dp, cloud) else None,
        2: (rot, dp, conv)        if None not in (rot, dp) else None,
        3: (rot, dp)              if None not in (rot, dp) else None,
        4: (dp, conv)             if dp is not None else None,
        5: (_cw(rot), _cdp(dp), _cc(cloud), conv)
           if None not in (rot, dp, cloud) else None,
        6: (rot, dp, cloud, conv, p_tend) if None not in (rot, dp, cloud, p_tend) else None,
        7: (rot, dp, conv, p_tend)        if None not in (rot, dp, p_tend) else None,
    }


def _build_conditionals(all_obs: list, solar_climo: dict) -> tuple[dict, dict, list]:
    """Single-pass scan over all historical obs building conditional delta tables.

    Computes all 5 member state tuples simultaneously per timestamp to avoid
    redundant scans over the full history.

    Returns (conds, by_ts, sorted_ts) so the caller can reuse the index for
    the live signal window lookup without rebuilding it.
    """
    by_ts = {row["timestamp"]: row for row in all_obs}
    sorted_ts = sorted(by_ts)
    accum: dict = {}

    for ts in sorted_ts:
        window = _obs_in_window(sorted_ts, by_ts, ts - _SIGNAL_WINDOW_SEC, ts)
        ts_3h = _find_nearest_ts(sorted_ts, ts - _SIGNAL_WINDOW_SEC, _LOOKUP_SEC)
        ts_1h = _find_nearest_ts(sorted_ts, ts - 3600, _LOOKUP_SEC)
        obs_3h = by_ts[ts_3h] if ts_3h is not None else None
        obs_1h = by_ts[ts_1h] if ts_1h is not None else None

        rot    = _wind_rotation_category(window)
        dp     = _dp_trend_category(by_ts[ts], obs_3h)
        cloud  = _solar_cloud_category(by_ts[ts], solar_climo)
        conv   = _convective_category(window, obs_1h, by_ts[ts])
        p_tend = _pressure_tendency_cat(by_ts[ts], obs_3h)

        states = _member_states(rot, dp, cloud, conv, p_tend)

        for lead in LEAD_HOURS:
            ts_fut = _find_nearest_ts(sorted_ts, ts + lead * 3600, _FUTURE_LOOKUP_SEC)
            if ts_fut is None:
                continue
            row_fut = by_ts[ts_fut]
            for mid, state in states.items():
                if state is None:
                    continue
                for col in VARIABLES.values():
                    v_now = by_ts[ts][col]
                    v_fut = row_fut[col]
                    if v_now is not None and v_fut is not None:
                        accum.setdefault((mid, state, col, lead), []).append(v_fut - v_now)

    conds = {
        k: sum(v) / len(v)
        for k, v in accum.items()
        if len(v) >= _MIN_SAMPLES
    }
    return conds, by_ts, sorted_ts


def run(obs, issued_at: int, *, conn_in, weights=None, all_obs=None) -> list[dict]:
    if all_obs is None:
        all_obs = db.tempest_obs_in_range(conn_in, 0, issued_at)

    solar_climo = _build_solar_climo(all_obs)
    conds, by_ts, sorted_ts = _build_conditionals(all_obs, solar_climo)

    # live signal categories — use obs["timestamp"] so the signal window matches
    # the obs that will be anchored, not the (slightly later) issued_at
    obs_ts = obs["timestamp"]
    window_obs = _obs_in_window(sorted_ts, by_ts, obs_ts - _SIGNAL_WINDOW_SEC, obs_ts)
    obs_3h = db.nearest_tempest_obs(conn_in, obs_ts - _SIGNAL_WINDOW_SEC, window_sec=_LOOKUP_SEC)
    obs_1h = db.nearest_tempest_obs(conn_in, obs_ts - 3600, window_sec=_LOOKUP_SEC)

    rot    = _wind_rotation_category(window_obs)
    dp     = _dp_trend_category(obs, obs_3h)
    cloud  = _solar_cloud_category(obs, solar_climo)
    conv   = _convective_category(window_obs, obs_1h, obs)
    p_tend = _pressure_tendency_cat(obs, obs_3h)

    live_states = _member_states(rot, dp, cloud, conv, p_tend)

    rows = []

    # member rows
    for mid in _ALL_MEMBER_IDS:
        state = live_states[mid]
        for variable, col in VARIABLES.items():
            obs_val = obs[col]
            for lead in LEAD_HOURS:
                valid_at = obs_ts + lead * 3600
                if state is None or obs_val is None:
                    value = None
                else:
                    mean_delta = conds.get((mid, state, col, lead))
                    value = obs_val + mean_delta if mean_delta is not None else None
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

    # ensemble mean (member_id=0): sector-aware weighted mean + spread
    for variable, col in VARIABLES.items():
        obs_val = obs[col]
        for lead in LEAD_HOURS:
            valid_at = obs_ts + lead * 3600
            sector = _sector(valid_at)

            valid_pairs = []
            for mid in _ALL_MEMBER_IDS:
                state = live_states[mid]
                if state is None or obs_val is None:
                    continue
                mean_delta = conds.get((mid, state, col, lead))
                if mean_delta is not None:
                    valid_pairs.append((mid, obs_val + mean_delta))

            if not valid_pairs:
                mean = None
            elif weights:
                w_pairs = [
                    (weights.get((mid, variable, lead, sector), None), v)
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
            })

    return rows
