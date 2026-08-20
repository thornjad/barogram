# synoptic_state_machine: joint synoptic state forecast from combined atmospheric signals.
# classifies current conditions using the same four signals as surface_signs — wind
# rotation, dewpoint spread trend, solar cloud cover, convective state — but treats them
# as a single combined state tuple so signal interactions influence the learned deltas.
#
# members 1-15 (original seven plus riffs on member 4, see 20260818 finding that wind
# rotation hurts as a joint dimension):
#   1  full-4               (wind_rot, dp_trend, cloud, conv) — 81 cells; abstains at night
#   2  no-cloud             (wind_rot, dp_trend, conv) — 27 cells; works at night
#   3  wind-moisture        (wind_rot, dp_trend) — 9 cells
#   4  moisture-convective  (dp_trend, conv) — 9 cells
#   5  coarse-4             coarsened binary × conv — 24 cells; more data per cell
#   6  full-4+ptend         (wind_rot, dp_trend, cloud, conv, p_tend) — 243 cells; abstains at night
#   7  no-cloud+ptend       (wind_rot, dp_trend, conv, p_tend) — 81 cells; works at night
#   8  moisture-only        (dp_trend,) — 3 cells
#   9  convective-only      (conv,) — 3 cells; never abstains for missing signal
#  10  moisture-ptend       (dp_trend, p_tend) — 9 cells
#  11  moisture-conv-ptend  (dp_trend, conv, p_tend) — 27 cells
#  12  moisture-cloud       (dp_trend, cloud) — 9 cells; abstains at night
#  13  no-wind              (dp_trend, cloud, conv, p_tend) — 81 cells; drops wind_rot
#  14  wind-only            (wind_rot,) — 3 cells
#  15  convective-cloud     (conv, cloud) — 9 cells; abstains at night
#
# members 16-94: parametric expansion (2026-08-20) following moisture/convective/ptend
# being the productive signal core and wind_rot/cloud contributing little as joint
# dimensions. Four new families, each swept across window length and/or pairing.
# Exact (member_id, name) assignment lives in migrations/039_synoptic_state_machine_expansion.sql
# and must stay in the same generation order as _build_expansion_states below.
#
#   ptend sweep (16-63, 48 members): pressure tendency at windows
#     {1,2,4,5,6,12,18,24}h (3h is the original signal, untouched) x granularity
#     {3-category "std", 5-bucket "graded"} x pairing {+dp, +dp+conv, alone}.
#   gust (64-75, 12 members): wind_gust/wind_avg ratio at windows {1,3,6}h,
#     categorized gusty/breezy/smooth, x pairing {+dp, +conv, +ptend(3h), +dp+conv}.
#   temptrend (76-87, 12 members): raw air_temp trend (not dp spread) at windows
#     {1,3,6,12}h x pairing {+ptend(3h), +dp+ptend(3h), +conv}.
#   preciprate (88-93, 6 members): precip accumulation rate trend
#     (accelerating/steady/decelerating) at windows {30min,1h,3h} x pairing {+conv, +dp}.
#   moisture-convective-cloud (94, 1 member): dp_trend + conv + cloud, three-way.

import statistics

import db
from models._utils import _sector
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
}

_MIN_SAMPLES = 3
_ALL_MEMBER_IDS = list(range(1, 16)) + list(range(16, 95))
_PTEND_THRESHOLD = 0.5
_PTEND_STRONG_THRESHOLD = 1.5
_GUST_MIN_WIND_MS = 0.5
_GUST_BREEZY_RATIO = 1.5
_GUST_GUSTY_RATIO = 2.5
_TEMP_TREND_THRESHOLD = 1.0
_PRECIP_RATE_THRESHOLD = 0.2  # mm/h change between windows

_PTEND_WINDOWS = [1, 2, 4, 5, 6, 12, 18, 24]
_PTEND_GRANS = ["std", "graded"]
_PTEND_PAIRINGS = ["dp", "dpconv", "alone"]
_GUST_WINDOWS = [1, 3, 6]
_GUST_PAIRINGS = ["dp", "conv", "ptend", "dpconv"]
_TEMP_WINDOWS = [1, 3, 6, 12]
_TEMP_PAIRINGS = ["ptend", "dpptend", "conv"]
_PRECIP_WINDOWS = [0.5, 1, 3]
_PRECIP_PAIRINGS = ["conv", "dp"]
_CLOUD_CONV_ID = 94

# every historical/live point lookback the expansion families need, deduplicated so
# _build_conditionals and run() each do one _find_nearest_ts (or db lookup) per offset
_ALL_POINT_OFFSETS = sorted(
    set(_PTEND_WINDOWS) | {3} | set(_TEMP_WINDOWS)
    | set(_PRECIP_WINDOWS) | {2 * w for w in _PRECIP_WINDOWS}
)


def _cw(cat: str) -> str:
    return "rotating" if cat in ("veering", "backing") else "steady"


def _cdp(cat: str) -> str:
    return "moistening" if cat == "narrowing" else "drying"


def _cc(cat: str) -> str:
    return "cloudy" if cat in ("partial_cloud", "heavy_cloud") else "clear"


def _pressure_tendency_cat(obs_now, obs_prior) -> str | None:
    if obs_now is None or obs_prior is None:
        return None
    p_now = obs_now["station_pressure"]
    p_prior = obs_prior["station_pressure"]
    if p_now is None or p_prior is None:
        return None
    delta = p_now - p_prior
    if delta > _PTEND_THRESHOLD:   return "rising"
    if delta < -_PTEND_THRESHOLD:  return "falling"
    return "steady"


def _pressure_tendency_cat_graded(obs_now, obs_prior) -> str | None:
    """5-bucket version of _pressure_tendency_cat, splitting rising/falling by rate."""
    if obs_now is None or obs_prior is None:
        return None
    p_now = obs_now["station_pressure"]
    p_prior = obs_prior["station_pressure"]
    if p_now is None or p_prior is None:
        return None
    delta = p_now - p_prior
    if delta > _PTEND_STRONG_THRESHOLD:   return "strong_rising"
    if delta > _PTEND_THRESHOLD:          return "rising"
    if delta < -_PTEND_STRONG_THRESHOLD:  return "strong_falling"
    if delta < -_PTEND_THRESHOLD:         return "falling"
    return "steady"


def _gustiness_category(window_obs) -> str | None:
    """Categorize wind_gust/wind_avg ratio over the window — turbulence proxy,
    orthogonal to wind_rotation (direction). None below the wind-speed floor."""
    valid = [
        r for r in window_obs
        if r["wind_gust"] is not None
        and r["wind_avg"] is not None
        and r["wind_avg"] > _GUST_MIN_WIND_MS
    ]
    if not valid:
        return None
    avg_gust = sum(r["wind_gust"] for r in valid) / len(valid)
    avg_wind = sum(r["wind_avg"] for r in valid) / len(valid)
    ratio = avg_gust / avg_wind
    if ratio > _GUST_GUSTY_RATIO:   return "gusty"
    if ratio > _GUST_BREEZY_RATIO:  return "breezy"
    return "smooth"


def _temp_trend_category(obs_now, obs_prior) -> str | None:
    """Raw air_temp trend, distinct from the dp_trend spread signal — captures
    warm/cold air advection independent of moisture."""
    if obs_now is None or obs_prior is None:
        return None
    t_now = obs_now["air_temp"]
    t_prior = obs_prior["air_temp"]
    if t_now is None or t_prior is None:
        return None
    delta = t_now - t_prior
    if delta > _TEMP_TREND_THRESHOLD:   return "warming"
    if delta < -_TEMP_TREND_THRESHOLD:  return "cooling"
    return "steady"


def _precip_rate_trend_category(obs_now, obs_w, obs_2w, window_hours) -> str | None:
    """Compares precip rate over the last window to the window before it —
    convective intensity trajectory, not just the binary dry/precip/lightning state.
    precip_accum_day resets at midnight, so rates are clamped to >= 0."""
    if obs_now is None or obs_w is None or obs_2w is None:
        return None
    p_now = obs_now["precip_accum_day"]
    p_w   = obs_w["precip_accum_day"]
    p_2w  = obs_2w["precip_accum_day"]
    if p_now is None or p_w is None or p_2w is None:
        return None
    rate_now   = max(0.0, p_now - p_w) / window_hours
    rate_prior = max(0.0, p_w - p_2w) / window_hours
    delta = rate_now - rate_prior
    if delta > _PRECIP_RATE_THRESHOLD:   return "accelerating"
    if delta < -_PRECIP_RATE_THRESHOLD:  return "decelerating"
    return "steady"


def _member_states(rot, dp, cloud, conv, p_tend) -> dict:
    """Build {member_id: state_tuple_or_None} for the original 15 members from the
    five raw signal categories."""
    return {
        1: (rot, dp, cloud, conv) if None not in (rot, dp, cloud) else None,
        2: (rot, dp, conv)        if None not in (rot, dp) else None,
        3: (rot, dp)              if None not in (rot, dp) else None,
        4: (dp, conv)             if dp is not None else None,
        5: (_cw(rot), _cdp(dp), _cc(cloud), conv)
           if None not in (rot, dp, cloud) else None,
        6: (rot, dp, cloud, conv, p_tend) if None not in (rot, dp, cloud, p_tend) else None,
        7: (rot, dp, conv, p_tend)        if None not in (rot, dp, p_tend) else None,
        8: (dp,)                  if dp is not None else None,
        9: (conv,),
        10: (dp, p_tend)          if None not in (dp, p_tend) else None,
        11: (dp, conv, p_tend)    if None not in (dp, p_tend) else None,
        12: (dp, cloud)           if None not in (dp, cloud) else None,
        13: (dp, cloud, conv, p_tend) if None not in (dp, cloud, p_tend) else None,
        14: (rot,)                if rot is not None else None,
        15: (conv, cloud)         if cloud is not None else None,
    }


def _build_expansion_states(dp, conv, cloud, ptend_std, ptend_graded, gust, temp_trend, precip_rate) -> dict:
    """Build {member_id: state_tuple_or_None} for members 16-94. Generation order here
    must match migrations/039_synoptic_state_machine_expansion.sql exactly — both were
    produced from the same (window, granularity, pairing) loop nesting."""
    states = {}
    mid = 16

    for w in _PTEND_WINDOWS:
        for gran in _PTEND_GRANS:
            p_val = (ptend_graded if gran == "graded" else ptend_std).get(w)
            for pairing in _PTEND_PAIRINGS:
                if pairing == "dp":
                    state = (dp, p_val) if dp is not None and p_val is not None else None
                elif pairing == "dpconv":
                    state = (dp, conv, p_val) if dp is not None and p_val is not None else None
                else:  # alone
                    state = (p_val,) if p_val is not None else None
                states[mid] = state
                mid += 1

    p3 = ptend_std.get(3)
    for w in _GUST_WINDOWS:
        g_val = gust.get(w)
        for pairing in _GUST_PAIRINGS:
            if pairing == "dp":
                state = (dp, g_val) if dp is not None and g_val is not None else None
            elif pairing == "conv":
                state = (conv, g_val) if g_val is not None else None
            elif pairing == "ptend":
                state = (p3, g_val) if p3 is not None and g_val is not None else None
            else:  # dpconv
                state = (dp, conv, g_val) if dp is not None and g_val is not None else None
            states[mid] = state
            mid += 1

    for w in _TEMP_WINDOWS:
        t_val = temp_trend.get(w)
        for pairing in _TEMP_PAIRINGS:
            if pairing == "ptend":
                state = (t_val, p3) if t_val is not None and p3 is not None else None
            elif pairing == "dpptend":
                state = (dp, t_val, p3) if None not in (dp, t_val, p3) else None
            else:  # conv
                state = (t_val, conv) if t_val is not None else None
            states[mid] = state
            mid += 1

    for w in _PRECIP_WINDOWS:
        pr_val = precip_rate.get(w)
        for pairing in _PRECIP_PAIRINGS:
            if pairing == "conv":
                state = (conv, pr_val) if pr_val is not None else None
            else:  # dp
                state = (dp, pr_val) if dp is not None and pr_val is not None else None
            states[mid] = state
            mid += 1

    states[_CLOUD_CONV_ID] = (dp, conv, cloud) if dp is not None and cloud is not None else None
    assert mid == _CLOUD_CONV_ID, "expansion state generation order drifted from migration"
    return states


def _signals_at(ts, sorted_ts, by_ts, solar_climo):
    """Compute every raw signal category at a historical timestamp: the original four
    plus the point lookups and windowed slices the expansion families need."""
    row_now = by_ts[ts]
    window = _obs_in_window(sorted_ts, by_ts, ts - _SIGNAL_WINDOW_SEC, ts)
    obs_1h = by_ts.get(_find_nearest_ts(sorted_ts, ts - 3600, _LOOKUP_SEC))

    rot   = _wind_rotation_category(window)
    obs_3h = by_ts.get(_find_nearest_ts(sorted_ts, ts - _SIGNAL_WINDOW_SEC, _LOOKUP_SEC))
    dp    = _dp_trend_category(row_now, obs_3h)
    cloud = _solar_cloud_category(row_now, solar_climo)
    conv  = _convective_category(window, obs_1h, row_now)

    obs_at = {
        off: by_ts.get(_find_nearest_ts(sorted_ts, ts - off * 3600, _LOOKUP_SEC))
        for off in _ALL_POINT_OFFSETS
    }
    ptend_std    = {w: _pressure_tendency_cat(row_now, obs_at[w]) for w in obs_at}
    ptend_graded = {w: _pressure_tendency_cat_graded(row_now, obs_at[w]) for w in _PTEND_WINDOWS}
    temp_trend   = {w: _temp_trend_category(row_now, obs_at[w]) for w in _TEMP_WINDOWS}
    precip_rate  = {
        w: _precip_rate_trend_category(row_now, obs_at[w], obs_at[2 * w], w)
        for w in _PRECIP_WINDOWS
    }
    gust = {
        w: _gustiness_category(_obs_in_window(sorted_ts, by_ts, ts - w * 3600, ts))
        for w in _GUST_WINDOWS
    }

    states = _member_states(rot, dp, cloud, conv, ptend_std[3])
    states.update(_build_expansion_states(dp, conv, cloud, ptend_std, ptend_graded, gust, temp_trend, precip_rate))
    return states


def _build_conditionals(all_obs: list, solar_climo: dict) -> tuple[dict, dict, list]:
    """Single-pass scan over all historical obs building conditional delta tables.

    Computes all member state tuples simultaneously per timestamp to avoid redundant
    scans over the full history.

    Returns (conds, by_ts, sorted_ts) so the caller can reuse the index for
    the live signal window lookup without rebuilding it.
    """
    by_ts = {row["timestamp"]: row for row in all_obs}
    sorted_ts = sorted(by_ts)
    accum: dict = {}

    for ts in sorted_ts:
        states = _signals_at(ts, sorted_ts, by_ts, solar_climo)

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

    obs_at = {
        off: db.nearest_tempest_obs(conn_in, obs_ts - off * 3600, window_sec=_LOOKUP_SEC)
        for off in _ALL_POINT_OFFSETS
    }
    ptend_std    = {w: _pressure_tendency_cat(obs, obs_at[w]) for w in obs_at}
    ptend_graded = {w: _pressure_tendency_cat_graded(obs, obs_at[w]) for w in _PTEND_WINDOWS}
    temp_trend   = {w: _temp_trend_category(obs, obs_at[w]) for w in _TEMP_WINDOWS}
    precip_rate  = {
        w: _precip_rate_trend_category(obs, obs_at[w], obs_at[2 * w], w)
        for w in _PRECIP_WINDOWS
    }
    gust = {
        w: _gustiness_category(_obs_in_window(sorted_ts, by_ts, obs_ts - w * 3600, obs_ts))
        for w in _GUST_WINDOWS
    }

    live_states = _member_states(rot, dp, cloud, conv, ptend_std[3])
    live_states.update(_build_expansion_states(dp, conv, cloud, ptend_std, ptend_graded, gust, temp_trend, precip_rate))

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
                    (weights.get((mid, variable, lead, sector)), v)
                    for mid, v in valid_pairs
                ]
                weighted = [(w, v) for w, v in w_pairs if w is not None]
                if weighted:
                    total_w = sum(w for w, _ in weighted)
                    mean = sum(w * v for w, v in weighted) / total_w
                else:
                    mean = sum(v for _, v in valid_pairs) / len(valid_pairs)
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
