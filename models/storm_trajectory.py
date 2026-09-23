# storm_trajectory: reads actual lightning distance (migration 004's
# lightning_avg_distance, full history) instead of just count, to classify an
# active convective event as approaching/overhead/departing/near-miss and to
# learn distance- and rate-conditioned deltas around it. Same bucket-
# history/average-future-delta/look-it-up-live pattern as frontal_trigger and
# regime_stability.
#
# lightning_avg_distance and lightning_strike_last_distance are Tempest API
# fields in km, not miles -- distance thresholds below are km, converted from
# the mile figures in the 2026-09-18 "Synoptic Signal Ideas" brainstorm
# (storm_trajectory section).
#
# members:
#   1  lightning_accel      lightning_count rate over a 30min window vs the
#                           previous 30min, cross-checked against
#                           lightning_strike_count_last_3hr's own 3h delta
#                           where that station-derived column is populated
#   2  precip_onset_lag     state = (distance bucket, elapsed bucket) since
#                           the current lightning event started -- learns how
#                           temp/dewpoint/pressure evolve by elapsed time,
#                           conditioned on how close the event's first strike
#                           reading was
#   3  storm_state          approaching / overhead / departing / near_miss,
#                           from pressure trend + lightning trend + distance
#                           trend + precip activity + wind veer, jointly
#   4  precip_pressure_state  narrower, faster cousin of 3: while actively
#                           raining, is pressure still falling (intensifying)
#                           or already recovering (past_peak)?
#   5  dry_lightning_flag   single "dry_lightning_risk" state: high lightning
#                           count at a large average distance, flat
#                           precip_accum_day, low relative_humidity
#   6  self_correction      standard self-correction member (models/_self_correction.py)
#                           -- member_id=0 minus this model's own learned bias

import bisect
import statistics

import db
import models._confidence as _confidence
import models._self_correction as _self_correction
from models._climo_weights import LEAD_HOURS, VARIABLES
from models._utils import _sector
from models.pressure_tendency import _zambretti_category
from models.surface_signs import _find_nearest_ts, _obs_in_window
from models.wind_veer_detector import _rotation_category

MODEL_ID = 26
MODEL_NAME = "storm_trajectory"
NEEDS_CONN_IN = True
NEEDS_CONN_OUT = True
NEEDS_WEIGHTS = True
NEEDS_ALL_OBS = True
NEEDS_MATCH_HISTORY = True

_SELF_CORRECTION_MEMBER = 6

_SHORT_WINDOW_SEC = 30 * 60      # member 1's rate window
_FAST_WINDOW_SEC = 3600          # member 4's precip/pressure window
_TREND_WINDOW_SEC = 3 * 3600     # member 1 cross-check + member 3's pressure/wind/distance window
_QUIET_GAP_SEC = 3600            # gap of this long with no lightning ends an "event"
_LOOKUP_SEC = 600
_FUTURE_LOOKUP_SEC = 900
_MIN_SAMPLES = 3

_NEAR_KM = 8.0    # ~5mi
_FAR_KM = 24.0    # ~15mi -- spec's near-miss floor ("never drops below ~15-20mi")
_DISTANCE_TREND_KM = 2.0  # minimum change to call distance shrinking/growing rather than steady

_GUST_RATIO = 1.4
_GUST_MIN_WIND_MS = 0.5

_DRY_LIGHTNING_WINDOW_SEC = 3 * 3600
_DRY_LIGHTNING_MIN_STRIKES = 5
_DRY_LIGHTNING_MIN_DIST_KM = 24.0   # ~15mi, "large average distance"
_DRY_LIGHTNING_MAX_RH = 40.0        # percent
_DRY_LIGHTNING_FLAT_PRECIP_MM = 0.5  # precip_accum_day change over the window

_PRECIP_PRESSURE_STEADY_HPA = 0.3  # over the 1h fast window

_MEMBERS = [
    (1, "lightning_accel"),
    (2, "precip_onset_lag"),
    (3, "storm_state"),
    (4, "precip_pressure_state"),
    (5, "dry_lightning_flag"),
]
_ALL_MEMBER_IDS = [mid for mid, _ in _MEMBERS]


def _lightning_sum(window):
    vals = [r["lightning_count"] for r in window if r.get("lightning_count") is not None]
    return sum(vals) if vals else None


def _lightning_trend_cat(window_new, window_old, cross_now, cross_past):
    new_sum, old_sum = _lightning_sum(window_new), _lightning_sum(window_old)
    if new_sum is None and old_sum is None:
        return None
    new_sum, old_sum = new_sum or 0, old_sum or 0
    if new_sum == 0 and old_sum == 0:
        return None
    if new_sum > old_sum:
        primary = "increasing"
    elif new_sum < old_sum:
        primary = "decreasing"
    else:
        primary = "steady"
    if cross_now is not None and cross_past is not None:
        cross_delta = cross_now - cross_past
        cross_cat = "increasing" if cross_delta > 0 else "decreasing" if cross_delta < 0 else "steady"
        # disagreement between the fine 30min read and the coarse 3h rolling count --
        # neither is trustworthy alone, abstain to steady rather than pick a side
        if primary != "steady" and cross_cat != "steady" and cross_cat != primary:
            return "steady"
    return primary


def _distance_trend_cat(dist_now, dist_past):
    if dist_now is None or dist_past is None:
        return None
    delta = dist_now - dist_past
    if delta <= -_DISTANCE_TREND_KM:
        return "shrinking"
    if delta >= _DISTANCE_TREND_KM:
        return "growing"
    return "steady"


def _pressure_dir_cat(delta):
    if delta is None:
        return None
    cat = _zambretti_category(delta)
    if cat in ("rapid_rise", "slow_rise"):
        return "rising"
    if cat in ("rapid_fall", "slow_fall"):
        return "falling"
    return "steady"


def _distance_bucket(km):
    if km is None:
        return None
    if km <= _NEAR_KM:
        return "near"
    if km <= _FAR_KM:
        return "mid"
    return "far"


def _elapsed_bucket(elapsed_sec):
    if elapsed_sec <= 1800:
        return "0_30m"
    if elapsed_sec <= 5400:
        return "30_90m"
    return "90m_plus"


def _gusty(row):
    gust, avg = row.get("wind_gust"), row.get("wind_avg")
    if gust is None or avg is None or avg <= _GUST_MIN_WIND_MS:
        return False
    return (gust / avg) >= _GUST_RATIO


def _event_start(ts, sorted_ts, by_ts, lightning_count_now, dist_now):
    """Backward walk from ts through an unbroken run of lightning_count > 0,
    tolerating gaps up to _QUIET_GAP_SEC. Returns the run's start timestamp, the
    earliest available distance reading in it, and the closest (min) distance
    seen -- or None if no lightning is present right now."""
    if not lightning_count_now:
        return None
    idx = bisect.bisect_left(sorted_ts, ts)
    last_seen = ts
    start_ts = ts
    start_dist = dist_now
    min_dist = dist_now
    i = idx - 1
    while i >= 0:
        t = sorted_ts[i]
        if last_seen - t > _QUIET_GAP_SEC:
            break
        row = by_ts[t]
        if row.get("lightning_count"):
            start_ts = t
            last_seen = t
            d = row.get("lightning_avg_distance")
            if d is not None:
                start_dist = d
                min_dist = d if min_dist is None else min(min_dist, d)
        i -= 1
    return {"start_ts": start_ts, "start_dist": start_dist, "min_dist": min_dist}


def _storm_state(pressure_cat, lightning_trend, distance_trend, precip_active,
                  precip_was_active, gusty, veer_cat, event):
    if (pressure_cat == "falling" and lightning_trend == "increasing"
            and distance_trend == "shrinking" and not precip_active):
        return "approaching"
    if (precip_active and gusty and pressure_cat != "rising"
            and distance_trend in ("shrinking", "steady")):
        return "overhead"
    if (precip_was_active and not precip_active and pressure_cat == "rising"
            and veer_cat == "veering" and distance_trend == "growing"):
        return "departing"
    if (event is not None and event["min_dist"] is not None
            and event["min_dist"] >= _FAR_KM
            and pressure_cat == "steady" and veer_cat in (None, "steady")):
        return "near_miss"
    return None


def _precip_pressure_state(precip_now, delta_p_1h):
    if not precip_now or delta_p_1h is None:
        return None
    if delta_p_1h <= -_PRECIP_PRESSURE_STEADY_HPA:
        return "intensifying"
    if delta_p_1h >= _PRECIP_PRESSURE_STEADY_HPA:
        return "past_peak"
    return None


def _dry_lightning_state(window, row_now, row_past):
    count = _lightning_sum(window)
    if count is None or count < _DRY_LIGHTNING_MIN_STRIKES:
        return None
    dists = [r["lightning_avg_distance"] for r in window if r.get("lightning_avg_distance") is not None]
    if not dists or (sum(dists) / len(dists)) < _DRY_LIGHTNING_MIN_DIST_KM:
        return None
    rh = row_now.get("relative_humidity")
    if rh is None or rh > _DRY_LIGHTNING_MAX_RH:
        return None
    if row_past is None:
        return None
    accum_now, accum_past = row_now.get("precip_accum_day"), row_past.get("precip_accum_day")
    if accum_now is None or accum_past is None:
        return None
    if abs(accum_now - accum_past) > _DRY_LIGHTNING_FLAT_PRECIP_MM:
        return None
    return "dry_lightning_risk"


def _signals(ts, sorted_ts, by_ts, row_now):
    """Every intermediate signal every member needs, computed once per ts so
    history-building and the live call can't drift from each other."""
    row_past_trend_ts = _find_nearest_ts(sorted_ts, ts - _TREND_WINDOW_SEC, _LOOKUP_SEC)
    row_past_trend = by_ts.get(row_past_trend_ts) if row_past_trend_ts is not None else None
    row_past_fast_ts = _find_nearest_ts(sorted_ts, ts - _FAST_WINDOW_SEC, _LOOKUP_SEC)
    row_past_fast = by_ts.get(row_past_fast_ts) if row_past_fast_ts is not None else None

    window_new = _obs_in_window(sorted_ts, by_ts, ts - _SHORT_WINDOW_SEC, ts)
    window_old = _obs_in_window(sorted_ts, by_ts, ts - 2 * _SHORT_WINDOW_SEC, ts - _SHORT_WINDOW_SEC)
    window_trend = _obs_in_window(sorted_ts, by_ts, ts - _TREND_WINDOW_SEC, ts)
    window_recent_precip = _obs_in_window(sorted_ts, by_ts, ts - _SHORT_WINDOW_SEC, ts)
    window_earlier_precip = _obs_in_window(
        sorted_ts, by_ts, ts - 2 * _TREND_WINDOW_SEC, ts - _SHORT_WINDOW_SEC
    )
    window_dry = _obs_in_window(sorted_ts, by_ts, ts - _DRY_LIGHTNING_WINDOW_SEC, ts)

    cross_now = row_now.get("lightning_strike_count_last_3hr")
    cross_past = row_past_trend.get("lightning_strike_count_last_3hr") if row_past_trend else None
    lightning_trend = _lightning_trend_cat(window_new, window_old, cross_now, cross_past)

    dist_now = row_now.get("lightning_avg_distance")
    dist_past = row_past_trend.get("lightning_avg_distance") if row_past_trend else None
    distance_trend = _distance_trend_cat(dist_now, dist_past)

    p_now, p_past_trend = row_now.get("station_pressure"), row_past_trend.get("station_pressure") if row_past_trend else None
    pressure_cat = _pressure_dir_cat(p_now - p_past_trend) if p_now is not None and p_past_trend is not None else None

    p_past_fast = row_past_fast.get("station_pressure") if row_past_fast else None
    delta_p_1h = p_now - p_past_fast if p_now is not None and p_past_fast is not None else None

    precip_active = any((r.get("precip") or 0) > 0 for r in window_recent_precip)
    precip_was_active = any((r.get("precip") or 0) > 0 for r in window_earlier_precip)

    veer_cat = _rotation_category(window_trend, 0.0)
    gusty = _gusty(row_now)

    event = _event_start(ts, sorted_ts, by_ts, row_now.get("lightning_count"), dist_now)

    return {
        "lightning_trend": lightning_trend,
        "distance_trend": distance_trend,
        "pressure_cat": pressure_cat,
        "delta_p_1h": delta_p_1h,
        "precip_active": precip_active,
        "precip_was_active": precip_was_active,
        "veer_cat": veer_cat,
        "gusty": gusty,
        "event": event,
        "window_dry": window_dry,
        "row_past_trend": row_past_trend,
    }


def _member_states(sig, row_now):
    storm_state = _storm_state(
        sig["pressure_cat"], sig["lightning_trend"], sig["distance_trend"],
        sig["precip_active"], sig["precip_was_active"], sig["gusty"], sig["veer_cat"], sig["event"],
    )
    precip_pressure = _precip_pressure_state(row_now.get("precip"), sig["delta_p_1h"])
    dry_flag = _dry_lightning_state(sig["window_dry"], row_now, sig["row_past_trend"])

    event = sig["event"]
    if event is None:
        onset_state = None
    else:
        bucket = _distance_bucket(event["start_dist"])
        onset_state = None if bucket is None else (bucket, _elapsed_bucket(row_now["timestamp"] - event["start_ts"]))

    return {
        1: sig["lightning_trend"],
        2: onset_state,
        3: storm_state,
        4: precip_pressure,
        5: dry_flag,
    }


def _build_conditionals(all_obs):
    by_ts = {row["timestamp"]: row for row in all_obs}
    sorted_ts = sorted(by_ts)
    all_cols = list(VARIABLES.values())
    accum = {mid: {} for mid, _ in _MEMBERS}

    for ts in sorted_ts:
        row_now = by_ts[ts]
        sig = _signals(ts, sorted_ts, by_ts, row_now)
        states = _member_states(sig, row_now)

        ts_fut_by_lead = {
            lead: _find_nearest_ts(sorted_ts, ts + lead * 3600, _FUTURE_LOOKUP_SEC)
            for lead in LEAD_HOURS
        }
        for mid, state in states.items():
            if state is None:
                continue
            for lead in LEAD_HOURS:
                ts_fut = ts_fut_by_lead[lead]
                if ts_fut is None:
                    continue
                row_fut = by_ts[ts_fut]
                for col in all_cols:
                    v_now, v_fut = row_now[col], row_fut[col]
                    if v_now is None or v_fut is None:
                        continue
                    accum[mid].setdefault((state, col, lead), []).append(v_fut - v_now)

    return {
        mid: {k: sum(v) / len(v) for k, v in table.items() if len(v) >= _MIN_SAMPLES}
        for mid, table in accum.items()
    }


def run(obs, issued_at, *, conn_in, conn_out=None, weights=None, all_obs=None,
        member_history=None, default_matches=None):
    if all_obs is None:
        all_obs = db.tempest_obs_in_range(conn_in, 0, issued_at)

    conds = _build_conditionals(all_obs)

    by_ts = {row["timestamp"]: row for row in all_obs}
    sorted_ts = sorted(by_ts)
    obs_ts = obs["timestamp"]

    sig = _signals(obs_ts, sorted_ts, by_ts, obs)
    states = _member_states(sig, obs)

    rows = []

    def _member_value(mid, col, lead):
        state = states.get(mid)
        if state is None:
            return None
        obs_val = obs[col]
        if obs_val is None:
            return None
        delta = conds[mid].get((state, col, lead))
        return obs_val + delta if delta is not None else None

    cell_confidences_by_var_lead = {
        (variable, lead): _confidence.member_confidences(
            member_history, default_matches, _ALL_MEMBER_IDS + [_SELF_CORRECTION_MEMBER], variable, lead
        )
        for variable in VARIABLES
        for lead in LEAD_HOURS
    }

    for mid, _ in _MEMBERS:
        for variable, col in VARIABLES.items():
            for lead in LEAD_HOURS:
                rows.append({
                    "model_id": MODEL_ID,
                    "model": MODEL_NAME,
                    "member_id": mid,
                    "issued_at": issued_at,
                    "valid_at": obs_ts + lead * 3600,
                    "lead_hours": lead,
                    "variable": variable,
                    "value": _member_value(mid, col, lead),
                    "confidence": cell_confidences_by_var_lead[(variable, lead)].get(mid),
                })

    for variable, col in VARIABLES.items():
        for lead in LEAD_HOURS:
            valid_at = obs_ts + lead * 3600
            cell_confidences = cell_confidences_by_var_lead[(variable, lead)]
            valid_pairs = [
                (mid, v) for mid, _ in _MEMBERS
                if (v := _member_value(mid, col, lead)) is not None
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
