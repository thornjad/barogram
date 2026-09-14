# wind_veer_detector: classifies wind-direction change over a trailing 3h window into
# veering / backing / steady — the same classification surface_signs's wind_rotation
# signal uses — but without (or with a much lower) minimum wind-speed floor.
#
# Motivated by the 2026-09-12 dry-airmass intrusion: the wind veered roughly 120
# degrees (SW to NW) between 03:00 and 06:00 local, a clean 2+ hour lead on the
# airmass change that showed up before the dewpoint itself turned over. It happened
# entirely at wind speeds of 0.2-1.8 mph (0.1-0.8 m/s) — well under surface_signs's
# and synoptic_state_machine's shared 1.5 m/s floor for trusting wind direction.
# That floor meant neither existing model could have used this signal at all for
# this event. This model tests whether direction is still informative below it.
#
# members:
#   1  veer_nogate           veering/backing/steady classification with the
#                            wind-speed floor removed (only true zero-wind excluded)
#   2  veer_lowgate          same, floor lowered to 0.3 m/s — excludes dead calm
#                            but keeps light-breeze veers like the 2026-09-12 case
#   3  veer_gust_confirmed   veer_nogate's classification, but a veering/backing
#                            call is only trusted when a coincident gust/avg ratio
#                            uptick also occurs — confirmation to cut false
#                            positives from noisy near-calm direction readings

import statistics

import db
import models._confidence as _confidence
from models._climo_weights import LEAD_HOURS, VARIABLES
from models._utils import _sector
from models.surface_signs import _angular_diff, _find_nearest_ts, _obs_in_window

MODEL_ID = 20
MODEL_NAME = "wind_veer_detector"
NEEDS_CONN_IN = True
NEEDS_WEIGHTS = True
NEEDS_ALL_OBS = True
NEEDS_MATCH_HISTORY = True

_SIGNAL_WINDOW_SEC = 3 * 3600  # 3h lookback, matches surface_signs's wind_rotation
_FUTURE_LOOKUP_SEC = 900       # +/- 15 min
_MIN_SAMPLES = 3
_VEER_THRESHOLD_DEG = 15.0
_LOWGATE_MIN_WIND_MS = 0.3
_GUST_MIN_WIND_MS = 0.3
_GUST_RATIO_THRESHOLD = 1.8  # mean gust/avg ratio above this counts as confirming

_MEMBERS = [
    (1, "veer_nogate"),
    (2, "veer_lowgate"),
    (3, "veer_gust_confirmed"),
]
_ALL_MEMBER_IDS = [mid for mid, _ in _MEMBERS]


def _rotation_category(window_obs, min_wind_ms):
    """Same net-direction-change classification as surface_signs._wind_rotation_category,
    but with a caller-supplied minimum wind speed instead of a fixed 1.5 m/s floor."""
    valid = [
        r for r in window_obs
        if r["wind_direction"] is not None
        and r["wind_avg"] is not None
        and r["wind_avg"] > min_wind_ms
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


def _gust_confirms(window_obs):
    valid = [
        r for r in window_obs
        if r["wind_gust"] is not None
        and r["wind_avg"] is not None
        and r["wind_avg"] > _GUST_MIN_WIND_MS
    ]
    if not valid:
        return False
    avg_gust = sum(r["wind_gust"] for r in valid) / len(valid)
    avg_wind = sum(r["wind_avg"] for r in valid) / len(valid)
    return (avg_gust / avg_wind) > _GUST_RATIO_THRESHOLD


def _category(mid, window_obs):
    if mid == 1:
        return _rotation_category(window_obs, 0.0)
    if mid == 2:
        return _rotation_category(window_obs, _LOWGATE_MIN_WIND_MS)
    # mid == 3: nogate classification, but a veer/back call needs gust confirmation
    cat = _rotation_category(window_obs, 0.0)
    if cat in ("veering", "backing") and not _gust_confirms(window_obs):
        return None
    return cat


def _build_conditionals(all_obs):
    """Per-member conditional mean delta, learned from history: {(cat, col, lead): delta}."""
    by_ts = {row["timestamp"]: row for row in all_obs}
    sorted_ts = sorted(by_ts)
    all_cols = list(VARIABLES.values())
    accum = {mid: {} for mid, _ in _MEMBERS}

    for ts in sorted_ts:
        window = _obs_in_window(sorted_ts, by_ts, ts - _SIGNAL_WINDOW_SEC, ts)
        row_now = by_ts[ts]
        for mid, _ in _MEMBERS:
            cat = _category(mid, window)
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


def run(obs, issued_at, *, conn_in, weights=None, all_obs=None,
        member_history=None, default_matches=None):
    if all_obs is None:
        all_obs = db.tempest_obs_in_range(conn_in, 0, issued_at)

    conds = _build_conditionals(all_obs)

    by_ts = {row["timestamp"]: row for row in all_obs}
    sorted_ts = sorted(by_ts)
    window_obs = _obs_in_window(
        sorted_ts, by_ts, obs["timestamp"] - _SIGNAL_WINDOW_SEC, obs["timestamp"]
    )
    live_cat = {mid: _category(mid, window_obs) for mid, _ in _MEMBERS}

    cell_confidences_by_var_lead = {
        (variable, lead): _confidence.member_confidences(
            member_history, default_matches, _ALL_MEMBER_IDS, variable, lead
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

    return rows
