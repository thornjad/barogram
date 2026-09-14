# frontal_trigger: joint pressure-tendency + wind-veer trigger. Combines the two
# earliest precursors found in the 2026-09-12 dry-airmass intrusion (see
# wind_veer_detector and pressure_trend_cascade) into a single signal, rather than
# folding wind rotation into synoptic_state_machine's joint state space — its own
# 2026-08-20 finding is that wind rotation hurts as a *joint* dimension there, so
# this stays a separate model instead of adding more wind-joint members to that one.
#
# members:
#   1  ptend_veer_strict     joint (pressure-tendency category, wind-veer category)
#                            state, but only when BOTH are actively signaling
#                            (non-"steady") — high precision, fires rarely
#   2  ptend_veer_loose      same joint state, but fires when EITHER signal is
#                            actively signaling (the other may be steady or
#                            unavailable) — earlier and more frequent, lower
#                            precision; a direct comparison point against member 1
#   3  ptend_veer_weighted   continuous blend of the two signals' own (marginal,
#                            not joint) conditional-mean deltas, weighted by
#                            whether each is actively signaling — no hard
#                            state-tuple lookup, so it never abstains for a novel
#                            combination the way members 1-2 can

import statistics

import db
import models._confidence as _confidence
from models._climo_weights import LEAD_HOURS, VARIABLES
from models._utils import _sector
from models.pressure_tendency import _zambretti_category
from models.surface_signs import _find_nearest_ts, _obs_in_window
from models.wind_veer_detector import _rotation_category

MODEL_ID = 21
MODEL_NAME = "frontal_trigger"
NEEDS_CONN_IN = True
NEEDS_WEIGHTS = True
NEEDS_ALL_OBS = True
NEEDS_MATCH_HISTORY = True

_PTEND_WINDOW_SEC = 3 * 3600  # 3h, matches pressure_tendency's zambretti member
_VEER_WINDOW_SEC = 3 * 3600   # matches wind_veer_detector's veer_nogate member
_LOOKUP_SEC = 600
_FUTURE_LOOKUP_SEC = 900
_MIN_SAMPLES = 3

_MEMBERS = [
    (1, "ptend_veer_strict"),
    (2, "ptend_veer_loose"),
    (3, "ptend_veer_weighted"),
]
_ALL_MEMBER_IDS = [mid for mid, _ in _MEMBERS]


def _active(cat: str | None) -> bool:
    return cat is not None and cat != "steady"


def _ptend_cat(row_now, row_past):
    if row_now is None or row_past is None:
        return None
    p_now, p_past = row_now["station_pressure"], row_past["station_pressure"]
    if p_now is None or p_past is None:
        return None
    cat = _zambretti_category(p_now - p_past)
    # collapse the 5-bucket zambretti category to 3 for this model: only the
    # rise/fall/steady direction matters here, not rapid-vs-slow magnitude
    if cat in ("rapid_rise", "slow_rise"):
        return "rising"
    if cat in ("rapid_fall", "slow_fall"):
        return "falling"
    return "steady"


def _strict_state(ptend_cat, veer_cat):
    if ptend_cat is None or veer_cat is None:
        return None
    if not _active(ptend_cat) or not _active(veer_cat):
        return None
    return (ptend_cat, veer_cat)


def _loose_state(ptend_cat, veer_cat):
    if not _active(ptend_cat) and not _active(veer_cat):
        return None
    return (ptend_cat or "unknown", veer_cat or "unknown")


def _weight(cat):
    if cat is None:
        return 0.0
    return 1.0 if cat != "steady" else 0.3


def _build_conditionals(all_obs):
    """One pass over history: strict/loose joint tables, plus marginal (single-signal)
    tables for the weighted member."""
    by_ts = {row["timestamp"]: row for row in all_obs}
    sorted_ts = sorted(by_ts)
    all_cols = list(VARIABLES.values())

    strict_accum, loose_accum = {}, {}
    ptend_accum, veer_accum = {}, {}

    for ts in sorted_ts:
        row_now = by_ts[ts]
        ts_past = _find_nearest_ts(sorted_ts, ts - _PTEND_WINDOW_SEC, _LOOKUP_SEC)
        row_past = by_ts.get(ts_past) if ts_past is not None else None
        ptend_cat = _ptend_cat(row_now, row_past)

        window = _obs_in_window(sorted_ts, by_ts, ts - _VEER_WINDOW_SEC, ts)
        veer_cat = _rotation_category(window, 0.0)

        strict_state = _strict_state(ptend_cat, veer_cat)
        loose_state = _loose_state(ptend_cat, veer_cat)

        for lead in LEAD_HOURS:
            ts_fut = _find_nearest_ts(sorted_ts, ts + lead * 3600, _FUTURE_LOOKUP_SEC)
            if ts_fut is None:
                continue
            row_fut = by_ts[ts_fut]
            for col in all_cols:
                v_now = row_now[col]
                v_fut = row_fut[col]
                if v_now is None or v_fut is None:
                    continue
                delta = v_fut - v_now
                if strict_state is not None:
                    strict_accum.setdefault((strict_state, col, lead), []).append(delta)
                if loose_state is not None:
                    loose_accum.setdefault((loose_state, col, lead), []).append(delta)
                if ptend_cat is not None:
                    ptend_accum.setdefault((ptend_cat, col, lead), []).append(delta)
                if veer_cat is not None:
                    veer_accum.setdefault((veer_cat, col, lead), []).append(delta)

    def _finish(accum):
        return {k: sum(v) / len(v) for k, v in accum.items() if len(v) >= _MIN_SAMPLES}

    return _finish(strict_accum), _finish(loose_accum), _finish(ptend_accum), _finish(veer_accum)


def run(obs, issued_at, *, conn_in, weights=None, all_obs=None,
        member_history=None, default_matches=None):
    if all_obs is None:
        all_obs = db.tempest_obs_in_range(conn_in, 0, issued_at)

    strict_conds, loose_conds, ptend_conds, veer_conds = _build_conditionals(all_obs)

    by_ts = {row["timestamp"]: row for row in all_obs}
    sorted_ts = sorted(by_ts)
    obs_ts = obs["timestamp"]

    ts_past = _find_nearest_ts(sorted_ts, obs_ts - _PTEND_WINDOW_SEC, _LOOKUP_SEC)
    row_past = by_ts.get(ts_past) if ts_past is not None else None
    ptend_cat = _ptend_cat(obs, row_past)

    window = _obs_in_window(sorted_ts, by_ts, obs_ts - _VEER_WINDOW_SEC, obs_ts)
    veer_cat = _rotation_category(window, 0.0)

    strict_state = _strict_state(ptend_cat, veer_cat)
    loose_state = _loose_state(ptend_cat, veer_cat)
    wp, wv = _weight(ptend_cat), _weight(veer_cat)

    rows = []

    def _member_value(mid, variable, col, lead):
        obs_val = obs[col]
        if obs_val is None:
            return None
        if mid == 1:
            if strict_state is None:
                return None
            delta = strict_conds.get((strict_state, col, lead))
            return obs_val + delta if delta is not None else None
        if mid == 2:
            if loose_state is None:
                return None
            delta = loose_conds.get((loose_state, col, lead))
            return obs_val + delta if delta is not None else None
        # mid == 3: weighted marginal blend
        p_delta = ptend_conds.get((ptend_cat, col, lead)) if ptend_cat is not None else None
        v_delta = veer_conds.get((veer_cat, col, lead)) if veer_cat is not None else None
        w_p = wp if p_delta is not None else 0.0
        w_v = wv if v_delta is not None else 0.0
        if w_p + w_v <= 0:
            return None
        blended = (w_p * (p_delta or 0.0) + w_v * (v_delta or 0.0)) / (w_p + w_v)
        return obs_val + blended

    cell_confidences_by_var_lead = {
        (variable, lead): _confidence.member_confidences(
            member_history, default_matches, _ALL_MEMBER_IDS, variable, lead
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
                    "value": _member_value(mid, variable, col, lead),
                    "confidence": cell_confidences_by_var_lead[(variable, lead)].get(mid),
                })

    for variable, col in VARIABLES.items():
        for lead in LEAD_HOURS:
            valid_at = obs_ts + lead * 3600
            cell_confidences = cell_confidences_by_var_lead[(variable, lead)]
            valid_pairs = [
                (mid, v) for mid, _ in _MEMBERS
                if (v := _member_value(mid, variable, col, lead)) is not None
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

    return rows
