# regime_stability: classifies current conditions into a calm/transitional/active
# meta-state from the rolled-up variance across every Tempest sensor over a trailing
# window, then looks up the learned conditional-mean delta for that state — the same
# bucket-history/average-future-delta/look-it-up-live pattern as wind_veer_detector
# and frontal_trigger.
#
# Hypothesis (see the 2026-09-22 "Synoptic Signal Ideas" brainstorm, regime_stability
# section): knowing the current regime is calm/boring/high-persistence-confidence
# beats any single physical signal at short range, regardless of which direction any
# one sensor happens to be moving right now. Deliberately starts as a single member,
# the same narrow-then-expand discipline as everything else in that brainstorm — if
# it earns weight, it grows a family; if it doesn't, it's a cheap experiment to have
# run.
#
# members:
#   1  regime_meta_state   volatility index (per-column trailing-window stdev,
#                          normalized against that column's own long-run stdev,
#                          averaged across every available raw sensor column)
#                          bucketed into calm / transitional / active
#   2  self_correction     standard self-correction member (models/_self_correction.py)
#                          -- member_id=0 minus this model's own learned bias

import statistics

import db
import models._confidence as _confidence
import models._self_correction as _self_correction
import models._similarity as _similarity
from models._climo_weights import LEAD_HOURS, VARIABLES
from models._utils import _sector
from models.surface_signs import _find_nearest_ts, _obs_in_window

MODEL_ID = 24
MODEL_NAME = "regime_stability"
NEEDS_CONN_IN = True
NEEDS_CONN_OUT = True
NEEDS_WEIGHTS = True
NEEDS_ALL_OBS = True
NEEDS_MATCH_HISTORY = True

_SELF_CORRECTION_MEMBER = 2

_SIGNAL_WINDOW_SEC = 3 * 3600  # 3h lookback, matches the confidence fingerprint's own trend window
_FUTURE_LOOKUP_SEC = 900
_MIN_SAMPLES = 3
_MIN_WINDOW_OBS = 2  # need at least 2 points in the trailing window to compute a local stdev

# every raw sensor column present in the shared all_obs row shape db.py's
# tempest_obs_in_range returns -- wind_lull and raw precip aren't selected by
# that query (only precip_accum_day is), so they're not available here the way
# they are in models/_confidence.py's own separate snapshot queries.
# relative_humidity was added to that query 2026-09-22 for
# synoptic_state_machine's rh-wind-pressure member, but stays out of this
# volatility list -- it's not a signal this model's hypothesis is about, adding
# columns here is a separate decision from whether the query carries them.
# Of what is available, wind_direction is excluded (circular,
# plain stdev doesn't apply -- see models/_similarity.py's own _CIRCULAR
# exclusion) and precip_accum_day is excluded (resets at local midnight, so a
# window straddling that reset reads as a fake variance spike -- the same
# reasoning that made the 2026-09-16 confidence-fingerprint decision prefer raw
# precip over precip_accum_day for its own trend signal; here there's no raw
# precip column to fall back to, so it's dropped rather than substituted)
_VOLATILITY_COLUMNS = [
    "air_temp", "dew_point", "station_pressure",
    "wind_avg", "wind_gust",
    "solar_radiation", "uv_index", "lightning_count",
]

_MEMBERS = [
    (1, "regime_meta_state"),
]
_ALL_MEMBER_IDS = [mid for mid, _ in _MEMBERS]

_CALM_MAX = 0.5     # volatility index below this: calm
_ACTIVE_MIN = 1.5   # volatility index above this: active; between the two: transitional


def _volatility_index(window_obs, global_sigmas):
    """Average, across every column with enough data, of this window's own local
    stdev divided by that column's long-run stdev. Roughly 1.0 means "as variable as
    usual right now"; below 1 is calmer than usual, above 1 more active than usual."""
    ratios = []
    for col in _VOLATILITY_COLUMNS:
        sigma = global_sigmas.get(col)
        if sigma is None:
            continue
        vals = [r[col] for r in window_obs if r[col] is not None]
        if len(vals) < _MIN_WINDOW_OBS:
            continue
        ratios.append(statistics.pstdev(vals) / sigma)
    if not ratios:
        return None
    return sum(ratios) / len(ratios)


def _regime_category(index):
    if index is None:
        return None
    if index < _CALM_MAX:
        return "calm"
    if index > _ACTIVE_MIN:
        return "active"
    return "transitional"


def _build_conditionals(all_obs, global_sigmas):
    """Conditional mean delta per (regime category, column, lead), learned from history."""
    by_ts = {row["timestamp"]: row for row in all_obs}
    sorted_ts = sorted(by_ts)
    all_cols = list(VARIABLES.values())
    accum = {}

    for ts in sorted_ts:
        window = _obs_in_window(sorted_ts, by_ts, ts - _SIGNAL_WINDOW_SEC, ts)
        cat = _regime_category(_volatility_index(window, global_sigmas))
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


def run(obs, issued_at, *, conn_in, conn_out=None, weights=None, all_obs=None,
        member_history=None, default_matches=None):
    if all_obs is None:
        all_obs = db.tempest_obs_in_range(conn_in, 0, issued_at)

    global_sigmas = _similarity.norm_sigmas(all_obs, _VOLATILITY_COLUMNS)
    conds = _build_conditionals(all_obs, global_sigmas)

    by_ts = {row["timestamp"]: row for row in all_obs}
    sorted_ts = sorted(by_ts)
    window_obs = _obs_in_window(
        sorted_ts, by_ts, obs["timestamp"] - _SIGNAL_WINDOW_SEC, obs["timestamp"]
    )
    live_cat = _regime_category(_volatility_index(window_obs, global_sigmas))

    cell_confidences_by_var_lead = {
        (variable, lead): _confidence.member_confidences(
            member_history, default_matches, _ALL_MEMBER_IDS + [_SELF_CORRECTION_MEMBER], variable, lead
        )
        for variable in VARIABLES
        for lead in LEAD_HOURS
    }

    rows = []
    for mid, _ in _MEMBERS:
        for variable, col in VARIABLES.items():
            obs_val = obs[col]
            for lead in LEAD_HOURS:
                if live_cat is None or obs_val is None:
                    value = None
                else:
                    mean_delta = conds.get((live_cat, col, lead))
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
            obs_val = obs[col]
            valid_pairs = []
            if live_cat is not None and obs_val is not None:
                mean_delta = conds.get((live_cat, col, lead))
                if mean_delta is not None:
                    valid_pairs.append((1, obs_val + mean_delta))

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
