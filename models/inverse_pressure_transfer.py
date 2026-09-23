# inverse_pressure_transfer: runs the pressure-tendency transfer relationship
# backwards. Instead of using pressure to predict temperature/dewpoint (what
# pressure_tendency and pressure_trend_cascade do), this model reads other base
# models' predicted temperature/dewpoint for this run (via conn_out) and infers
# what pressure change would be consistent with that prediction, using a transfer
# function trained the same way but with the roles of predictor and target swapped.
#
# outputs pressure only — this model has nothing new to say about temperature or
# dewpoint, it consumes them.
#
# must run after any base models it reads from, in _MODELS order.
#
# members:
#   1  temp_only_inverse       infers pressure delta from the temperature-only
#                              inverse transfer function
#   2  dewpoint_only_inverse   infers pressure delta from the dewpoint-only
#                              inverse transfer function (moisture-led signal)
#   3  joint_inverse           average of the two single-variable inverses
#   4  self_correction         standard self-correction member (models/_self_correction.py)
#                              -- member_id=0 minus this model's own learned bias

import statistics

import db
import models._confidence as _confidence
import models._self_correction as _self_correction
from models._climo_weights import LEAD_HOURS, VARIABLES
from models._utils import _sector
from models.pressure_tendency import _find_nearest_ts, _ols1

MODEL_ID = 19
MODEL_NAME = "inverse_pressure_transfer"
NEEDS_CONN_IN = True
NEEDS_CONN_OUT = True
NEEDS_WEIGHTS = True
NEEDS_ALL_OBS = True
NEEDS_MATCH_HISTORY = True

_SELF_CORRECTION_MEMBER = 4

_FUTURE_LOOKUP_SEC = 900  # +-15 min

_MEMBERS = [
    (1, "temp_only_inverse"),
    (2, "dewpoint_only_inverse"),
    (3, "joint_inverse"),
]
_ALL_MEMBER_IDS = [mid for mid, _ in _MEMBERS]

_SOURCE_VARIABLES = ["temperature", "dewpoint"]


def _build_inverse_transfer_fns(all_obs):
    """Map (col, lead) -> (slope, intercept) regressing pressure's own delta over
    [t, t+lead] against a *non-pressure* variable's delta over the same window —
    the mirror image of pressure_trend_cascade's _build_delta_transfer_fns, with
    predictor and target swapped."""
    by_ts = {row["timestamp"]: row for row in all_obs}
    sorted_ts = sorted(by_ts)
    non_pressure = [(col, var) for var, col in VARIABLES.items() if col != "station_pressure"]
    result = {}

    for lead in LEAD_HOURS:
        lead_sec = lead * 3600
        xs = {col: [] for col, _ in non_pressure}
        ys = {col: [] for col, _ in non_pressure}

        for ts in sorted_ts:
            p_now = by_ts[ts]["station_pressure"]
            if p_now is None:
                continue
            ts_fut = _find_nearest_ts(sorted_ts, ts + lead_sec, _FUTURE_LOOKUP_SEC)
            if ts_fut is None:
                continue
            p_fut = by_ts[ts_fut]["station_pressure"]
            if p_fut is None:
                continue
            dp = p_fut - p_now
            row_now = by_ts[ts]
            row_fut = by_ts[ts_fut]
            for col, _ in non_pressure:
                v_now = row_now[col]
                v_fut = row_fut[col]
                if v_now is not None and v_fut is not None:
                    xs[col].append(v_fut - v_now)
                    ys[col].append(dp)

        for col, _ in non_pressure:
            pairs = list(zip(xs[col], ys[col]))
            if len(pairs) >= 3:
                tf = _ols1([p[0] for p in pairs], [p[1] for p in pairs])
                if tf is not None:
                    result[(col, lead)] = tf

    return result


def run(obs, issued_at, *, conn_in, conn_out, weights=None, all_obs=None,
        member_history=None, default_matches=None):
    if all_obs is None:
        all_obs = db.tempest_obs_in_range(conn_in, 0, issued_at)
    inv_fns = _build_inverse_transfer_fns(all_obs)

    source_rows = db.base_model_forecasts(conn_out, issued_at, _SOURCE_VARIABLES)
    source_rows = [r for r in source_rows if r["model_id"] != MODEL_ID]

    by_lead_var: dict[tuple[int, str], list[float]] = {}
    for r in source_rows:
        if r["value"] is None:
            continue
        by_lead_var.setdefault((r["lead_hours"], r["variable"]), []).append(r["value"])

    p_now = obs["station_pressure"]
    rows = []
    member_vals: dict[tuple[int, int], float | None] = {}

    for lead in LEAD_HOURS:
        valid_at = obs["timestamp"] + lead * 3600

        # consensus (mean) predicted delta per source variable, from other models
        temp_vals = by_lead_var.get((lead, "temperature"), [])
        dew_vals = by_lead_var.get((lead, "dewpoint"), [])
        temp_delta = (
            (sum(temp_vals) / len(temp_vals)) - obs["air_temp"]
            if temp_vals and obs["air_temp"] is not None else None
        )
        dew_delta = (
            (sum(dew_vals) / len(dew_vals)) - obs["dew_point"]
            if dew_vals and obs["dew_point"] is not None else None
        )

        def _infer(col, delta):
            tf = inv_fns.get((col, lead))
            if tf is None or delta is None or p_now is None:
                return None
            slope, intercept = tf
            return p_now + slope * delta + intercept

        pred_temp = _infer("air_temp", temp_delta)
        pred_dew = _infer("dew_point", dew_delta)
        pred_joint = None
        if pred_temp is not None and pred_dew is not None:
            pred_joint = (pred_temp + pred_dew) / 2.0
        elif pred_temp is not None:
            pred_joint = pred_temp
        elif pred_dew is not None:
            pred_joint = pred_dew

        cell_confidences = _confidence.member_confidences(
            member_history, default_matches, _ALL_MEMBER_IDS + [_SELF_CORRECTION_MEMBER], "pressure", lead
        )

        preds = {1: pred_temp, 2: pred_dew, 3: pred_joint}
        for mid, _name in _MEMBERS:
            member_vals[(mid, lead)] = preds[mid]
            rows.append({
                "model_id": MODEL_ID,
                "model": MODEL_NAME,
                "member_id": mid,
                "issued_at": issued_at,
                "valid_at": valid_at,
                "lead_hours": lead,
                "variable": "pressure",
                "value": preds[mid],
                "confidence": cell_confidences.get(mid),
            })

        valid_pairs = [
            (mid, member_vals[(mid, lead)])
            for mid in _ALL_MEMBER_IDS
            if member_vals[(mid, lead)] is not None
        ]
        if not valid_pairs:
            mean, group_confidence = None, None
        elif weights:
            member_weights = {
                mid: weights.get((mid, "pressure", lead, _sector(valid_at)))
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
            "variable": "pressure",
            "value": mean,
            "spread": spread,
            "confidence": group_confidence,
        })

        corrected = _self_correction.corrected_value(
            conn_out, MODEL_ID, "pressure", lead, mean, issued_at
        )
        rows.append({
            "model_id": MODEL_ID,
            "model": MODEL_NAME,
            "member_id": _SELF_CORRECTION_MEMBER,
            "issued_at": issued_at,
            "valid_at": valid_at,
            "lead_hours": lead,
            "variable": "pressure",
            "value": corrected,
            "confidence": cell_confidences.get(_SELF_CORRECTION_MEMBER),
        })

    return rows
