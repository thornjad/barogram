# pressure_trend_cascade: fork of pressure_tendency that goes one step further —
# instead of feeding the instantaneous tendency rate into the temp/dewpoint transfer
# function (what pressure_tendency does), each member first extrapolates its own
# predicted pressure at the target lead, then feeds that *predicted total delta*
# into a transfer function trained on total deltas rather than 3h rates. Two-stage:
# predict pressure, then predict everything else off the model's own prediction.
#
# members:
#   1  linear_extrap    linear (degree 1) fit over 3h window, mean-reverted extrapolation
#   2  quad_extrap      quadratic (degree 2) fit over 6h window, mean-reverted extrapolation
#   3  damped_extrap    3h tendency rate decayed toward zero over the lead (OU-style rate decay,
#                       analytically integrated) rather than extrapolating the raw polynomial
#   4  fast_damped_extrap  same as damped_extrap but with a much shorter decay half-life
#                       (~2h vs ~4.6h) — member 3 was the best-scoring internal model on
#                       dewpoint during the 2026-09-12 dry-intrusion event but still lagged
#                       the actual crash; this member tests whether a faster-decaying rate
#                       tracks rapid sub-6h transitions better without overreacting to noise
#                       during slower, more typical drift
#
# reuses the polynomial-fit and mean-reversion machinery from pressure_tendency rather
# than re-deriving it — these are genuinely the same numerics, just fed differently.

import math
import statistics

import db
import models._confidence as _confidence
from models._climo_weights import LEAD_HOURS, VARIABLES
from models._utils import _sector
from models.pressure_tendency import (
    _apply_mean_reversion,
    _exp_weights,
    _find_nearest_ts,
    _ols1,
    _poly_eval,
    _poly_fit,
    _poly_tendency_rate,
)

MODEL_ID = 16
MODEL_NAME = "pressure_trend_cascade"
NEEDS_CONN_IN = True
NEEDS_WEIGHTS = True
NEEDS_ALL_OBS = True
NEEDS_MATCH_HISTORY = True

_REVERSION_LAMBDA = 0.10   # per hour, matches pressure_tendency's OU e-folding
_DAMP_LAMBDA = 0.15        # per hour; e-folding ~6.7h for the damped-rate member
_FAST_DAMP_LAMBDA = 0.35   # per hour; e-folding ~2.9h for the fast-damped-rate member
_FUTURE_LOOKUP_SEC = 900   # +-15 min

_MEMBERS = [
    (1, "linear_extrap"),
    (2, "quad_extrap"),
    (3, "damped_extrap"),
    (4, "fast_damped_extrap"),
]
_ALL_MEMBER_IDS = [mid for mid, _ in _MEMBERS]


def _build_delta_transfer_fns(all_obs):
    """Map (col, lead) -> (slope, intercept) regressing a variable's own delta over
    [t, t+lead] against pressure's delta over the same window. Unlike pressure_tendency's
    transfer functions (trained on a fixed 3h backward rate), this is trained on the total
    predicted delta a member actually produces, over the horizon it actually forecasts."""
    by_ts = {row["timestamp"]: row for row in all_obs}
    sorted_ts = sorted(by_ts)
    non_pressure = [(col, var) for var, col in VARIABLES.items() if col != "station_pressure"]
    result = {}

    for lead in LEAD_HOURS:
        lead_sec = lead * 3600
        xs = []
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
            row_now = by_ts[ts]
            row_fut = by_ts[ts_fut]
            xs.append(p_fut - p_now)
            for col, _ in non_pressure:
                v_now = row_now[col]
                v_fut = row_fut[col]
                delta = (v_fut - v_now) if (v_now is not None and v_fut is not None) else None
                ys[col].append(delta)

        for col, _ in non_pressure:
            pairs = [(x, y) for x, y in zip(xs, ys[col]) if y is not None]
            if len(pairs) >= 3:
                tf = _ols1([p[0] for p in pairs], [p[1] for p in pairs])
                if tf is not None:
                    result[(col, lead)] = tf

    return result


def run(obs, issued_at, *, conn_in, weights=None, all_obs=None, member_history=None,
        default_matches=None):
    if all_obs is None:
        all_obs = db.tempest_obs_in_range(conn_in, 0, issued_at)

    transfer_fns = _build_delta_transfer_fns(all_obs)

    p_hist = [r["station_pressure"] for r in all_obs if r["station_pressure"] is not None]
    p_mean = sum(p_hist) / len(p_hist) if p_hist else None
    p_now = obs["station_pressure"]

    # linear_extrap: 3h window, degree 1
    start_3h = issued_at - 3 * 3600
    win_3h = [r for r in all_obs if r["timestamp"] >= start_3h and r["station_pressure"] is not None]
    coefs_lin = None
    if len(win_3h) >= 2:
        t_vals = [(r["timestamp"] - issued_at) / 3600.0 for r in win_3h]
        p_vals = [r["station_pressure"] for r in win_3h]
        coefs_lin = _poly_fit(t_vals, p_vals, 1)

    # quad_extrap: 6h window, degree 2
    start_6h = issued_at - 6 * 3600
    win_6h = [r for r in all_obs if r["timestamp"] >= start_6h and r["station_pressure"] is not None]
    coefs_quad = None
    if len(win_6h) >= 3:
        t_vals = [(r["timestamp"] - issued_at) / 3600.0 for r in win_6h]
        p_vals = [r["station_pressure"] for r in win_6h]
        coefs_quad = _poly_fit(t_vals, p_vals, 2)

    rate0 = _poly_tendency_rate(coefs_lin) if coefs_lin is not None else None

    rows = []
    member_vals: dict[tuple[int, str, int], float | None] = {}

    for lead in LEAD_HOURS:
        valid_at = obs["timestamp"] + lead * 3600
        lead_f = float(lead)

        preds: dict[int, float | None] = {}
        if coefs_lin is not None and p_mean is not None and p_now is not None:
            raw = _poly_eval(coefs_lin, lead_f)
            preds[1] = _apply_mean_reversion(raw, p_mean, lead_f)
        else:
            preds[1] = None

        if coefs_quad is not None and p_mean is not None and p_now is not None:
            raw = _poly_eval(coefs_quad, lead_f)
            preds[2] = _apply_mean_reversion(raw, p_mean, lead_f)
        else:
            preds[2] = None

        if rate0 is not None and p_now is not None:
            # analytic integral of an exponentially decaying rate: total predicted
            # delta = rate0 * (1 - exp(-lambda*lead)) / lambda
            delta = rate0 * (1.0 - math.exp(-_DAMP_LAMBDA * lead_f)) / _DAMP_LAMBDA
            preds[3] = p_now + delta
            fast_delta = rate0 * (1.0 - math.exp(-_FAST_DAMP_LAMBDA * lead_f)) / _FAST_DAMP_LAMBDA
            preds[4] = p_now + fast_delta
        else:
            preds[3] = None
            preds[4] = None

        variable_confidences = {
            variable: _confidence.member_confidences(
                member_history, default_matches, _ALL_MEMBER_IDS, variable, lead
            )
            for variable in VARIABLES
        }

        for mid, _name in _MEMBERS:
            pred_pressure = preds[mid]
            member_vals[(mid, "pressure", lead)] = pred_pressure

            if pred_pressure is not None and p_now is not None:
                delta_p = pred_pressure - p_now
                for variable, col in VARIABLES.items():
                    if col == "station_pressure":
                        continue
                    tf = transfer_fns.get((col, lead))
                    obs_val = obs[col]
                    if tf is not None and obs_val is not None:
                        slope, intercept = tf
                        member_vals[(mid, variable, lead)] = obs_val + slope * delta_p + intercept
                    else:
                        member_vals[(mid, variable, lead)] = None
            else:
                for variable, col in VARIABLES.items():
                    if col != "station_pressure":
                        member_vals[(mid, variable, lead)] = None

            for variable in VARIABLES:
                rows.append({
                    "model_id": MODEL_ID,
                    "model": MODEL_NAME,
                    "member_id": mid,
                    "issued_at": issued_at,
                    "valid_at": valid_at,
                    "lead_hours": lead,
                    "variable": variable,
                    "value": member_vals[(mid, variable, lead)],
                    "confidence": variable_confidences[variable].get(mid),
                })

        # member_id=0: weighted mean + spread
        for variable in VARIABLES:
            cell_confidences = variable_confidences[variable]
            valid_pairs = [
                (mid, member_vals[(mid, variable, lead)])
                for mid in _ALL_MEMBER_IDS
                if member_vals[(mid, variable, lead)] is not None
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
