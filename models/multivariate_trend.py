import math
import statistics

import db
import models._confidence as _confidence
from models._climo_weights import LEAD_HOURS, VARIABLES
from models._utils import _sector

MODEL_ID = 14
MODEL_NAME = "multivariate_trend"
NEEDS_CONN_IN = True
NEEDS_ALL_OBS = True
NEEDS_WEIGHTS = True
NEEDS_MATCH_HISTORY = True

# (member_id, name, degree, window_h, half_life_min, max_lead_h)
# max_lead_h: skip evaluation for leads > this value. Every member gets one — no
# extrapolating further than the window it was fit on, degree or member id notwithstanding.
#
# Members 3,4,6,7,8,9,10,11,12,13,14,15,16 retired 2026-09-18: consistently the
# worst performers in the whole barogram roster (cross-variable z-score analysis).
# All shared a structural mismatch rather than a data-maturity gap -- quad-3h/quad-6h
# fit a quadratic to a 3-7 point window (overshoots on extrapolation regardless of
# how much history accumulates), the rest (linear/WLS/ridge trend extrapolation at
# every window from 6h to 48h) lose to climatology-style members on temp/dewpoint
# because a straight line never learns a diurnal cycle no matter how long it's fit
# over. Historical forecast rows and the members-table registry entries are kept;
# only future generation stopped. Full writeup: thornlog message board
# "barogram-model-analysis".
_MEMBERS = [
    (1,  "linear-1h",      1, 1,   None, 6),
    (2,  "linear-3h",      1, 3,   None, 6),
    (5,  "wls-3h-hl20",    1, 3,   20,   6),
]
_ALL_MEMBER_IDS = [m[0] for m in _MEMBERS]
_MIN_PTS = {1: 2}


def _gauss_solve(A, b):
    n = len(b)
    M = [A[i][:] + [b[i]] for i in range(n)]
    for col in range(n):
        pivot = next((r for r in range(col, n) if abs(M[r][col]) > 1e-12), None)
        if pivot is None:
            return None
        M[col], M[pivot] = M[pivot], M[col]
        inv = 1.0 / M[col][col]
        for row in range(col + 1, n):
            f = M[row][col] * inv
            for j in range(col, n + 1):
                M[row][j] -= f * M[col][j]
    x = [0.0] * n
    for i in range(n - 1, -1, -1):
        x[i] = M[i][n]
        for j in range(i + 1, n):
            x[i] -= M[i][j] * x[j]
        x[i] /= M[i][i]
    return x


def _poly_fit(t_vals, y_vals, degree, weights=None):
    n = len(t_vals)
    d = degree + 1
    w = weights or [1.0] * n
    A = [[0.0] * d for _ in range(d)]
    b_vec = [0.0] * d
    for k in range(n):
        wk, tk, yk = w[k], t_vals[k], y_vals[k]
        tpow = [tk ** p for p in range(2 * d)]
        for i in range(d):
            b_vec[i] += wk * yk * tpow[i]
            for j in range(d):
                A[i][j] += wk * tpow[i + j]
    return _gauss_solve(A, b_vec)


def _poly_eval(coefs, t):
    result = 0.0
    for c in reversed(coefs):
        result = result * t + c
    return result


def _exp_weights(t_vals, half_life_h):
    lam = math.log(2) / half_life_h
    return [math.exp(lam * t) for t in t_vals]



def run(obs, issued_at, *, conn_in, weights=None, all_obs=None,
        member_history=None, default_matches=None):
    if all_obs is None:
        all_obs = db.tempest_obs_in_range(conn_in, 0, issued_at)

    member_vals = {}

    for mid, _name, degree, window_h, hl_min, max_lead_h in _MEMBERS:
        start_ts = issued_at - window_h * 3600
        window_obs = [r for r in all_obs if r["timestamp"] >= start_ts]
        t_all = [(r["timestamp"] - issued_at) / 3600.0 for r in window_obs]
        w_all = _exp_weights(t_all, hl_min / 60.0) if hl_min is not None else None

        for variable, col in VARIABLES.items():
            pairs = [
                (t_all[i], r[col], (w_all[i] if w_all is not None else 1.0))
                for i, r in enumerate(window_obs)
                if r[col] is not None
            ]
            if len(pairs) < _MIN_PTS[degree]:
                for lead in LEAD_HOURS:
                    member_vals[(mid, variable, lead)] = None
                continue
            t_f, y_f, w_f = zip(*pairs)
            coefs = _poly_fit(list(t_f), list(y_f), degree, list(w_f))
            for lead in LEAD_HOURS:
                if max_lead_h is not None and lead > max_lead_h:
                    member_vals[(mid, variable, lead)] = None
                else:
                    member_vals[(mid, variable, lead)] = (
                        _poly_eval(coefs, float(lead)) if coefs is not None else None
                    )

    all_variables = list(VARIABLES.keys())
    rows = []

    for lead in LEAD_HOURS:
        valid_at = obs["timestamp"] + lead * 3600

        # confidence per (variable, lead) cell, shared default fingerprint --
        # this model has no per-member analog selection of its own to reuse
        cell_confidences = {
            variable: _confidence.member_confidences(
                member_history, default_matches, _ALL_MEMBER_IDS, variable, lead
            )
            for variable in all_variables
        }

        for mid in _ALL_MEMBER_IDS:
            for variable in all_variables:
                rows.append({
                    "model_id": MODEL_ID,
                    "model": MODEL_NAME,
                    "member_id": mid,
                    "issued_at": issued_at,
                    "valid_at": valid_at,
                    "lead_hours": lead,
                    "variable": variable,
                    "value": member_vals.get((mid, variable, lead)),
                    "confidence": cell_confidences[variable].get(mid),
                })

        for variable in all_variables:
            valid_pairs = [
                (mid, member_vals[(mid, variable, lead)])
                for mid in _ALL_MEMBER_IDS
                if member_vals.get((mid, variable, lead)) is not None
            ]
            if not valid_pairs:
                mean, group_confidence = None, None
            elif weights:
                member_weights = {
                    mid: weights.get((mid, variable, lead, _sector(valid_at)))
                    for mid, _ in valid_pairs
                }
                confidences = {mid: cell_confidences[variable].get(mid) for mid, _ in valid_pairs}
                mean, group_confidence = _confidence.combine_pattern(valid_pairs, member_weights, confidences)
            else:
                mean = sum(v for _, v in valid_pairs) / len(valid_pairs)
                group_confidence = _confidence.average_confidence(
                    [cell_confidences[variable].get(mid) for mid, _ in valid_pairs]
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
