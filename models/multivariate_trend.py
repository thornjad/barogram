import math
import statistics

import db
from models._climo_weights import LEAD_HOURS, VARIABLES
from models._utils import _sector

MODEL_ID = 14
MODEL_NAME = "multivariate_trend"
NEEDS_CONN_IN = True
NEEDS_ALL_OBS = True
NEEDS_WEIGHTS = True

# (member_id, name, degree, window_h, half_life_min, ridge_alpha)
_MEMBERS = [
    (1,  "linear-1h",    1, 1,   None, 0.0),
    (2,  "linear-3h",    1, 3,   None, 0.0),
    (3,  "linear-6h",    1, 6,   None, 0.0),
    (4,  "linear-12h",   1, 12,  None, 0.0),
    (5,  "wls-3h-hl20",  1, 3,   20,   0.0),
    (6,  "wls-6h-hl45",  1, 6,   45,   0.0),
    (7,  "wls-6h-hl120", 1, 6,   120,  0.0),
    (8,  "quad-3h",      2, 3,   None, 0.0),
    (9,  "quad-6h",      2, 6,   None, 0.0),
    (10, "ridge-6h",     1, 6,   None, 5.0),
]
_ALL_MEMBER_IDS = [m[0] for m in _MEMBERS]
_MIN_PTS = {1: 2, 2: 3}


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


def _ridge_poly_fit(t_vals, y_vals, degree, weights=None, alpha=1.0):
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
    for i in range(1, d):
        A[i][i] += alpha
    return _gauss_solve(A, b_vec)


def _poly_eval(coefs, t):
    result = 0.0
    for c in reversed(coefs):
        result = result * t + c
    return result


def _exp_weights(t_vals, half_life_h):
    lam = math.log(2) / half_life_h
    return [math.exp(lam * t) for t in t_vals]


def _precip_prob(window_obs, t_all, w_all, obs, lead):
    pairs = [
        (t_all[i], r["precip_accum_day"], (w_all[i] if w_all is not None else 1.0))
        for i, r in enumerate(window_obs)
        if r["precip_accum_day"] is not None
    ]
    rate = 0.0
    if len(pairs) >= 2:
        t_f, y_f, w_f = zip(*pairs)
        coefs = _poly_fit(list(t_f), list(y_f), 1, list(w_f))
        if coefs:
            rate = coefs[1]

    temp = obs.get("air_temp")
    dp = obs.get("dew_point")
    dp_dep = (temp - dp) if (temp is not None and dp is not None) else None

    if rate > 0.2:
        return min(0.95, rate / (rate + 0.3)) * math.exp(-lead / 12.0)
    if dp_dep is not None:
        return max(0.0, 0.35 - dp_dep * 0.025) if dp_dep < 14.0 else 0.0
    return None


def run(obs, issued_at, *, conn_in, weights=None, all_obs=None):
    if all_obs is None:
        all_obs = db.tempest_obs_in_range(conn_in, 0, issued_at)

    member_vals = {}

    for mid, _name, degree, window_h, hl_min, ridge_alpha in _MEMBERS:
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
            if ridge_alpha > 0.0:
                coefs = _ridge_poly_fit(list(t_f), list(y_f), degree, list(w_f), ridge_alpha)
            else:
                coefs = _poly_fit(list(t_f), list(y_f), degree, list(w_f))
            for lead in LEAD_HOURS:
                member_vals[(mid, variable, lead)] = (
                    _poly_eval(coefs, float(lead)) if coefs is not None else None
                )

        for lead in LEAD_HOURS:
            member_vals[(mid, "precip_prob", lead)] = _precip_prob(
                window_obs, t_all, w_all, obs, lead
            )

    all_variables = list(VARIABLES.keys()) + ["precip_prob"]
    rows = []

    for lead in LEAD_HOURS:
        valid_at = obs["timestamp"] + lead * 3600

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
                })

        for variable in all_variables:
            valid_pairs = [
                (mid, member_vals[(mid, variable, lead)])
                for mid in _ALL_MEMBER_IDS
                if member_vals.get((mid, variable, lead)) is not None
            ]
            if not valid_pairs:
                mean = None
            elif weights:
                w_pairs = [
                    (weights.get((mid, variable, lead, _sector(valid_at)), None), v)
                    for mid, v in valid_pairs
                ]
                if any(wt is None for wt, _ in w_pairs):
                    mean = sum(v for _, v in valid_pairs) / len(valid_pairs)
                else:
                    total_w = sum(wt for wt, _ in w_pairs)
                    mean = sum(wt * v for wt, v in w_pairs) / total_w
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
