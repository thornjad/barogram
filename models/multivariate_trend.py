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

# (member_id, name, degree, window_h, half_life_min, ridge_alpha, max_lead_h)
# max_lead_h: skip evaluation for leads > this value. Every member gets one — no
# extrapolating further than the window it was fit on, degree or member id notwithstanding.
_MEMBERS = [
    (1,  "linear-1h",      1, 1,   None, 0.0,  6),
    (2,  "linear-3h",      1, 3,   None, 0.0,  6),
    (3,  "linear-6h",      1, 6,   None, 0.0,  12),
    (4,  "linear-12h",     1, 12,  None, 0.0,  12),
    (5,  "wls-3h-hl20",    1, 3,   20,   0.0,  6),
    (6,  "wls-6h-hl45",    1, 6,   45,   0.0,  12),
    (7,  "wls-6h-hl120",   1, 6,   120,  0.0,  12),
    (8,  "quad-3h",        2, 3,   None, 0.0,  6),
    (9,  "quad-6h",        2, 6,   None, 0.0,  6),
    (10, "ridge-6h",       1, 6,   None, 5.0,  12),
    # longer windows: fill the 18h/24h gap and sweep the window-vs-skill hypothesis
    (11, "linear-18h",     1, 18,  None, 0.0,  18),
    (12, "linear-24h",     1, 24,  None, 0.0,  24),
    (13, "linear-36h",     1, 36,  None, 0.0,  36),
    (14, "linear-48h",     1, 48,  None, 0.0,  48),
    (15, "wls-18h-hl240",  1, 18,  240,  0.0,  18),
    (16, "wls-24h-hl360",  1, 24,  360,  0.0,  24),
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



def run(obs, issued_at, *, conn_in, weights=None, all_obs=None):
    if all_obs is None:
        all_obs = db.tempest_obs_in_range(conn_in, 0, issued_at)

    member_vals = {}

    for mid, _name, degree, window_h, hl_min, ridge_alpha, max_lead_h in _MEMBERS:
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
                    (weights.get((mid, variable, lead, _sector(valid_at))), v)
                    for mid, v in valid_pairs
                ]
                weighted = [(wt, v) for wt, v in w_pairs if wt is not None]
                if weighted:
                    total_w = sum(wt for wt, _ in weighted)
                    mean = sum(wt * v for wt, v in weighted) / total_w
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
