# pressure_tendency: forecast from the recent barometric pressure time series.
#
# regression members fit polynomials (OLS or WLS, degree 1 or 2) to recent pressure
# observations, then extrapolate to each lead time for the pressure forecast. for
# other variables, a transfer function (OLS slope from historical data) maps the
# polynomial's tendency rate at t=0 to expected variable deltas.
#
# the zambretti member classifies the 3h tendency into one of five categories and
# applies historical conditional mean deltas — a categorical, rules-based approach
# for contrast against the regression members.
#
# zambretti_text() is a separate display-only function for the dashboard.

import bisect
import math
import statistics
import time

import db
import fmt
from models._climo_weights import LEAD_HOURS, VARIABLES
from models._utils import _sector

MODEL_ID = 5
MODEL_NAME = "pressure_tendency"
NEEDS_CONN_IN = True
NEEDS_WEIGHTS = True
NEEDS_ALL_OBS = True

# 3h tendency thresholds in hPa
_RAPID = 1.6
_SLOW = 0.1

# simplified Zambretti: tendency category -> (letter, description)
_ZAMBRETTI_TABLE = {
    "rapid_rise": ("A", "Settled fine"),
    "slow_rise":  ("B", "Fine weather"),
    "steady":     ("F", "Fair, possible showers"),
    "slow_fall":  ("K", "Unsettled, rain likely"),
    "rapid_fall": ("N", "Stormy, much rain"),
}

# (member_id, name, poly_degree, window_hours, half_life_minutes or None)
_MEMBERS = [
    (2,  "linear_1h",      1, 1,  None),
    (3,  "linear_3h",      1, 3,  None),
    (4,  "linear_6h",      1, 6,  None),
    (5,  "linear_3h_hl45", 1, 3,  45),
    (6,  "quad_3h",        2, 3,  None),
    (7,  "quad_6h",        2, 6,  None),
    (8,  "quad_3h_hl20",   2, 3,  20),
    (9,  "quad_3h_hl45",   2, 3,  45),
    (10, "quad_6h_hl20",   2, 6,  20),
    (11, "quad_6h_hl45",   2, 6,  45),
]

_MIN_PTS = {1: 2, 2: 3}
_TENDENCY_WINDOW_SEC = 3 * 3600  # 3h
_TENDENCY_LOOKUP_SEC = 600       # ±10 min
_FUTURE_LOOKUP_SEC = 900         # ±15 min

def _find_nearest_ts(sorted_ts, target, max_delta=600):
    """Binary search for the nearest timestamp within max_delta seconds of target."""
    if not sorted_ts:
        return None
    idx = bisect.bisect_left(sorted_ts, target)
    best = None
    best_d = max_delta + 1
    for i in (idx - 1, idx):
        if 0 <= i < len(sorted_ts):
            d = abs(sorted_ts[i] - target)
            if d <= max_delta and d < best_d:
                best_d = d
                best = sorted_ts[i]
    return best

def _zambretti_category(delta_p):
    """Classify a 3h pressure change (hPa) into a tendency category."""
    if delta_p >= _RAPID:
        return "rapid_rise"
    if delta_p >= _SLOW:
        return "slow_rise"
    if delta_p <= -_RAPID:
        return "rapid_fall"
    if delta_p <= -_SLOW:
        return "slow_fall"
    return "steady"

def _exp_weights(t_vals, half_life_h):
    """Exponential decay weights for centered time values (hours, 0=now, negative=past)."""
    lam = math.log(2) / half_life_h
    return [math.exp(lam * t) for t in t_vals]

def _gauss_solve(A, b):
    """Solve A @ x = b via Gaussian elimination. Returns None if matrix is singular."""
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
    """
    Fit a polynomial of given degree to (t, y) pairs using weighted normal equations.
    t_vals must be centered (hours relative to issued_at). Returns [a0, a1, ...] or None.
    """
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
    """Evaluate polynomial at t using Horner's method."""
    result = 0.0
    for c in reversed(coefs):
        result = result * t + c
    return result

def _poly_tendency_rate(coefs):
    """Instantaneous tendency rate at t=0 (units/hour). Equals the a1 coefficient."""
    return coefs[1] if len(coefs) > 1 else 0.0

def _ols1(xs, ys):
    """1-predictor OLS. Returns (slope, intercept) or None."""
    n = len(xs)
    if n < 2:
        return None
    sx = sum(xs)
    sy = sum(ys)
    sxy = sum(x * y for x, y in zip(xs, ys))
    sxx = sum(x * x for x in xs)
    denom = n * sxx - sx * sx
    if abs(denom) < 1e-12:
        return None
    slope = (n * sxy - sx * sy) / denom
    intercept = (sy - slope * sx) / n
    return slope, intercept

def _build_transfer_fns(all_obs, issued_at):
    """
    Compute transfer functions mapping 3h tendency rate (hPa/h) to expected variable
    delta for each non-pressure variable and lead time. Uses all available history.

    note: transfer functions are trained on 3h window rates. regression members apply
    their polynomial derivative at t=0 as the predictor, which may differ from the 3h
    rate (especially for short windows or quadratic fits). this mismatch is expected
    and illustrates sensitivity to the choice of tendency estimator.

    returns {(col, lead_hours): (slope, intercept)}
    """
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
            ts_past = _find_nearest_ts(sorted_ts, ts - _TENDENCY_WINDOW_SEC, _TENDENCY_LOOKUP_SEC)
            if ts_past is None:
                continue
            p_past = by_ts[ts_past]["station_pressure"]
            if p_past is None:
                continue
            ts_fut = _find_nearest_ts(sorted_ts, ts + lead_sec, _FUTURE_LOOKUP_SEC)
            if ts_fut is None:
                continue
            rate = (p_now - p_past) / 3.0
            row_now = by_ts[ts]
            row_fut = by_ts[ts_fut]
            xs.append(rate)
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

def _build_zambretti_conditionals(all_obs, issued_at):
    """
    Compute historical conditional mean deltas for each (category, col, lead).
    Cells with fewer than 3 historical pairs are omitted (returned as absent).

    returns {(category, col, lead_hours): mean_delta}
    """
    by_ts = {row["timestamp"]: row for row in all_obs}
    sorted_ts = sorted(by_ts)
    all_cols = list(VARIABLES.values())
    accum = {}

    for lead in LEAD_HOURS:
        lead_sec = lead * 3600
        for ts in sorted_ts:
            p_now = by_ts[ts]["station_pressure"]
            if p_now is None:
                continue
            ts_past = _find_nearest_ts(sorted_ts, ts - _TENDENCY_WINDOW_SEC, _TENDENCY_LOOKUP_SEC)
            if ts_past is None:
                continue
            p_past = by_ts[ts_past]["station_pressure"]
            if p_past is None:
                continue
            cat = _zambretti_category(p_now - p_past)
            ts_fut = _find_nearest_ts(sorted_ts, ts + lead_sec, _FUTURE_LOOKUP_SEC)
            if ts_fut is None:
                continue
            row_now = by_ts[ts]
            row_fut = by_ts[ts_fut]
            for col in all_cols:
                v_now = row_now[col]
                v_fut = row_fut[col]
                if v_now is not None and v_fut is not None:
                    accum.setdefault((cat, col, lead), []).append(v_fut - v_now)

    return {k: sum(v) / len(v) for k, v in accum.items() if len(v) >= 3}

def zambretti_text(obs, conn_in, elevation_m: float = 0.0):
    """
    Compute the Zambretti weather description for dashboard display.
    Not scored — display only.

    Pressure is reduced to sea level before computing tendency when
    elevation_m > 0; otherwise falls back to station pressure.
    Tendency classification is the same either way since the altitude
    correction is nearly constant and cancels in the difference.

    returns dict with keys: category, rate_hpa_per_h, letter, description.
    """
    ts = obs["timestamp"]
    row_past = db.nearest_tempest_obs(
        conn_in, ts - _TENDENCY_WINDOW_SEC, window_sec=_TENDENCY_LOOKUP_SEC
    )
    if row_past is None or obs["station_pressure"] is None or row_past["station_pressure"] is None:
        return {
            "category": "unknown",
            "rate_hpa_per_h": None,
            "letter": "\u2014",
            "description": "Insufficient pressure history",
        }

    def _slp(sp, temp_c):
        if elevation_m > 0.0 and temp_c is not None:
            return fmt.to_slp(sp, temp_c, elevation_m)
        return sp

    p_now = _slp(obs["station_pressure"], obs["air_temp"])
    p_past = _slp(row_past["station_pressure"], row_past["air_temp"])
    delta_p = p_now - p_past
    cat = _zambretti_category(delta_p)
    letter, desc = _ZAMBRETTI_TABLE[cat]
    return {
        "category": cat,
        "rate_hpa_per_h": round(delta_p / 3.0, 2),
        "letter": letter,
        "description": desc,
    }

def run(obs, issued_at, *, conn_in, weights=None, all_obs=None):
    # fetch full observation history for transfer functions and zambretti conditionals
    if all_obs is None:
        all_obs = db.tempest_obs_in_range(conn_in, 0, issued_at)

    transfer_fns = _build_transfer_fns(all_obs, issued_at)
    zambretti_conds = _build_zambretti_conditionals(all_obs, issued_at)

    # --- zambretti member (id=1) ---
    zambretti_vals = {}
    row_past = db.nearest_tempest_obs(
        conn_in, issued_at - _TENDENCY_WINDOW_SEC, window_sec=_TENDENCY_LOOKUP_SEC
    )
    if (
        row_past is not None
        and obs["station_pressure"] is not None
        and row_past["station_pressure"] is not None
    ):
        delta_p = obs["station_pressure"] - row_past["station_pressure"]
        cat = _zambretti_category(delta_p)
        for variable, col in VARIABLES.items():
            obs_val = obs[col]
            for lead in LEAD_HOURS:
                mean_delta = zambretti_conds.get((cat, col, lead))
                if obs_val is not None and mean_delta is not None:
                    zambretti_vals[(variable, lead)] = obs_val + mean_delta
                else:
                    zambretti_vals[(variable, lead)] = None
    else:
        for variable in VARIABLES:
            for lead in LEAD_HOURS:
                zambretti_vals[(variable, lead)] = None

    # --- regression members (ids 2-11) ---
    member_vals = {}

    for mid, _, degree, window_h, hl_min in _MEMBERS:
        start_ts = issued_at - window_h * 3600
        window_obs = [r for r in all_obs if r["timestamp"] >= start_ts]
        p_pairs = [
            (r["timestamp"], r["station_pressure"])
            for r in window_obs
            if r["station_pressure"] is not None
        ]

        if len(p_pairs) < _MIN_PTS[degree]:
            for variable in VARIABLES:
                for lead in LEAD_HOURS:
                    member_vals[(mid, variable, lead)] = None
            continue

        t_vals = [(ts - issued_at) / 3600.0 for ts, _ in p_pairs]
        p_vals = [p for _, p in p_pairs]
        w = _exp_weights(t_vals, hl_min / 60.0) if hl_min is not None else None
        coefs = _poly_fit(t_vals, p_vals, degree, w)

        if coefs is None:
            for variable in VARIABLES:
                for lead in LEAD_HOURS:
                    member_vals[(mid, variable, lead)] = None
            continue

        tendency_rate = _poly_tendency_rate(coefs)

        for variable, col in VARIABLES.items():
            for lead in LEAD_HOURS:
                if col == "station_pressure":
                    val = _poly_eval(coefs, float(lead))
                else:
                    tf = transfer_fns.get((col, lead))
                    obs_val = obs[col]
                    if tf is not None and obs_val is not None:
                        slope, intercept = tf
                        val = obs_val + slope * tendency_rate + intercept
                    else:
                        val = None
                member_vals[(mid, variable, lead)] = val

    # --- emit member rows ---
    all_member_ids = [1] + [mid for mid, *_ in _MEMBERS]
    rows = []

    for mid in all_member_ids:
        for variable in VARIABLES:
            for lead in LEAD_HOURS:
                val = (
                    zambretti_vals.get((variable, lead))
                    if mid == 1
                    else member_vals.get((mid, variable, lead))
                )
                rows.append({
                    "model_id": MODEL_ID,
                    "model": MODEL_NAME,
                    "member_id": mid,
                    "issued_at": issued_at,
                    "valid_at": obs["timestamp"] + lead * 3600,
                    "lead_hours": lead,
                    "variable": variable,
                    "value": val,
                })

    # --- ensemble mean (member_id=0) ---
    for variable in VARIABLES:
        for lead in LEAD_HOURS:
            valid_pairs = []
            for mid in all_member_ids:
                v = (
                    zambretti_vals.get((variable, lead))
                    if mid == 1
                    else member_vals.get((mid, variable, lead))
                )
                if v is not None:
                    valid_pairs.append((mid, v))

            valid_at = obs["timestamp"] + lead * 3600
            if not valid_pairs:
                mean = None
            elif weights:
                w_pairs = [(weights.get((mid, variable, lead, _sector(valid_at)), None), v) for mid, v in valid_pairs]
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
