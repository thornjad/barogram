import datetime as _dt
import math
import statistics

import db

MODEL_ID = 13
MODEL_NAME = "full_state_analog"
NEEDS_CONN_IN = True
NEEDS_WEIGHTS = True

LEAD_HOURS = [6, 12, 18, 24]

VARIABLES = {
    "temperature": "air_temp",
    "dewpoint": "dew_point",
    "pressure": "station_pressure",
}

_ALL_FEATURES = [
    "air_temp", "dew_point", "station_pressure",
    "wind_avg", "wind_direction", "wind_gust",
    "solar_radiation", "uv_index",
    "precip_accum_day", "lightning_count",
]

_CIRCULAR = {"wind_direction"}

# wind direction sigma fixed at one compass quadrant — circular std dev doesn't
# map cleanly onto the z-score framework used for other features
_WIND_DIR_SIGMA = 90.0

# (member_id, name, K, feature_subset, dist_weighted, seasonal)
_MEMBERS = [
    (1, "full-k5",           5,  _ALL_FEATURES,                                      False, False),
    (2, "full-k10",          10, _ALL_FEATURES,                                      False, False),
    (3, "thermo-wind",       5,  ["air_temp", "dew_point", "station_pressure",
                                  "wind_avg", "wind_direction"],                      False, False),
    (4, "solar-thermo",      5,  ["air_temp", "dew_point", "solar_radiation",
                                  "uv_index"],                                        False, False),
    (5, "synoptic",          5,  ["station_pressure", "wind_avg", "wind_direction"],  False, False),
    (6, "precip-signal",     5,  ["dew_point", "precip_accum_day",
                                  "lightning_count"],                                 False, False),
    (7, "full-seasonal",     5,  _ALL_FEATURES,                                      False, True),
    (8, "full-dist-weighted",10, _ALL_FEATURES,                                      True,  False),
]
_ALL_MEMBER_IDS = [m[0] for m in _MEMBERS]


def _arc_delta(a: float, b: float) -> float:
    d = abs(a - b)
    return min(d, 360.0 - d)


def _norm_sigmas(candidates: list, features: list) -> dict[str, float | None]:
    sigmas: dict[str, float | None] = {}
    for col in features:
        if col in _CIRCULAR:
            sigmas[col] = _WIND_DIR_SIGMA
            continue
        vals = [r[col] for r in candidates if r[col] is not None]
        if len(vals) < 2:
            sigmas[col] = None
        else:
            sigma = statistics.pstdev(vals)
            sigmas[col] = sigma if sigma > 0 else None
    return sigmas


def _distance(
    obs_vec: dict,
    candidate,
    features: list,
    sigmas: dict[str, float | None],
) -> float | None:
    total = 0.0
    used = 0
    for col in features:
        sigma = sigmas.get(col)
        if sigma is None:
            continue
        o = obs_vec.get(col)
        c = candidate[col]
        if o is None or c is None:
            continue
        delta = _arc_delta(o, c) if col in _CIRCULAR else (o - c)
        z = delta / sigma
        total += z * z
        used += 1
    if used == 0:
        return None
    return math.sqrt(total)


def _month_diff(ts1: int, ts2: int) -> int:
    m1 = _dt.datetime.fromtimestamp(ts1).month
    m2 = _dt.datetime.fromtimestamp(ts2).month
    diff = abs(m1 - m2)
    return min(diff, 12 - diff)


def _select_analogs(cands_with_dist: list, k: int) -> list:
    valid = [(d, c) for d, c in cands_with_dist if d is not None]
    valid.sort(key=lambda x: x[0])
    return valid[:k]


def _mean_forecast(futures: list) -> float | None:
    valid = [v for v in futures if v is not None]
    if not valid:
        return None
    return sum(valid) / len(valid)


def _dist_weighted_forecast(dist_val_pairs: list) -> float | None:
    valid = [(d, v) for d, v in dist_val_pairs if v is not None]
    if not valid:
        return None
    exact = [v for d, v in valid if d == 0]
    if exact:
        return sum(exact) / len(exact)
    total_w = sum(1.0 / d for d, _ in valid)
    return sum((1.0 / d) * v for d, v in valid) / total_w


def run(obs, issued_at: int, *, conn_in, weights=None) -> list[dict]:
    candidates = db.full_analog_candidates(conn_in, obs["timestamp"])
    obs_vec = {col: obs.get(col) for col in _ALL_FEATURES}

    member_analogs: dict[int, list] = {}
    for mid, name, k, features, dist_weighted, seasonal in _MEMBERS:
        sigmas = _norm_sigmas(candidates, features)
        if seasonal:
            cands_with_dist = []
            for cand in candidates:
                d = _distance(obs_vec, cand, features, sigmas)
                if d is not None:
                    d *= 1.0 + 0.2 * _month_diff(obs["timestamp"], cand["timestamp"])
                cands_with_dist.append((d, cand))
        else:
            cands_with_dist = [
                (_distance(obs_vec, cand, features, sigmas), cand)
                for cand in candidates
            ]
        member_analogs[mid] = _select_analogs(cands_with_dist, k)

    needed_ts = {
        cand["timestamp"]
        for analogs in member_analogs.values()
        for _, cand in analogs
    }

    rows = []
    for lead in LEAD_HOURS:
        valid_at = obs["timestamp"] + lead * 3600

        future_cache = {
            cand_ts: db.nearest_tempest_obs(conn_in, cand_ts + lead * 3600, window_sec=1800)
            for cand_ts in needed_ts
        }

        member_vals: dict[int, dict[str, float | None]] = {}

        for mid, name, _k, _features, dist_weighted, _seasonal in _MEMBERS:
            analogs = member_analogs[mid]
            member_vals[mid] = {}

            for variable, col in VARIABLES.items():
                if dist_weighted:
                    pairs = [
                        (d, future_cache[cand["timestamp"]][col]
                         if future_cache[cand["timestamp"]] is not None else None)
                        for d, cand in analogs
                    ]
                    value = _dist_weighted_forecast(pairs)
                else:
                    futures = [
                        future_cache[cand["timestamp"]][col]
                        if future_cache[cand["timestamp"]] is not None else None
                        for _, cand in analogs
                    ]
                    value = _mean_forecast(futures)

                member_vals[mid][variable] = value
                rows.append({
                    "model_id": MODEL_ID,
                    "model": MODEL_NAME,
                    "member_id": mid,
                    "issued_at": issued_at,
                    "valid_at": valid_at,
                    "lead_hours": lead,
                    "variable": variable,
                    "value": value,
                })

        for variable in VARIABLES:
            valid_pairs = [
                (mid, member_vals[mid][variable])
                for mid in _ALL_MEMBER_IDS
                if member_vals[mid][variable] is not None
            ]
            if not valid_pairs:
                mean = None
            elif weights:
                w_pairs = [
                    (weights.get((mid, variable, lead), None), v)
                    for mid, v in valid_pairs
                ]
                if any(w is None for w, _ in w_pairs):
                    mean = sum(v for _, v in valid_pairs) / len(valid_pairs)
                else:
                    total_w = sum(w for w, _ in w_pairs)
                    mean = sum(w * v for w, v in w_pairs) / total_w
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
