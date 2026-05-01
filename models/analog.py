# analog: finds K most similar historical days by weighted Euclidean distance
# in normalized feature space and uses their subsequent weather as a forecast.
# data-starved early on (only ~43 days at first write) but improves over time.
# when fewer candidates exist than K, uses however many are available.
# member_id=0: inverse-MAE weighted mean of members 1-8 when weights available.

import datetime
import math
import statistics

import db

MODEL_ID = 8
MODEL_NAME = "analog"
NEEDS_CONN_IN = True
NEEDS_WEIGHTS = True

LEAD_HOURS = [6, 12, 18, 24]

VARIABLES = {
    "temperature": "air_temp",
    "dewpoint": "dew_point",
    "pressure": "station_pressure",
}

# feature columns used for similarity — order matches feature weight lists
_FEATURES = ["air_temp", "dew_point", "station_pressure", "wind_avg"]

# (member_id, name, K, feature_weights)
# feature weights correspond to _FEATURES: [temp, dp, pressure, wind]
_MEMBERS = [
    (1, "k3",                3,  [1.0, 1.0, 1.0, 1.0]),
    (2, "k5",                5,  [1.0, 1.0, 1.0, 1.0]),
    (3, "k10",               10, [1.0, 1.0, 1.0, 1.0]),
    (4, "k20",               20, [1.0, 1.0, 1.0, 1.0]),
    (5, "k5-moisture",       5,  [2.0, 2.0, 1.0, 0.5]),
    (6, "k5-synoptic",       5,  [1.0, 0.5, 2.0, 1.5]),
    (7, "k10-dist-weighted", 10, [1.0, 1.0, 1.0, 1.0]),
    (8, "k5-seasonal",       5,  [1.0, 1.0, 1.0, 1.0]),
]
_ALL_MEMBER_IDS = [m[0] for m in _MEMBERS]


def _norm_sigmas(candidates: list) -> dict[str, float | None]:
    """Per-feature population std dev across candidates; None means skip the feature."""
    sigmas = {}
    for col in _FEATURES:
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
    weights: list[float],
    sigmas: dict[str, float | None],
) -> float | None:
    """Weighted Euclidean distance in sigma-normalized feature space."""
    total = 0.0
    used = 0
    for i, col in enumerate(_FEATURES):
        sigma = sigmas[col]
        if sigma is None:
            continue
        o = obs_vec[col]
        c = candidate[col]
        if o is None or c is None:
            continue
        z = (o - c) / sigma
        total += weights[i] * z * z
        used += 1
    if used == 0:
        return None
    return math.sqrt(total)


def _month_diff(ts1: int, ts2: int) -> int:
    """Circular calendar-month distance between two timestamps (0–6)."""
    m1 = datetime.datetime.fromtimestamp(ts1).month
    m2 = datetime.datetime.fromtimestamp(ts2).month
    diff = abs(m1 - m2)
    return min(diff, 12 - diff)


def _select_analogs(cands_with_dist: list, k: int) -> list:
    """Return up to K (distance, candidate) pairs sorted by distance ascending."""
    valid = [(d, c) for d, c in cands_with_dist if d is not None]
    valid.sort(key=lambda x: x[0])
    return valid[:k]


def _mean_forecast(futures: list) -> float | None:
    valid = [v for v in futures if v is not None]
    if not valid:
        return None
    return sum(valid) / len(valid)


def _dist_weighted_forecast(dist_val_pairs: list) -> float | None:
    """Inverse-distance-weighted mean; exact matches (d=0) dominate."""
    valid = [(d, v) for d, v in dist_val_pairs if v is not None]
    if not valid:
        return None
    exact = [v for d, v in valid if d == 0]
    if exact:
        return sum(exact) / len(exact)
    total_w = sum(1.0 / d for d, _ in valid)
    return sum((1.0 / d) * v for d, v in valid) / total_w


def run(obs, issued_at: int, *, conn_in, weights=None) -> list[dict]:
    candidates = db.analog_candidates(conn_in, obs["timestamp"])
    obs_vec = {col: obs[col] for col in _FEATURES}
    sigmas = _norm_sigmas(candidates)

    # compute distances and select analogs once per member (reused across leads)
    member_analogs: dict[int, list] = {}
    for mid, name, k, feat_weights in _MEMBERS:
        if name == "k5-seasonal":
            cands_with_dist = []
            for cand in candidates:
                d = _distance(obs_vec, cand, feat_weights, sigmas)
                if d is not None:
                    d *= 1.0 + 0.2 * _month_diff(obs["timestamp"], cand["timestamp"])
                cands_with_dist.append((d, cand))
        else:
            cands_with_dist = [
                (_distance(obs_vec, cand, feat_weights, sigmas), cand)
                for cand in candidates
            ]
        member_analogs[mid] = _select_analogs(cands_with_dist, k)

    # deduplicated set of candidate timestamps needed across all members
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

        for mid, name, _k, _fw in _MEMBERS:
            analogs = member_analogs[mid]
            member_vals[mid] = {}

            for variable, col in VARIABLES.items():
                if name == "k10-dist-weighted":
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

        # member_id=0: weighted mean + spread across all named members
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
