# analog: finds K most similar historical days by weighted Euclidean distance
# in normalized feature space and uses their subsequent weather as a forecast.
# data-starved early on (only ~43 days at first write) but improves over time.
# when fewer candidates exist than K, uses however many are available.
# member_id=0: inverse-MAE weighted mean of members 1-8 when weights available.

import datetime as _dt
import statistics
import time

import db
import models._confidence as _confidence
import models._similarity as _similarity
from models._utils import _sector

MODEL_ID = 8
MODEL_NAME = "analog"
NEEDS_CONN_IN = True
NEEDS_WEIGHTS = True
NEEDS_MATCH_HISTORY = True

from models._climo_weights import LEAD_HOURS

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
    return _similarity.norm_sigmas(candidates, _FEATURES)

def _distance(
    obs_vec: dict,
    candidate,
    weights: list[float],
    sigmas: dict[str, float | None],
) -> float | None:
    """Weighted Euclidean distance in sigma-normalized feature space."""
    return _similarity.distance(obs_vec, candidate, _FEATURES, sigmas, weights)

def _month_diff(ts1: int, ts2: int) -> int:
    """Circular calendar-month distance between two timestamps (0–6)."""
    m1 = _dt.datetime.fromtimestamp(ts1).month
    m2 = _dt.datetime.fromtimestamp(ts2).month
    diff = abs(m1 - m2)
    return min(diff, 12 - diff)

def _select_analogs(cands_with_dist: list, k: int) -> list:
    """Return up to K (distance, candidate) pairs sorted by distance ascending."""
    return _similarity.select_k_nearest(cands_with_dist, k)

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

def run(obs, issued_at: int, *, conn_in, weights=None, member_history=None,
        default_matches=None) -> list[dict]:
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

    # each member's own selected analog days, reused as its confidence match
    # set too, instead of the shared default fingerprint every other model uses
    matched_ts_by_mid = {
        mid: [c["timestamp"] for _, c in member_analogs[mid]]
        for mid in _ALL_MEMBER_IDS
    }

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

        # member_id=0: weighted mean + spread across all named members
        for variable in VARIABLES:
            cell_confidences = _confidence.member_confidences(
                member_history, default_matches, _ALL_MEMBER_IDS, variable, lead,
                matched_ts_by_mid,
            )
            for mid in _ALL_MEMBER_IDS:
                value = member_vals[mid][variable]
                rows.append({
                    "model_id": MODEL_ID,
                    "model": MODEL_NAME,
                    "member_id": mid,
                    "issued_at": issued_at,
                    "valid_at": valid_at,
                    "lead_hours": lead,
                    "variable": variable,
                    "value": value,
                    "confidence": cell_confidences.get(mid),
                })

            valid_pairs = [
                (mid, member_vals[mid][variable])
                for mid in _ALL_MEMBER_IDS
                if member_vals[mid][variable] is not None
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
