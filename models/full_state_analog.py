import datetime as _dt
import math
import statistics

import db
import models._confidence as _confidence
import models._self_correction as _self_correction

MODEL_ID = 13
MODEL_NAME = "full_state_analog"
NEEDS_CONN_IN = True
NEEDS_CONN_OUT = True
NEEDS_WEIGHTS = True
NEEDS_MATCH_HISTORY = True

# member 18 (self_correction): standard self-correction member
# (models/_self_correction.py) -- member_id=0 minus this model's own learned
# historical bias.
_SELF_CORRECTION_MEMBER = 18

from models._climo_weights import LEAD_HOURS

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
    (9, "full-k3",           3,  _ALL_FEATURES,                                      False, False),
    (10, "full-k15",         15, _ALL_FEATURES,                                      False, False),
    (11, "full-k20",         20, _ALL_FEATURES,                                      False, False),
    (12, "full-k35",         35, _ALL_FEATURES,                                      False, False),
    (13, "full-k50",         50, _ALL_FEATURES,                                      False, False),
]
_ALL_MEMBER_IDS = [m[0] for m in _MEMBERS]

# members 14-17: candidate-pool variants that don't fit the plain (member_id,
# name, K, features, dist_weighted, seasonal) shape above -- each restricts or
# reshapes the candidate pool itself before the same K-nearest/mean-forecast
# machinery runs, registered in migrations/053_full_state_analog_pool_variant_members.sql
_SEASONAL_WINDOW_ID = 14
_REGIME_GATED_ID = 15
_TRAJECTORY_ID = 16
_FULL_FINGERPRINT_ID = 17
_POOL_VARIANT_MEMBER_IDS = [_SEASONAL_WINDOW_ID, _REGIME_GATED_ID, _TRAJECTORY_ID, _FULL_FINGERPRINT_ID]
_ALL_MEMBER_IDS = _ALL_MEMBER_IDS + _POOL_VARIANT_MEMBER_IDS
_CONFIDENCE_MEMBER_IDS = _ALL_MEMBER_IDS + [_SELF_CORRECTION_MEMBER]

_SEASONAL_WINDOW_DAYS = 21
_SEASONAL_WINDOW_K = 15

# same 3h-window, 0.5 hPa rising/falling/steady convention as
# synoptic_state_machine.py's _pressure_tendency_cat, kept independent since
# that function lives in a model file, not a shared module
_REGIME_TREND_THRESHOLD_HPA = 0.5
_REGIME_GATED_K = 15

# trend-only features for trajectory-analog: this model's own _ALL_FEATURES,
# each promoted to its 3h trend delta instead of an instantaneous snapshot
_TRAJECTORY_FEATURES = [f"{col}_trend" for col in _ALL_FEATURES]
_TRAJECTORY_K = 10

# the full snapshot+trend confidence fingerprint (models/_confidence.py's
# _DEFAULT_FEATURES + _TREND_FEATURES), reused here as a forecast-state
# lookup rather than only a confidence-matching input
_FULL_FINGERPRINT_FEATURES = _confidence._DEFAULT_FEATURES + _confidence._TREND_FEATURES
_FULL_FINGERPRINT_K = 20


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


def _day_of_year_diff(ts1: int, ts2: int) -> int:
    d1 = _dt.datetime.fromtimestamp(ts1).timetuple().tm_yday
    d2 = _dt.datetime.fromtimestamp(ts2).timetuple().tm_yday
    diff = abs(d1 - d2)
    return min(diff, 366 - diff)


def _pressure_regime(pressure_trend: float | None) -> str | None:
    if pressure_trend is None:
        return None
    if pressure_trend > _REGIME_TREND_THRESHOLD_HPA:
        return "rising"
    if pressure_trend < -_REGIME_TREND_THRESHOLD_HPA:
        return "falling"
    return "steady"


def _with_trends(conn_in, base: dict) -> dict:
    """base plus one '<col>_trend' key per _confidence._DEFAULT_FEATURES,
    computed against base's own 3h-prior Tempest obs. Used to build the
    trend-augmented obs/candidate views members 15-17 match against."""
    prior = db.nearest_tempest_obs(
        conn_in, base["timestamp"] - _confidence._TREND_WINDOW_SEC, window_sec=1800
    )
    return {**base, **_confidence.compute_trends(base, prior, _confidence._DEFAULT_FEATURES)}


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


def run(obs, issued_at: int, *, conn_in, conn_out=None, weights=None, member_history=None,
        default_matches=None) -> list[dict]:
    candidates = [dict(c) for c in db.full_analog_candidates(conn_in, obs["timestamp"])]
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

    # seasonal-window-restricted analog: same _ALL_FEATURES distance as the
    # full-k members, but candidates outside +/-21 calendar days never enter
    # the pool at all, instead of full-seasonal's whole-year decay penalty
    seasonal_pool = [
        c for c in candidates
        if _day_of_year_diff(obs["timestamp"], c["timestamp"]) <= _SEASONAL_WINDOW_DAYS
    ]
    sigmas = _norm_sigmas(seasonal_pool, _ALL_FEATURES)
    cands_with_dist = [(_distance(obs_vec, c, _ALL_FEATURES, sigmas), c) for c in seasonal_pool]
    member_analogs[_SEASONAL_WINDOW_ID] = _select_analogs(cands_with_dist, _SEASONAL_WINDOW_K)

    # trend-augmented views for the three members that need more than an
    # instantaneous snapshot -- one extra nearest_tempest_obs lookup per
    # candidate, same cost accepted for the shared confidence fingerprint
    obs_trend = _with_trends(conn_in, obs)
    candidates_trend = [_with_trends(conn_in, c) for c in candidates]

    # regime-gated analog: candidate pool filtered to the current 3h
    # pressure-trend regime (rising/falling/steady) before the same
    # _ALL_FEATURES distance runs within that narrower pool. No obs_regime
    # (unknown current trend) means no basis to gate -- abstain rather than
    # falling back to the unfiltered pool.
    obs_regime = _pressure_regime(obs_trend.get("station_pressure_trend"))
    regime_pool = [
        c for c in candidates_trend if _pressure_regime(c.get("station_pressure_trend")) == obs_regime
    ] if obs_regime is not None else []
    sigmas = _norm_sigmas(regime_pool, _ALL_FEATURES)
    cands_with_dist = [(_distance(obs_vec, c, _ALL_FEATURES, sigmas), c) for c in regime_pool]
    member_analogs[_REGIME_GATED_ID] = _select_analogs(cands_with_dist, _REGIME_GATED_K)

    # trajectory-analog: match on each _ALL_FEATURES column's own 3h trend
    # delta instead of its instantaneous value -- the same trend vectors
    # models/_confidence.py already computes for confidence matching,
    # promoted here into an actual forecasting member
    sigmas = _norm_sigmas(candidates_trend, _TRAJECTORY_FEATURES)
    cands_with_dist = [
        (_distance(obs_trend, c, _TRAJECTORY_FEATURES, sigmas), c) for c in candidates_trend
    ]
    member_analogs[_TRAJECTORY_ID] = _select_analogs(cands_with_dist, _TRAJECTORY_K)

    # full trend+snapshot state lookup: the full confidence fingerprint
    # (snapshot + trend, every Tempest sensor) reused as a forecast-state
    # lookup rather than only a confidence-matching input
    sigmas = _norm_sigmas(candidates_trend, _FULL_FINGERPRINT_FEATURES)
    cands_with_dist = [
        (_distance(obs_trend, c, _FULL_FINGERPRINT_FEATURES, sigmas), c) for c in candidates_trend
    ]
    member_analogs[_FULL_FINGERPRINT_ID] = _select_analogs(cands_with_dist, _FULL_FINGERPRINT_K)

    # each member's own selected analog days, reused as its confidence match
    # set too, instead of the shared default fingerprint every other model uses
    matched_ts_by_mid = {
        mid: [c["timestamp"] for _, c in member_analogs[mid]]
        for mid in _ALL_MEMBER_IDS
    }

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

        # the four pool-variant members: plain mean forecast, no
        # distance-weighting, same shape as any non-dist-weighted member above
        for mid in _POOL_VARIANT_MEMBER_IDS:
            analogs = member_analogs[mid]
            member_vals[mid] = {}
            for variable, col in VARIABLES.items():
                futures = [
                    future_cache[cand["timestamp"]][col]
                    if future_cache[cand["timestamp"]] is not None else None
                    for _, cand in analogs
                ]
                member_vals[mid][variable] = _mean_forecast(futures)

        for variable in VARIABLES:
            cell_confidences = _confidence.member_confidences(
                member_history, default_matches, _CONFIDENCE_MEMBER_IDS, variable, lead,
                matched_ts_by_mid,
            )
            for mid in _ALL_MEMBER_IDS:
                rows.append({
                    "model_id": MODEL_ID,
                    "model": MODEL_NAME,
                    "member_id": mid,
                    "issued_at": issued_at,
                    "valid_at": valid_at,
                    "lead_hours": lead,
                    "variable": variable,
                    "value": member_vals[mid][variable],
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
                    mid: weights.get((mid, variable, lead))
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
