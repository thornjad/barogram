# similarity_tune: backtests candidate feature-weight vectors for the shared
# analog-matching fingerprint (models/_confidence.py's find_default_matches)
# against held-out forecast skill. Answers a different question than `tune`:
# `tune` scores how accurate a model has been (ensemble skill weights);
# this scores whether a given weight vector picks GOOD analog days at all,
# independent of any one model's accuracy. Adjacent to `tune` conceptually,
# deliberately separate in code (see the 2026-09-23 barogram-confidence
# message-board thread).
#
# Method: walk every historical day with Tempest data (one representative
# snapshot per day, near local noon) as a simulated "today." For each, search
# for analog matches using ONLY strictly earlier days -- never a day at or
# after the one being tested, so a weight vector can't get credit from
# information the live system could never have had at the time. Score match
# quality by predicting the test day's own actual change in temperature,
# dewpoint and pressure, at +3h and +12h, as the mean change across its matched
# days, then compare to what actually happened. Each target is scored as its
# MAE divided by the MAE of the no-matching baseline (the mean change over all
# earlier days), so the targets' different units average fairly and a ratio
# under 1 means matching beats ignoring the fingerprint. Lower is better, and a
# held-out check, since no weight vector is scored against the same features
# it was used to match on.

import time

import db
import models._confidence as _confidence
import models._similarity as _similarity

_MIN_PRIOR_CANDIDATES = 20  # skip test days too early in history to have a
                            # meaningful candidate pool behind them yet

_GUESS_1 = {
    **{c: w for c, w in {
        "air_temp": 1.5, "dew_point": 1.5, "station_pressure": 1.2, "wind_avg": 0.6,
        "wind_direction": 0.6, "wind_gust": 0.4, "solar_radiation": 0.5, "uv_index": 0.3,
        "precip_accum_day": 0.5, "lightning_count": 0.3, "precip": 0.4, "wind_lull": 0.3,
        "battery": 0.05, "lightning_avg_distance": 0.3, "lightning_strike_last_distance": 0.3,
        "nc_rain": 0.4}.items()},
}
_GUESS_1.update({f"{c}_trend": w * 0.7 for c, w in list(_GUESS_1.items())})

_GUESS_2 = {
    **{c: w for c, w in _GUESS_1.items() if not c.endswith("_trend")},
    "wind_avg": 0.5, "wind_direction": 0.4, "wind_gust": 0.2, "uv_index": 0.1,
    "lightning_count": 0.1, "wind_lull": 0.1, "battery": 0.02, "lightning_avg_distance": 0.1,
    "lightning_strike_last_distance": 0.1, "nc_rain": 0.2,
}
_GUESS_2.update({f"{c}_trend": w * 0.7 for c, w in list(_GUESS_2.items())})

CANDIDATE_WEIGHT_VECTORS: dict[str, dict[str, float] | None] = {
    "flat (pre-2026-09-23 baseline)": None,
    "guess-1 (pre-2026-11-11 weights)": _GUESS_1,
    "guess-2 (noisy lowered, humidity dropped)": _GUESS_2,
    "guess-3 (meteorological tiers, live since 2026-11-11)": _confidence.ALL_FEATURE_WEIGHTS,
}

# (label, column, horizon in seconds): what each test day is asked to predict
TARGETS = [
    (f"{label}{hours}h", col, hours * 3600)
    for hours in (3, 12)
    for label, col in (("temp +", "air_temp"), ("dew +", "dew_point"), ("pres +", "station_pressure"))
]


def _day_anchor_ts() -> int:
    """An arbitrary local-noon timestamp, used only to seed the time-of-day
    that full_analog_candidates matches every historical day's own snapshot
    against. Noon has no special meaning here -- any fixed hour would do,
    since this enumerates one representative observation per calendar day,
    not a specific real forecast run."""
    now = time.localtime()
    anchor = time.struct_time((now.tm_year, now.tm_mon, now.tm_mday, 12, 0, 0, 0, 0, -1))
    return int(time.mktime(anchor))


def _augment_day(conn_in, row: dict) -> dict:
    """Adds this day's own trend deltas (same computation the live system
    uses) and its actual change in each TARGETS column, the held-out targets
    this backtest scores against."""
    row = dict(row)
    prior = db.nearest_tempest_obs(conn_in, row["timestamp"] - _confidence._TREND_WINDOW_SEC, window_sec=1800)
    row.update(_confidence.compute_trends(row, prior))
    row["_actual"] = {}
    for label, col, horizon in TARGETS:
        future = db.nearest_tempest_obs(conn_in, row["timestamp"] + horizon, window_sec=1800)
        if future is not None and future[col] is not None and row.get(col) is not None:
            row["_actual"][label] = future[col] - row[col]
    return row


def evaluate_weight_vector(conn_in, weight_map: dict[str, float] | None) -> dict:
    """Returns {'n_test_days', 'n_with_matches', 'targets': {label: {'mae', 'baseline_mae',
    'ratio'}}, 'score'}. A test day counts for a target only when it has a matched day
    with that target known. 'ratio' is mae / baseline_mae per target and 'score' their
    mean, so lower is better and under 1 means the fingerprint beats the no-matching
    baseline. n_with_matches counts test days with any match at all."""
    anchor = _day_anchor_ts()
    raw_days = db.full_analog_candidates(conn_in, anchor, lookback_sec=None)
    days = sorted((_augment_day(conn_in, dict(r)) for r in raw_days), key=lambda r: r["timestamp"])

    features = _confidence._DEFAULT_FEATURES + _confidence._TREND_FEATURES
    weights = [weight_map[col] for col in features] if weight_map is not None else None

    errors = {label: [] for label, _, _ in TARGETS}
    base_errors = {label: [] for label, _, _ in TARGETS}
    n_with_matches = 0
    for i, test_day in enumerate(days):
        if i < _MIN_PRIOR_CANDIDATES:
            continue
        prior_days = days[:i]  # strictly earlier days only -- no future leakage
        sigmas = _similarity.norm_sigmas(prior_days, features)
        dists = [(_similarity.distance(test_day, c, features, sigmas, weights), c)
                 for c in prior_days]
        norm = _confidence.distance_norm(features, weights, sigmas) if weights is not None else len(features) ** 0.5
        within = [(d / norm, c) for d, c in dists
                  if d is not None and d / norm <= _confidence._SIM_MAX_DISTANCE]
        nearest = _similarity.select_k_nearest(within, _confidence._MATCH_POOL)
        if not nearest:
            continue
        n_with_matches += 1
        for label, _, _ in TARGETS:
            actual = test_day["_actual"].get(label)
            matched = [c["_actual"][label] for _, c in nearest if label in c["_actual"]]
            everyone = [c["_actual"][label] for c in prior_days if label in c["_actual"]]
            if actual is None or not matched or not everyone:
                continue
            errors[label].append(abs(sum(matched) / len(matched) - actual))
            base_errors[label].append(abs(sum(everyone) / len(everyone) - actual))

    targets = {}
    for label, _, _ in TARGETS:
        if not errors[label]:
            continue
        mae = sum(errors[label]) / len(errors[label])
        base = sum(base_errors[label]) / len(base_errors[label])
        targets[label] = {"mae": mae, "baseline_mae": base, "ratio": mae / base if base else None}
    ratios = [t["ratio"] for t in targets.values() if t["ratio"] is not None]
    return {
        "n_test_days": len(days),
        "n_with_matches": n_with_matches,
        "targets": targets,
        "score": sum(ratios) / len(ratios) if ratios else None,
    }
