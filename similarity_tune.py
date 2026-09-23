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
# quality by predicting the test day's own actual +3h temperature change as
# the mean +3h temperature change across its matched days, then compare to
# what actually happened. Lower error means the vector is picking days whose
# near-term behavior actually resembles the test day's -- a held-out check,
# since no weight vector is scored against the same features it was used to
# match on.

import time

import db
import models._confidence as _confidence
import models._similarity as _similarity

_MIN_PRIOR_CANDIDATES = 20  # skip test days too early in history to have a
                            # meaningful candidate pool behind them yet

CANDIDATE_WEIGHT_VECTORS: dict[str, dict[str, float] | None] = {
    "flat (pre-2026-09-23 baseline)": None,
    "guess-1 (temp/dewpoint/pressure weighted)": _confidence.ALL_FEATURE_WEIGHTS,
}


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
    uses) and its actual +3h temperature change, the held-out target this
    backtest scores against."""
    row = dict(row)
    prior = db.nearest_tempest_obs(conn_in, row["timestamp"] - _confidence._TREND_WINDOW_SEC, window_sec=1800)
    row.update(_confidence.compute_trends(row, prior))
    future = db.nearest_tempest_obs(conn_in, row["timestamp"] + _confidence._TREND_WINDOW_SEC, window_sec=1800)
    if future is not None and future["air_temp"] is not None and row.get("air_temp") is not None:
        row["_actual_future_temp_delta"] = future["air_temp"] - row["air_temp"]
    else:
        row["_actual_future_temp_delta"] = None
    return row


def evaluate_weight_vector(conn_in, weight_map: dict[str, float] | None) -> dict:
    """Returns {'n_test_days': int, 'n_with_matches': int, 'mae': float | None}.
    mae is the mean absolute error between each test day's predicted and
    actual +3h temperature change, averaged over every test day that got at
    least one match under this weight vector. None means no test day ever
    matched. Lower mae means this vector picks better analog days."""
    anchor = _day_anchor_ts()
    raw_days = db.full_analog_candidates(conn_in, anchor, lookback_sec=None)
    days = sorted((_augment_day(conn_in, dict(r)) for r in raw_days), key=lambda r: r["timestamp"])

    features = _confidence._DEFAULT_FEATURES + _confidence._TREND_FEATURES
    weights = [weight_map[col] for col in features] if weight_map is not None else None

    errors = []
    n_with_matches = 0
    for i, test_day in enumerate(days):
        if i < _MIN_PRIOR_CANDIDATES or test_day["_actual_future_temp_delta"] is None:
            continue
        prior_days = days[:i]  # strictly earlier days only -- no future leakage
        sigmas = _similarity.norm_sigmas(prior_days, features)
        dists = [(_similarity.distance(test_day, c, features, sigmas, weights), c)
                 for c in prior_days]
        within = [(d, c) for d, c in dists
                  if d is not None and d <= _confidence._MATCH_DISTANCE_THRESHOLD]
        nearest = _similarity.select_k_nearest(within, _confidence._MATCH_MAX_CANDIDATES)
        matched_deltas = [c["_actual_future_temp_delta"] for _, c in nearest
                           if c["_actual_future_temp_delta"] is not None]
        if not matched_deltas:
            continue
        predicted = sum(matched_deltas) / len(matched_deltas)
        errors.append(abs(predicted - test_day["_actual_future_temp_delta"]))
        n_with_matches += 1

    return {
        "n_test_days": len(days),
        "n_with_matches": n_with_matches,
        "mae": (sum(errors) / len(errors)) if errors else None,
    }
