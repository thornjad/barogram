import datetime
import math
import time

import models._confidence as confidence
from tests.conftest import make_input_db, make_obs

_DAY = 86400


def _row(variable, lead_hours, issued_at, mae):
    return {"variable": variable, "lead_hours": lead_hours, "issued_at": issued_at, "mae": mae}


def _insert_obs(conn, ts: int, air_temp=20.0, dew_point=12.0,
                station_pressure=1013.0, wind_avg=3.0, battery=None):
    conn.execute(
        """
        insert into tempest_obs
            (station_id, timestamp, air_temp, dew_point, station_pressure,
             wind_avg, wind_gust, wind_direction, precip_accum_day,
             solar_radiation, uv_index, lightning_count, battery)
        values ('KTEST', ?, ?, ?, ?, ?, null, null, 0.0, 0.0, 0.0, 0, ?)
        """,
        (ts, air_temp, dew_point, station_pressure, wind_avg, battery),
    )


# --- confidence_for_cell ---

def _history(n_moments, err, base=10_000_000, extra_days=10):
    """History with a scored run (mae `err`) near each of n_moments matched moments, plus
    unrelated days so the cell has plenty of distinct days. Returns (history, matched timestamps)."""
    base -= base % _DAY
    matched = [base + d * _DAY + 3600 for d in range(n_moments)]
    history = [_row("temperature", 24, ts + 600, err) for ts in matched]
    history += [_row("temperature", 24, base + (n_moments + d) * _DAY, 99.0) for d in range(extra_days)]
    return history, matched


def test_no_history_returns_zero():
    assert confidence.confidence_for_cell([], "temperature", 24, [1000]) == 0.0


def test_too_little_history_returns_zero():
    # exactly _MIN_HISTORY_DAYS distinct days, not more, must still return 0.0
    base = 10_000_000
    history = [_row("temperature", 24, base + d * _DAY, 1.0) for d in range(confidence._MIN_HISTORY_DAYS)]
    assert confidence.confidence_for_cell(history, "temperature", 24, [base]) == 0.0


def test_filters_wrong_cell_and_no_matches_is_zero():
    base = 10_000_000
    history = [
        _row("pressure", 24, base, 1.0),
        _row("temperature", 6, base, 1.0),
    ] + [_row("temperature", 24, base + d * _DAY, 5.0) for d in range(1, 10)]
    assert confidence.confidence_for_cell(history, "temperature", 24, []) == 0.0


def test_no_known_spread_is_zero():
    history, matched = _history(5, 0.0)
    confidence.set_spread({})
    assert confidence.confidence_for_cell(history, "temperature", 24, matched) == 0.0


def test_matches_with_no_scored_run_near_them_are_zero():
    history, _ = _history(5, 0.0)
    far = [10_000_000 - 400 * _DAY]
    assert confidence.confidence_for_cell(history, "temperature", 24, far) == 0.0


def test_two_runs_near_one_matched_moment_count_once():
    """Two scored runs within _MATCH_HOUR_TOLERANCE_SEC of one matched moment are one
    piece of evidence, their errors averaged (0 and 8 give 4), not two."""
    base = 10_000_000
    base -= base % _DAY
    moment = base + 5 * _DAY
    history = [_row("temperature", 24, base + d * _DAY, 10.0) for d in range(10) if d != 5]
    history.append(_row("temperature", 24, moment - 20 * 60, 0.0))
    history.append(_row("temperature", 24, moment + 40 * 60, 8.0))
    confidence.set_spread({("temperature", 24): 5.0})
    expected = math.exp(-2 * (4.0 / 5.0) ** 2) * confidence.evidence(1.0)
    assert abs(confidence.confidence_for_cell(history, "temperature", 24, [moment]) - expected) < 1e-12


def test_runs_outside_hour_tolerance_do_not_count():
    base = 10_000_000
    base -= base % _DAY
    moment = base + 5 * _DAY
    history = [_row("temperature", 24, base + d * _DAY, 10.0) for d in range(10) if d != 5]
    history.append(_row("temperature", 24, moment + 1 * 3600, 0.0))    # in window
    history.append(_row("temperature", 24, moment + 7 * 3600, 20.0))   # same day, outside window
    confidence.set_spread({("temperature", 24): 5.0})
    expected = 1.0 * confidence.evidence(1.0)   # only the zero-error run counts: perfect performance
    assert abs(confidence.confidence_for_cell(history, "temperature", 24, [moment]) - expected) < 1e-12


def test_evidence_curve_shape():
    assert confidence.evidence(0) == 0.0
    assert 0.005 < confidence.evidence(1) < 0.015   # one match: low, but well above the 0.001 floor
    assert confidence.evidence(1) > 5 * confidence._CONFIDENCE_FLOOR
    assert confidence.evidence(10) < 0.07
    assert abs(confidence.evidence(confidence._EVIDENCE_FULL_AT) - 0.99) < 1e-9
    assert confidence.evidence(100000) < 1.0
    values = [confidence.evidence(n) for n in range(0, 400, 7)]
    assert values == sorted(values)


def test_one_perfect_match_is_low_but_above_the_floor():
    history, matched = _history(1, 0.0)
    value = confidence.confidence_for_cell(history, "temperature", 24, matched)
    assert confidence._CONFIDENCE_FLOOR * 5 < value < 0.02


def test_many_perfect_matches_reach_high_confidence():
    history, matched = _history(confidence._EVIDENCE_FULL_AT, 0.0, extra_days=0)
    assert confidence.confidence_for_cell(history, "temperature", 24, matched) >= 0.99


def test_mediocre_member_does_not_gain_confidence_from_volume():
    """q does not grow with data, so a member that is only so-so on its matches stays capped
    at its q however many matches pile up."""
    confidence.set_spread({("temperature", 24): 5.0})
    q = math.exp(-2 * (5.0 / 5.0) ** 2)   # error equal to the spread
    for n in (50, 400, 2000):
        history, matched = _history(n, 5.0, extra_days=0)
        assert confidence.confidence_for_cell(history, "temperature", 24, matched) < q


def test_poor_performance_gives_low_confidence_even_with_lots_of_evidence():
    history, matched = _history(confidence._EVIDENCE_FULL_AT, 20.0, extra_days=0)
    confidence.set_spread({("temperature", 24): 5.0})
    assert confidence.confidence_for_cell(history, "temperature", 24, matched) < 0.001


def test_closer_match_gives_higher_confidence_than_a_looser_one_with_the_same_record():
    history, matched = _history(1, 0.0)
    close = confidence.confidence_for_cell(history, "temperature", 24, [(matched[0], 0.95)])
    loose = confidence.confidence_for_cell(history, "temperature", 24, [(matched[0], 0.3)])
    assert close > loose > 0.0


def test_zero_similarity_matches_are_ignored_and_bare_timestamps_count_as_one():
    history, matched = _history(3, 0.0)
    with_zero = confidence.confidence_for_cell(history, "temperature", 24, [(matched[0], 0.0), matched[1], matched[2]])
    without = confidence.confidence_for_cell(history, "temperature", 24, [(matched[1], 1.0), (matched[2], 1.0)])
    assert with_zero == without


def test_similarity_weights_the_performance_average():
    """a close match that went well and a loose match that went badly: the close one dominates q."""
    base = 10_000_000
    base -= base % _DAY
    good, bad = base + 2 * _DAY + 3600, base + 3 * _DAY + 3600
    history = [_row("temperature", 24, base + d * _DAY, 9.0) for d in range(10, 20)]
    history += [_row("temperature", 24, good + 600, 0.0), _row("temperature", 24, bad + 600, 20.0)]
    confidence.set_spread({("temperature", 24): 5.0})
    weighted = confidence.confidence_for_cell(history, "temperature", 24, [(good, 0.9), (bad, 0.1)])
    flipped = confidence.confidence_for_cell(history, "temperature", 24, [(good, 0.1), (bad, 0.9)])
    assert weighted > flipped


# --- compute_trends ---

def test_compute_trends_none_prior_returns_all_none():
    now = {"timestamp": 1000, "air_temp": 20.0}
    result = confidence.compute_trends(now, None)
    assert all(v is None for v in result.values())
    assert set(result.keys()) == set(confidence._TREND_FEATURES)


def test_compute_trends_plain_diff():
    now = {"timestamp": 20000, "air_temp": 22.0}
    prior = {"timestamp": 10000, "air_temp": 18.0}
    result = confidence.compute_trends(now, prior)
    assert abs(result["air_temp_trend"] - 4.0) < 1e-9


def test_compute_trends_missing_value_is_none():
    now = {"timestamp": 20000, "air_temp": None}
    prior = {"timestamp": 10000, "air_temp": 18.0}
    result = confidence.compute_trends(now, prior)
    assert result["air_temp_trend"] is None


def test_compute_trends_wind_direction_uses_signed_veer():
    now = {"timestamp": 20000, "wind_direction": 10.0}
    prior = {"timestamp": 10000, "wind_direction": 350.0}
    result = confidence.compute_trends(now, prior)
    assert abs(result["wind_direction_trend"] - 20.0) < 1e-9


def _local_midnight_ts(year, month, day):
    """Local-midnight epoch seconds, matching datetime.fromtimestamp's own
    (local) interpretation -- epoch-day-boundary arithmetic (ts % 86400)
    assumes UTC and is wrong everywhere not on UTC."""
    return int(datetime.datetime(year, month, day, 0, 0, 0).timestamp())


def test_compute_trends_precip_accum_day_same_date_is_plain_diff():
    midnight = _local_midnight_ts(2026, 6, 15)
    same_day_ts_a = midnight + 1 * 3600   # 01:00 local
    same_day_ts_b = midnight + 4 * 3600   # 04:00 local, same day
    now = {"timestamp": same_day_ts_b, "precip_accum_day": 5.0}
    prior = {"timestamp": same_day_ts_a, "precip_accum_day": 2.0}
    result = confidence.compute_trends(now, prior)
    assert abs(result["precip_accum_day_trend"] - 3.0) < 1e-9


def test_compute_trends_precip_accum_day_crosses_midnight_is_none():
    midnight = _local_midnight_ts(2026, 6, 15)
    prior_ts = midnight - 3600   # 23:00 the day before
    now_ts = midnight + 3600     # 01:00 the next day
    now = {"timestamp": now_ts, "precip_accum_day": 0.3}
    prior = {"timestamp": prior_ts, "precip_accum_day": 8.6}
    result = confidence.compute_trends(now, prior)
    assert result["precip_accum_day_trend"] is None


def test_compute_trends_precip_not_gated_across_midnight():
    """Unlike precip_accum_day, raw precip isn't a since-midnight counter,
    so its trend is a plain diff even across a midnight boundary."""
    midnight = _local_midnight_ts(2026, 6, 15)
    prior = {"timestamp": midnight - 3600, "precip": 1.0}
    now = {"timestamp": midnight + 3600, "precip": 0.4}
    result = confidence.compute_trends(now, prior)
    assert abs(result["precip_trend"] - (-0.6)) < 1e-9


# --- find_default_matches ---

def test_find_default_matches_empty_on_fresh_database():
    conn_in = make_input_db()
    assert confidence.find_default_matches(conn_in, int(time.time())) == []


def test_find_default_matches_returns_historical_timestamps():
    conn_in = make_input_db()
    now = int(time.time())
    same_time_of_day = [now - d * _DAY for d in range(1, 6)]
    # vary air_temp across candidates -- norm_sigmas needs nonzero variance to
    # compute a real distance at all; identical obs everywhere give every
    # candidate a None distance and an empty result
    for i, ts in enumerate(same_time_of_day):
        _insert_obs(conn_in, ts, air_temp=15.0 + i)
    _insert_obs(conn_in, now, air_temp=20.0)
    matches = confidence.find_default_matches(conn_in, now)
    assert len(matches) > 0
    assert all(ts in same_time_of_day and 0 < sim <= 1 for ts, sim in matches)


def test_default_features_includes_migration_004_columns():
    # battery included deliberately (2026-09-18): a low/declining battery may
    # itself be a signature worth matching on. lightning_avg_distance and
    # lightning_strike_last_distance are raw, independent sensor readings;
    # nc_rain is the same raw per-interval quantity precip already is.
    # heat_index/wind_chill/delta_t and precip_type are deliberately excluded --
    # see the comment above _DEFAULT_FEATURES.
    for col in ("battery", "lightning_avg_distance", "lightning_strike_last_distance", "nc_rain"):
        assert col in confidence._DEFAULT_FEATURES
        assert f"{col}_trend" in confidence._TREND_FEATURES
    for col in ("heat_index", "wind_chill", "delta_t", "precip_type"):
        assert col not in confidence._DEFAULT_FEATURES


def test_find_default_matches_tolerates_missing_battery_everywhere():
    # no candidate (and not "now" either) has a battery reading -- norm_sigmas
    # sees zero non-null values and drops the feature; must not raise or
    # silently return no matches because of it
    conn_in = make_input_db()
    now = int(time.time())
    same_time_of_day = [now - d * _DAY for d in range(1, 6)]
    for i, ts in enumerate(same_time_of_day):
        _insert_obs(conn_in, ts, air_temp=15.0 + i)
    _insert_obs(conn_in, now, air_temp=20.0)
    matches = confidence.find_default_matches(conn_in, now)
    assert len(matches) > 0


def test_find_default_matches_drops_conditions_unlike_anything_recorded():
    # temperature 60 degrees off breaks the critical limit and the distance cap: no real match
    conn_in = make_input_db()
    now = int(time.time())
    for i, ts in enumerate(now - d * _DAY for d in range(1, 6)):
        _insert_obs(conn_in, ts, air_temp=15.0 + i)
    _insert_obs(conn_in, now, air_temp=80.0)
    assert confidence.find_default_matches(conn_in, now) == []


def test_find_default_matches_keeps_the_closest_pool(monkeypatch):
    monkeypatch.setattr(confidence, "_SIM_MAX_DISTANCE", 1e9)   # sigma-normalized, a lone varying column spans several sigma
    conn_in = make_input_db()
    now = int(time.time())
    n_candidates = confidence._MATCH_POOL + 10
    for d in range(1, n_candidates + 1):
        _insert_obs(conn_in, now - d * _DAY, air_temp=20.0 + 0.001 * d)
    _insert_obs(conn_in, now, air_temp=20.0)
    matches = confidence.find_default_matches(conn_in, now)
    assert len(matches) == confidence._MATCH_POOL
    assert {ts for ts, _ in matches} == {now - d * _DAY for d in range(1, confidence._MATCH_POOL + 1)}
    sims = [sim for _, sim in matches]
    assert sims == sorted(sims, reverse=True)   # closer days are more similar


def test_default_similarity_falls_with_distance_and_is_zero_beyond_the_cap():
    cur, cand = {"air_temp": 10.0, "dew_point": 5.0, "station_pressure": 1000.0}, {"air_temp": 10.0, "dew_point": 5.0, "station_pressure": 1000.0}
    assert confidence.default_similarity(0.0, cur, cand) == 1.0
    assert confidence.default_similarity(0.3, cur, cand) > confidence.default_similarity(0.8, cur, cand) > 0.0
    assert confidence.default_similarity(confidence._SIM_MAX_DISTANCE + 0.01, cur, cand) == 0.0


def test_default_similarity_is_zero_when_a_critical_variable_is_too_different():
    cur = {"air_temp": 10.0, "dew_point": 5.0, "station_pressure": 1000.0}
    for col, limit in confidence._CRITICAL_LIMITS.items():
        far = {**cur, col: cur[col] + limit + 0.1}
        assert confidence.default_similarity(0.5, cur, far) == 0.0
        near = {**cur, col: cur[col] + limit - 0.1}
        assert confidence.default_similarity(0.5, cur, near) > 0.0


def test_find_default_matches_uses_battery_when_present():
    # air_temp held identical across every candidate so it can't drive the
    # distance; only battery varies. If battery participates, the candidate
    # closest to "now"'s own battery reading must win.
    conn_in = make_input_db()
    now = int(time.time())
    same_time_of_day = [now - d * _DAY for d in range(1, 6)]
    batteries = [2.0, 2.2, 2.9, 2.5, 2.1]
    for ts, batt in zip(same_time_of_day, batteries):
        _insert_obs(conn_in, ts, air_temp=15.0, battery=batt)
    _insert_obs(conn_in, now, air_temp=15.0, battery=2.91)
    matches = confidence.find_default_matches(conn_in, now)
    assert len(matches) > 0
    assert matches[0][0] == same_time_of_day[2]  # battery=2.9, nearest to 2.91


# --- member_confidences ---

def test_member_confidences_degrades_to_zero_without_history():
    result = confidence.member_confidences(None, None, [1, 2, 3], "temperature", 24)
    assert result == {1: 0.0, 2: 0.0, 3: 0.0}


def test_member_confidences_per_member_lookup():
    base = 10_000_000
    good_history = [_row("temperature", 24, base + d * _DAY, 1.0) for d in range(10)]
    confidence.set_spread({("temperature", 24): 1.0})
    member_history = {1: good_history, 2: []}
    result = confidence.member_confidences(member_history, [base], [1, 2], "temperature", 24)
    assert result[1] > 0.0
    assert result[2] == 0.0


# --- average_confidence ---

def test_average_confidence_all_none():
    assert confidence.average_confidence([None, None]) is None


def test_average_confidence_mixed():
    assert abs(confidence.average_confidence([0.4, None, 0.6]) - 0.5) < 1e-9


# --- combine_pattern / _inject ---

def test_combine_pattern_uniform_confidence_reproduces_weighted_mean():
    pairs = [(1, 10.0), (2, 20.0)]
    weights = {1: 1.0, 2: 3.0}
    confidences = {1: 0.7, 2: 0.7}  # uniform -- confidence should be a no-op
    mean, group_conf = confidence.combine_pattern(pairs, weights, confidences)
    expected = (1.0 * 10.0 + 3.0 * 20.0) / (1.0 + 3.0)
    assert abs(mean - expected) < 1e-9
    assert abs(group_conf - 0.7) < 1e-9


def test_combine_pattern_drops_only_missing_member_not_whole_group():
    """The real, independent-of-confidence behavior fix: a member missing a
    weight is dropped, the rest still combine by weight -- not a plain
    average of the entire group."""
    pairs = [(1, 10.0), (2, 20.0), (3, 30.0)]
    weights = {1: 1.0, 2: None, 3: 1.0}  # member 2 has no weight
    confidences = {1: 0.5, 2: 0.5, 3: 0.5}  # uniform, isolates the weight-fallback behavior
    mean, _ = confidence.combine_pattern(pairs, weights, confidences)
    # only members 1 and 3 combine (equal weight) -> mean of 10.0 and 30.0 = 20.0,
    # NOT a plain average of all three (which would be 20.0 too by coincidence --
    # use asymmetric values to actually discriminate)
    pairs2 = [(1, 10.0), (2, 100.0), (3, 30.0)]
    weights2 = {1: 1.0, 2: None, 3: 1.0}
    mean2, _ = confidence.combine_pattern(pairs2, weights2, confidences)
    assert abs(mean2 - 20.0) < 1e-9  # (10+30)/2, member 2 dropped entirely
    assert mean2 != sum(v for _, v in pairs2) / len(pairs2)  # not the whole-group average


def test_combine_pattern_all_missing_weights_falls_back_to_equal_average():
    pairs = [(1, 10.0), (2, 20.0)]
    weights = {1: None, 2: None}
    confidences = {1: None, 2: None}
    mean, group_conf = confidence.combine_pattern(pairs, weights, confidences)
    assert abs(mean - 15.0) < 1e-9
    assert group_conf is None


def test_combine_pattern_missing_confidence_defaults_to_group_average():
    pairs = [(1, 10.0), (2, 20.0)]
    weights = {1: 1.0, 2: 1.0}
    confidences = {1: 0.8, 2: None}  # member 2's confidence unknown
    mean, group_conf = confidence.combine_pattern(pairs, weights, confidences)
    # member 2 defaults to the group's own known average (0.8), not a fixed constant
    expected = (0.8 * 10.0 + 0.8 * 20.0) / (0.8 + 0.8)
    assert abs(mean - expected) < 1e-9


def test_combine_pattern_floor_prevents_zero_influence():
    pairs = [(1, 10.0), (2, 20.0)]
    weights = {1: 1.0, 2: 1.0}
    confidences = {1: 0.0, 2: 1.0}  # member 1's confidence would be zero unfloored
    mean, _ = confidence.combine_pattern(pairs, weights, confidences)
    # member 1 still contributes at _CONFIDENCE_FLOOR, not exactly 0
    floor = confidence._CONFIDENCE_FLOOR
    expected = (floor * 10.0 + 1.0 * 20.0) / (floor + 1.0)
    assert abs(mean - expected) < 1e-9


def test_inject_total_influence_zero_does_not_raise():
    """The exact case that raised ZeroDivisionError in review: a member at
    weight 0 and another with a real confidence, before the guard existed."""
    pairs = [(1, 10.0), (2, 20.0)]
    weights = {1: 0.0, 2: 0.0}
    confidences = {1: 0.9, 2: None}
    mean, group_conf = confidence._inject(pairs, weights, confidences)
    assert mean == 15.0  # equal-average fallback
    assert abs(group_conf - 0.9) < 1e-9  # average of known confidences, not None


def test_combine_pattern_no_pairs_returns_none():
    assert confidence.combine_pattern([], {}, {}) == (None, None)


def test_inject_all_confidences_unknown_uses_zero_not_neutral_fallback():
    """Every member's own confidence is unknown (never computed, not the
    same as a computed 0.0) -- the group has nothing to lean on, so the
    internal per-member fallback used in place of a missing confidence is
    0.0, not a neutral 0.5 guess. Both members floor identically here (floor
    is symmetric), so the mean is unaffected -- this only proves the
    fallback itself no longer assumes 50% trust when nothing is known."""
    pairs = [(1, 10.0), (2, 20.0)]
    weights = {1: 1.0, 2: 1.0}
    confidences = {1: None, 2: None}
    mean, group_conf = confidence._inject(pairs, weights, confidences)
    assert abs(mean - 15.0) < 1e-9
    assert group_conf is None


# --- CellHistory ---

def _naive_confidence_for_cell(history, variable, lead_hours, matched, spread):
    """The straightforward implementation: filter the whole history per call, loop per match."""
    cell_rows = [r for r in history if r["variable"] == variable and r["lead_hours"] == lead_hours]
    if not cell_rows or len({r["issued_at"] // 86400 for r in cell_rows}) <= confidence._MIN_HISTORY_DAYS:
        return 0.0
    if spread is None or spread <= 0:
        return 0.0
    seen_days, sims, perfs = set(), [], []
    for ts, sim in confidence.as_pairs(matched):
        if ts // 86400 in seen_days or sim <= 0:
            continue
        seen_days.add(ts // 86400)
        errs = [r["mae"] for r in cell_rows if abs(r["issued_at"] - ts) <= confidence._MATCH_HOUR_TOLERANCE_SEC]
        if errs:
            sims.append(sim)
            perfs.append(math.exp(-2 * (sum(errs) / len(errs) / spread) ** 2))
    if not sims:
        return 0.0
    return sum(s * p for s, p in zip(sims, perfs)) / sum(sims) * confidence.evidence(sum(sims))


def test_cell_history_matches_naive_filter_across_cells(monkeypatch):
    import random
    rng = random.Random(7)
    base = 10_000_000
    history = [
        _row(rng.choice(["temperature", "dewpoint"]), rng.choice([1, 6]),
             base + d * _DAY + rng.randrange(0, _DAY, 600), rng.uniform(0.1, 4.0))
        for d in range(60) for _ in range(6)
    ]
    # repeated and same-day matches: the day dedupe must keep only the first
    matched = [(base + 20 * _DAY + 3600, 0.9), (base + 20 * _DAY + 7200, 0.5),
               (base + 33 * _DAY + 40_000, 0.7), base + 50 * _DAY, (base + 41 * _DAY, 0.0)]
    monkeypatch.setattr(confidence, "_spread", {
        (v, l): 2.5 for v in ("temperature", "dewpoint") for l in (1, 6)
    })
    indexed = confidence.CellHistory(history)
    for variable in ("temperature", "dewpoint", "pressure"):
        for lead in (1, 6, 24):
            expected = _naive_confidence_for_cell(history, variable, lead, matched, 2.5 if variable != "pressure" and lead != 24 else None)
            assert abs(confidence.confidence_for_cell(indexed, variable, lead, matched) - expected) < 1e-12
            assert abs(confidence.confidence_for_cell(history, variable, lead, matched) - expected) < 1e-12


def test_distance_norm_is_root_of_total_weight_of_usable_features():
    norm = confidence.distance_norm(["a", "b", "c"], [1.0, 3.0, 5.0], {"a": 1.0, "b": 2.0, "c": None})
    assert norm == 2.0   # sqrt(1 + 3), the feature with no sigma does not count
