import datetime
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

def test_confidence_for_cell_no_history_returns_none():
    assert confidence.confidence_for_cell([], "temperature", 24, [1000]) is None


def test_confidence_for_cell_too_little_history_returns_none():
    # exactly _MIN_HISTORY_DAYS distinct days, not more — must still return None
    base = 10_000_000
    history = [
        _row("temperature", 24, base + d * _DAY, 1.0)
        for d in range(confidence._MIN_HISTORY_DAYS)
    ]
    assert confidence.confidence_for_cell(history, "temperature", 24, [base]) is None


def test_confidence_for_cell_filters_wrong_cell():
    base = 10_000_000
    history = [
        _row("pressure", 24, base, 1.0),
        _row("temperature", 6, base, 1.0),
    ] + [_row("temperature", 24, base + d * _DAY, 5.0) for d in range(1, 10)]
    # no matched days -> blended_confidence's empty-matched-errors branch (0.0),
    # confirming the cell-filter didn't silently pull in the wrong rows first
    result = confidence.confidence_for_cell(history, "temperature", 24, [])
    assert result == 0.0


def test_confidence_for_cell_counts_multiple_runs_within_tolerance_window():
    """Two scored runs both within _MATCH_HOUR_TOLERANCE_SEC of the matched
    day's own clock-time snapshot both get counted -- not just the nearest
    one. Uses two in-window runs with DIFFERENT errors (0.0 and 8.0) so
    aggregating both (average 4.0) is numerically distinguishable from
    counting only one."""
    base = 10_000_000
    base -= base % _DAY  # day-align so hour offsets below can't cross midnight
    matched_day_start = base + 5 * _DAY
    history = [_row("temperature", 24, base + d * _DAY, 10.0) for d in range(10) if d != 5]
    history.append(_row("temperature", 24, matched_day_start - 20 * 60, 0.0))   # -20min: in window
    history.append(_row("temperature", 24, matched_day_start + 40 * 60, 8.0))   # +40min: in window
    result = confidence.confidence_for_cell(history, "temperature", 24, [matched_day_start])
    assert result is not None

    overall_avg = sum(r["mae"] for r in history) / len(history)
    expected_both_counted = confidence.blended_confidence(
        [0.0, 8.0], overall_avg, confidence._CONFIDENCE_PSEUDOCOUNT
    )
    expected_nearest_only = confidence.blended_confidence(
        [0.0], overall_avg, confidence._CONFIDENCE_PSEUDOCOUNT
    )
    assert abs(result - expected_both_counted) < 1e-9
    assert abs(result - expected_nearest_only) > 1e-6


def test_confidence_for_cell_excludes_runs_outside_hour_tolerance():
    """A run on the SAME matched calendar day but outside
    _MATCH_HOUR_TOLERANCE_SEC of the matched clock-time must NOT count --
    otherwise a match at 3pm would drag in that day's unrelated midnight
    run, diluting the pool with an unrelated time of day."""
    base = 10_000_000
    base -= base % _DAY
    matched_day_start = base + 5 * _DAY
    history = [_row("temperature", 24, base + d * _DAY, 10.0) for d in range(10) if d != 5]
    history.append(_row("temperature", 24, matched_day_start + 1 * 3600, 0.0))   # +1h: in window
    history.append(_row("temperature", 24, matched_day_start + 7 * 3600, 20.0))  # +7h: outside window
    result = confidence.confidence_for_cell(history, "temperature", 24, [matched_day_start])
    assert result is not None

    overall_avg = sum(r["mae"] for r in history) / len(history)
    expected_in_window_only = confidence.blended_confidence(
        [0.0], overall_avg, confidence._CONFIDENCE_PSEUDOCOUNT
    )
    expected_if_whole_day_counted = confidence.blended_confidence(
        [0.0, 20.0], overall_avg, confidence._CONFIDENCE_PSEUDOCOUNT
    )
    assert abs(result - expected_in_window_only) < 1e-9
    assert abs(result - expected_if_whole_day_counted) > 1e-6


def test_confidence_for_cell_ramp_uses_fixed_pseudocount_not_sample_count():
    """The exact bug found in review of the original ramp: the trust
    denominator must be the fixed _CONFIDENCE_PSEUDOCOUNT constant, not
    derived from len(matched_errors), or blend_frac is always 1.0
    regardless of how little evidence exists."""
    base = 10_000_000
    matched_day_start = base + 5 * _DAY
    # long history at a stable baseline error of 10.0, skipping the matched day
    history = [_row("temperature", 24, base + d * _DAY, 10.0) for d in range(30) if d != 5]
    # exactly ONE matched-day sample, wildly different from the baseline
    history.append(_row("temperature", 24, matched_day_start + 3600, 0.0))
    result = confidence.confidence_for_cell(history, "temperature", 24, [matched_day_start])
    # with a fixed pseudocount, 1 sample out of _CONFIDENCE_PSEUDOCOUNT=8 barely
    # blends away from the neutral overall-average baseline -- r should stay
    # close to 1.0 (confidence well below full trust), not snap toward 1.0 the
    # way a broken n/n=1.0 ramp would
    assert result is not None
    assert result < 0.55


# --- blended_confidence ---

def test_blended_confidence_none_overall_avg():
    assert confidence.blended_confidence([1.0], None, 20) is None


def test_blended_confidence_zero_overall_avg():
    assert confidence.blended_confidence([1.0], 0.0, 20) is None


def test_blended_confidence_empty_matched_is_zero():
    assert confidence.blended_confidence([], 5.0, 20) == 0.0


def test_blended_confidence_high_trust_with_many_matches():
    # matched errors well past k, half the overall average error -> better than average
    matched = [1.0] * 20
    result = confidence.blended_confidence(matched, 2.0, 20)
    assert result is not None and result > 0.5


def test_blended_confidence_keeps_climbing_past_old_cap():
    # no hard ceiling: more matched evidence at the same quality keeps
    # increasing trust rather than plateauing once n reaches some fixed cap
    fewer = confidence.blended_confidence([1.0] * 20, 2.0, 8)
    more = confidence.blended_confidence([1.0] * 100, 2.0, 8)
    assert more > fewer


def test_blended_confidence_uniform_group_reproduces_identical_value():
    a = confidence.blended_confidence([2.0, 2.0], 2.0, 20)
    b = confidence.blended_confidence([2.0, 2.0], 2.0, 20)
    assert a == b


# --- _compute_trends ---

def test_compute_trends_none_prior_returns_all_none():
    now = {"timestamp": 1000, "air_temp": 20.0}
    result = confidence._compute_trends(now, None)
    assert all(v is None for v in result.values())
    assert set(result.keys()) == set(confidence._TREND_FEATURES)


def test_compute_trends_plain_diff():
    now = {"timestamp": 20000, "air_temp": 22.0}
    prior = {"timestamp": 10000, "air_temp": 18.0}
    result = confidence._compute_trends(now, prior)
    assert abs(result["air_temp_trend"] - 4.0) < 1e-9


def test_compute_trends_missing_value_is_none():
    now = {"timestamp": 20000, "air_temp": None}
    prior = {"timestamp": 10000, "air_temp": 18.0}
    result = confidence._compute_trends(now, prior)
    assert result["air_temp_trend"] is None


def test_compute_trends_wind_direction_uses_signed_veer():
    now = {"timestamp": 20000, "wind_direction": 10.0}
    prior = {"timestamp": 10000, "wind_direction": 350.0}
    result = confidence._compute_trends(now, prior)
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
    result = confidence._compute_trends(now, prior)
    assert abs(result["precip_accum_day_trend"] - 3.0) < 1e-9


def test_compute_trends_precip_accum_day_crosses_midnight_is_none():
    midnight = _local_midnight_ts(2026, 6, 15)
    prior_ts = midnight - 3600   # 23:00 the day before
    now_ts = midnight + 3600     # 01:00 the next day
    now = {"timestamp": now_ts, "precip_accum_day": 0.3}
    prior = {"timestamp": prior_ts, "precip_accum_day": 8.6}
    result = confidence._compute_trends(now, prior)
    assert result["precip_accum_day_trend"] is None


def test_compute_trends_precip_not_gated_across_midnight():
    """Unlike precip_accum_day, raw precip isn't a since-midnight counter,
    so its trend is a plain diff even across a midnight boundary."""
    midnight = _local_midnight_ts(2026, 6, 15)
    prior = {"timestamp": midnight - 3600, "precip": 1.0}
    now = {"timestamp": midnight + 3600, "precip": 0.4}
    result = confidence._compute_trends(now, prior)
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
    assert all(ts in same_time_of_day for ts in matches)


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


def test_find_default_matches_excludes_candidates_beyond_distance_threshold():
    # conditions far outside anything recorded -- every candidate should
    # land beyond _MATCH_DISTANCE_THRESHOLD, so there is no real match at all
    conn_in = make_input_db()
    now = int(time.time())
    same_time_of_day = [now - d * _DAY for d in range(1, 6)]
    for i, ts in enumerate(same_time_of_day):
        _insert_obs(conn_in, ts, air_temp=15.0 + i)  # 15..19, tight historical spread
    _insert_obs(conn_in, now, air_temp=80.0)  # wildly outside anything recorded
    matches = confidence.find_default_matches(conn_in, now)
    assert matches == []


def test_find_default_matches_caps_at_max_candidates_keeping_closest():
    # more candidates within threshold than _MATCH_MAX_CANDIDATES -- must
    # keep the closest ones by distance, not an arbitrary/first-found slice
    conn_in = make_input_db()
    now = int(time.time())
    n_candidates = confidence._MATCH_MAX_CANDIDATES + 10
    offsets = list(range(1, n_candidates + 1))
    for d in offsets:
        _insert_obs(conn_in, now - d * _DAY, air_temp=20.0 + 0.01 * d)
    _insert_obs(conn_in, now, air_temp=20.0)
    matches = confidence.find_default_matches(conn_in, now)
    assert len(matches) == confidence._MATCH_MAX_CANDIDATES
    expected_closest = {now - d * _DAY for d in offsets[:confidence._MATCH_MAX_CANDIDATES]}
    assert set(matches) == expected_closest


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
    assert matches[0] == same_time_of_day[2]  # battery=2.9, nearest to 2.91


# --- member_confidences ---

def test_member_confidences_degrades_to_none_without_history():
    result = confidence.member_confidences(None, None, [1, 2, 3], "temperature", 24)
    assert result == {1: None, 2: None, 3: None}


def test_member_confidences_per_member_lookup():
    base = 10_000_000
    good_history = [_row("temperature", 24, base + d * _DAY, 1.0) for d in range(10)]
    member_history = {1: good_history, 2: []}
    result = confidence.member_confidences(member_history, [base], [1, 2], "temperature", 24)
    assert result[1] is not None
    assert result[2] is None


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
