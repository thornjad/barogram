import time

import models._confidence as confidence
from tests.conftest import make_input_db, make_obs

_DAY = 86400


def _row(variable, lead_hours, issued_at, mae):
    return {"variable": variable, "lead_hours": lead_hours, "issued_at": issued_at, "mae": mae}


def _insert_obs(conn, ts: int, air_temp=20.0, dew_point=12.0,
                station_pressure=1013.0, wind_avg=3.0):
    conn.execute(
        """
        insert into tempest_obs
            (station_id, timestamp, air_temp, dew_point, station_pressure,
             wind_avg, wind_gust, wind_direction, precip_accum_day,
             solar_radiation, uv_index, lightning_count)
        values ('KTEST', ?, ?, ?, ?, ?, null, null, 0.0, 0.0, 0.0, 0)
        """,
        (ts, air_temp, dew_point, station_pressure, wind_avg),
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
    # no matched days -> blended_confidence's empty-matched-errors branch (0.5),
    # confirming the cell-filter didn't silently pull in the wrong rows first
    result = confidence.confidence_for_cell(history, "temperature", 24, [])
    assert result == 0.5


def test_confidence_for_cell_aggregates_every_run_from_a_matched_day():
    """Two scored runs from the SAME matched calendar day both get counted,
    not just the nearest one -- the exact bug an earlier design had. Uses two
    matched-day runs with DIFFERENT errors (0.0 and 20.0) so aggregating both
    (average 10.0) is numerically distinguishable from counting only one."""
    base = 10_000_000
    base -= base % _DAY  # day-align so hour offsets below can't cross midnight
    matched_day_start = base + 5 * _DAY
    history = [_row("temperature", 24, base + d * _DAY, 10.0) for d in range(10) if d != 5]
    history.append(_row("temperature", 24, matched_day_start + 1 * 3600, 0.0))
    history.append(_row("temperature", 24, matched_day_start + 7 * 3600, 20.0))
    result = confidence.confidence_for_cell(history, "temperature", 24, [matched_day_start])
    assert result is not None

    overall_avg = sum(r["mae"] for r in history) / len(history)
    expected_both_counted = confidence.blended_confidence(
        [0.0, 20.0], overall_avg, confidence._MIN_MATCHES_CAP
    )
    expected_nearest_only = confidence.blended_confidence(
        [0.0], overall_avg, confidence._MIN_MATCHES_CAP
    )
    assert abs(result - expected_both_counted) < 1e-9
    assert abs(result - expected_nearest_only) > 1e-6


def test_confidence_for_cell_ramp_uses_fixed_threshold_not_sample_count():
    """The exact bug found in review: min_matches must be the fixed
    _MIN_MATCHES_CAP constant, not derived from len(matched_errors), or
    blend_frac is always 1.0 regardless of how little evidence exists."""
    base = 10_000_000
    matched_day_start = base + 5 * _DAY
    # long history at a stable baseline error of 10.0
    history = [_row("temperature", 24, base + d * _DAY, 10.0) for d in range(30)]
    # exactly ONE matched-day sample, wildly different from the baseline
    history.append(_row("temperature", 24, matched_day_start + 3600, 0.0))
    result = confidence.confidence_for_cell(history, "temperature", 24, [matched_day_start])
    # with a fixed ramp, 1 sample out of _MIN_MATCHES_CAP=20 barely blends away
    # from the neutral overall-average baseline -- r should stay close to 1.0
    # (confidence close to 0.5), not snap to full trust in the single sample
    assert result is not None
    assert result < 0.55  # nowhere near the ~1.0 confidence a dead ramp would give


# --- blended_confidence ---

def test_blended_confidence_none_overall_avg():
    assert confidence.blended_confidence([1.0], None, 20) is None


def test_blended_confidence_zero_overall_avg():
    assert confidence.blended_confidence([1.0], 0.0, 20) is None


def test_blended_confidence_empty_matched_is_neutral():
    assert confidence.blended_confidence([], 5.0, 20) == 0.5


def test_blended_confidence_full_trust_at_cap():
    # matched errors at the cap, half the overall average error -> better than average
    matched = [1.0] * 20
    result = confidence.blended_confidence(matched, 2.0, 20)
    assert result is not None and result > 0.5


def test_blended_confidence_uniform_group_reproduces_identical_value():
    a = confidence.blended_confidence([2.0, 2.0], 2.0, 20)
    b = confidence.blended_confidence([2.0, 2.0], 2.0, 20)
    assert a == b


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
