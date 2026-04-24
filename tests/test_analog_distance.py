"""Unit tests for analog model distance and aggregation logic."""
import math

import pytest

from models.analog import (
    _dist_weighted_forecast,
    _mean_forecast,
    _month_diff,
    _norm_sigmas,
    _select_analogs,
    _distance,
)


# --- _norm_sigmas ---

def test_norm_sigmas_empty():
    result = _norm_sigmas([])
    assert all(v is None for v in result.values())


def test_norm_sigmas_single_candidate():
    cand = {"air_temp": 20.0, "dew_point": 12.0, "station_pressure": 1013.0, "wind_avg": 3.0}
    result = _norm_sigmas([cand])
    assert all(v is None for v in result.values())


def test_norm_sigmas_all_same_value():
    cands = [
        {"air_temp": 20.0, "dew_point": 12.0, "station_pressure": 1013.0, "wind_avg": 3.0},
        {"air_temp": 20.0, "dew_point": 12.0, "station_pressure": 1013.0, "wind_avg": 3.0},
    ]
    result = _norm_sigmas(cands)
    # pstdev of identical values is 0, which gets converted to None
    assert all(v is None for v in result.values())


def test_norm_sigmas_two_distinct_candidates():
    cands = [
        {"air_temp": 18.0, "dew_point": 10.0, "station_pressure": 1010.0, "wind_avg": 2.0},
        {"air_temp": 22.0, "dew_point": 14.0, "station_pressure": 1016.0, "wind_avg": 4.0},
    ]
    result = _norm_sigmas(cands)
    # pstdev([18, 22]) = 2.0
    assert result["air_temp"] == pytest.approx(2.0)
    assert result["dew_point"] == pytest.approx(2.0)
    assert result["station_pressure"] == pytest.approx(3.0)
    assert result["wind_avg"] == pytest.approx(1.0)


def test_norm_sigmas_skips_none_values():
    cands = [
        {"air_temp": None, "dew_point": 10.0, "station_pressure": 1010.0, "wind_avg": 2.0},
        {"air_temp": None, "dew_point": 14.0, "station_pressure": 1016.0, "wind_avg": 4.0},
    ]
    result = _norm_sigmas(cands)
    assert result["air_temp"] is None
    assert result["dew_point"] is not None


# --- _distance ---

def _make_sigmas():
    return {"air_temp": 2.0, "dew_point": 2.0, "station_pressure": 3.0, "wind_avg": 1.0}


def test_distance_identical_returns_zero():
    obs = {"air_temp": 20.0, "dew_point": 12.0, "station_pressure": 1013.0, "wind_avg": 3.0}
    cand = {"air_temp": 20.0, "dew_point": 12.0, "station_pressure": 1013.0, "wind_avg": 3.0}
    d = _distance(obs, cand, [1, 1, 1, 1], _make_sigmas())
    assert d == pytest.approx(0.0)


def test_distance_single_feature_diff():
    obs = {"air_temp": 22.0, "dew_point": 12.0, "station_pressure": 1013.0, "wind_avg": 3.0}
    cand = {"air_temp": 20.0, "dew_point": 12.0, "station_pressure": 1013.0, "wind_avg": 3.0}
    sigmas = _make_sigmas()
    # only temp differs: (22-20)/2 = 1.0; sqrt(1*1^2) = 1.0
    d = _distance(obs, cand, [1, 1, 1, 1], sigmas)
    assert d == pytest.approx(1.0)


def test_distance_feature_weight_doubles_contribution():
    obs = {"air_temp": 22.0, "dew_point": 12.0, "station_pressure": 1013.0, "wind_avg": 3.0}
    cand = {"air_temp": 20.0, "dew_point": 12.0, "station_pressure": 1013.0, "wind_avg": 3.0}
    sigmas = _make_sigmas()
    d_w1 = _distance(obs, cand, [1, 1, 1, 1], sigmas)
    d_w2 = _distance(obs, cand, [2, 1, 1, 1], sigmas)
    # weight=2: sqrt(2 * 1^2) = sqrt(2); weight=1: sqrt(1^2) = 1
    assert d_w2 == pytest.approx(math.sqrt(2))
    assert d_w1 == pytest.approx(1.0)


def test_distance_skips_none_sigma():
    obs = {"air_temp": 22.0, "dew_point": 12.0, "station_pressure": 1013.0, "wind_avg": 3.0}
    cand = {"air_temp": 20.0, "dew_point": 12.0, "station_pressure": 1013.0, "wind_avg": 3.0}
    sigmas = {"air_temp": None, "dew_point": 2.0, "station_pressure": 3.0, "wind_avg": 1.0}
    # temp feature skipped; only non-None dimensions contribute; all other dims equal -> distance=0
    d = _distance(obs, cand, [1, 1, 1, 1], sigmas)
    assert d == pytest.approx(0.0)


def test_distance_all_none_sigmas_returns_none():
    obs = {"air_temp": 22.0, "dew_point": 12.0, "station_pressure": 1013.0, "wind_avg": 3.0}
    cand = {"air_temp": 20.0, "dew_point": 12.0, "station_pressure": 1013.0, "wind_avg": 3.0}
    sigmas = {"air_temp": None, "dew_point": None, "station_pressure": None, "wind_avg": None}
    d = _distance(obs, cand, [1, 1, 1, 1], sigmas)
    assert d is None


def test_distance_none_obs_value_skips_dimension():
    obs = {"air_temp": None, "dew_point": 12.0, "station_pressure": 1013.0, "wind_avg": 3.0}
    cand = {"air_temp": 20.0, "dew_point": 12.0, "station_pressure": 1013.0, "wind_avg": 3.0}
    # temp skipped due to None in obs; remaining dims equal -> distance=0
    d = _distance(obs, cand, [1, 1, 1, 1], _make_sigmas())
    assert d == pytest.approx(0.0)


def test_distance_symmetric():
    obs = {"air_temp": 20.0, "dew_point": 10.0, "station_pressure": 1010.0, "wind_avg": 2.0}
    cand = {"air_temp": 24.0, "dew_point": 14.0, "station_pressure": 1016.0, "wind_avg": 5.0}
    sigmas = _make_sigmas()
    d_forward = _distance(obs, cand, [1, 1, 1, 1], sigmas)
    d_reverse = _distance(cand, obs, [1, 1, 1, 1], sigmas)
    assert d_forward == pytest.approx(d_reverse)


# --- _month_diff ---

def test_month_diff_same_month():
    import time
    # two timestamps in the same month
    ts = int(time.time())
    assert _month_diff(ts, ts) == 0


def test_month_diff_adjacent_months():
    import datetime
    jan = int(datetime.datetime(2026, 1, 15).timestamp())
    feb = int(datetime.datetime(2026, 2, 15).timestamp())
    assert _month_diff(jan, feb) == 1


def test_month_diff_circular():
    import datetime
    jan = int(datetime.datetime(2026, 1, 15).timestamp())
    dec = int(datetime.datetime(2025, 12, 15).timestamp())
    assert _month_diff(jan, dec) == 1  # circular: Jan-Dec = 1, not 11


def test_month_diff_max_six():
    import datetime
    jan = int(datetime.datetime(2026, 1, 15).timestamp())
    jul = int(datetime.datetime(2026, 7, 15).timestamp())
    assert _month_diff(jan, jul) == 6


# --- _select_analogs ---

def test_select_analogs_returns_k_nearest():
    cands = [(3.0, "c"), (1.0, "a"), (2.0, "b"), (4.0, "d")]
    result = _select_analogs(cands, k=2)
    assert len(result) == 2
    assert result[0] == (1.0, "a")
    assert result[1] == (2.0, "b")


def test_select_analogs_fewer_than_k():
    cands = [(1.0, "a"), (2.0, "b")]
    result = _select_analogs(cands, k=5)
    assert len(result) == 2


def test_select_analogs_skips_none_distance():
    cands = [(None, "x"), (1.0, "a"), (2.0, "b")]
    result = _select_analogs(cands, k=3)
    assert len(result) == 2
    assert all(d is not None for d, _ in result)


def test_select_analogs_all_none():
    cands = [(None, "x"), (None, "y")]
    result = _select_analogs(cands, k=2)
    assert result == []


# --- _mean_forecast ---

def test_mean_forecast_normal():
    assert _mean_forecast([10.0, 20.0, 30.0]) == pytest.approx(20.0)


def test_mean_forecast_all_none():
    assert _mean_forecast([None, None]) is None


def test_mean_forecast_empty():
    assert _mean_forecast([]) is None


def test_mean_forecast_single_value():
    assert _mean_forecast([42.0]) == pytest.approx(42.0)


def test_mean_forecast_mixed_none():
    assert _mean_forecast([None, 10.0, None, 20.0]) == pytest.approx(15.0)


# --- _dist_weighted_forecast ---

def test_dist_weighted_equal_distances():
    pairs = [(1.0, 10.0), (1.0, 20.0)]
    # 1/1 * 10 + 1/1 * 20 / (1/1 + 1/1) = 15
    assert _dist_weighted_forecast(pairs) == pytest.approx(15.0)


def test_dist_weighted_closer_dominates():
    pairs = [(1.0, 10.0), (10.0, 20.0)]
    # weight closer analog more; result should be closer to 10 than 20
    result = _dist_weighted_forecast(pairs)
    assert result < 15.0


def test_dist_weighted_exact_match_dominates():
    pairs = [(0.0, 5.0), (1.0, 100.0)]
    assert _dist_weighted_forecast(pairs) == pytest.approx(5.0)


def test_dist_weighted_all_none():
    assert _dist_weighted_forecast([(1.0, None), (2.0, None)]) is None


def test_dist_weighted_empty():
    assert _dist_weighted_forecast([]) is None


def test_dist_weighted_skips_none_values():
    pairs = [(1.0, None), (2.0, 20.0)]
    result = _dist_weighted_forecast(pairs)
    assert result == pytest.approx(20.0)
