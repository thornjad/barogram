import math
import time

from models._climo_weights import weighted_mean, MEMBERS


def _row(ts, value):
    return {"timestamp": ts, "air_temp": value}


def test_empty_list():
    assert weighted_mean([], "air_temp", 0, lambda a: 1.0) is None


def test_all_none_values():
    now = int(time.time())
    rows = [{"timestamp": now, "air_temp": None}]
    assert weighted_mean(rows, "air_temp", now, lambda a: 1.0) is None


def test_uniform_weights_equals_arithmetic_mean():
    now = int(time.time())
    rows = [_row(now, 10.0), _row(now, 20.0), _row(now, 30.0)]
    result = weighted_mean(rows, "air_temp", now, lambda a: 1.0)
    assert result is not None
    assert abs(result - 20.0) < 1e-9


def test_two_rows_manual_weighted_result():
    # weight_fn returns 2x for both rows; result should still be arithmetic mean
    now = int(time.time())
    rows = [_row(now, 10.0), _row(now, 30.0)]
    result = weighted_mean(rows, "air_temp", now, lambda a: 2.0)
    assert result is not None
    assert abs(result - 20.0) < 1e-9


def test_today_only_lambda_weights_recent_higher():
    # today-only: weight=20 if age < 1 day, else 1
    today_only = MEMBERS[0][2]  # lambda a: 20.0 if a < 1 else 1.0
    issued_at = int(time.time())
    recent_ts = issued_at - int(0.5 * 86400)   # 0.5 days old
    old_ts = issued_at - int(2.0 * 86400)      # 2 days old
    rows = [_row(recent_ts, 10.0), _row(old_ts, 20.0)]
    # expected = (20*10 + 1*20) / (20 + 1) = 220/21
    expected = 220.0 / 21.0
    result = weighted_mean(rows, "air_temp", issued_at, today_only)
    assert result is not None
    assert abs(result - expected) < 1e-9
    assert result < 15.0  # closer to 10 than to 20
