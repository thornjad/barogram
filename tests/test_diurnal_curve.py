import math
import time
from datetime import datetime

from models.diurnal_curve import (
    _local_midnight_ts,
    _local_hour_float,
    _hour_means,
    _fit_sine,
    _eval_sine,
    _eval_piecewise,
    _eval_asymmetric,
    _solar_peak_hour,
)


# --- _local_midnight_ts ---

def test_local_midnight_ts_zeroes_time():
    d = datetime(2024, 6, 15, 14, 30, 0)
    ts = int(d.timestamp())
    result = _local_midnight_ts(ts)
    back = datetime.fromtimestamp(result)
    assert back.hour == 0
    assert back.minute == 0
    assert back.second == 0
    assert back.date() == d.date()


# --- _local_hour_float ---

def test_local_hour_float_whole_hour():
    ts = int(datetime(2024, 6, 15, 14, 0, 0).timestamp())
    assert abs(_local_hour_float(ts) - 14.0) < 1e-9


def test_local_hour_float_half_hour():
    ts = int(datetime(2024, 6, 15, 14, 30, 0).timestamp())
    assert abs(_local_hour_float(ts) - 14.5) < 1e-9


def test_local_hour_float_midnight():
    ts = int(datetime(2024, 6, 15, 0, 0, 0).timestamp())
    assert abs(_local_hour_float(ts) - 0.0) < 1e-9


# --- _hour_means ---

def _obs_at_hour(hour: int, value: float, day_offset: int = 0) -> dict:
    """Return a synthetic obs dict at a specific local hour."""
    d = datetime(2024, 6, 15 + day_offset, hour, 0, 0)
    return {"timestamp": int(d.timestamp()), "air_temp": value}


def test_hour_means_empty():
    assert _hour_means([], "air_temp") is None


def test_hour_means_too_few_buckets():
    # 8 distinct hours, each with 3 obs — below min_buckets=12
    rows = [_obs_at_hour(h, 20.0, day_offset=d) for h in range(8) for d in range(3)]
    assert _hour_means(rows, "air_temp") is None


def test_hour_means_sufficient_data():
    # 12 distinct hours, each with 3 obs
    rows = [_obs_at_hour(h, float(h), day_offset=d) for h in range(12) for d in range(3)]
    result = _hour_means(rows, "air_temp")
    assert result is not None
    # each hour's mean should equal the hour value
    assert abs(result[0] - 0.0) < 1e-9
    assert abs(result[5] - 5.0) < 1e-9


def test_hour_means_excludes_thin_bucket():
    # 12 hours with 3 obs each, plus hour 12 with only 2 obs
    rows = [_obs_at_hour(h, float(h), day_offset=d) for h in range(12) for d in range(3)]
    rows += [_obs_at_hour(12, 12.0, day_offset=d) for d in range(2)]
    result = _hour_means(rows, "air_temp")
    assert result is not None
    assert 12 not in result
    assert 0 in result


def test_hour_means_all_none_values():
    rows = [{"timestamp": int(datetime(2024, 6, 15, h, 0, 0).timestamp()), "air_temp": None}
            for h in range(24) for _ in range(3)]
    assert _hour_means(rows, "air_temp") is None


# --- _fit_sine ---

def test_fit_sine_too_few_hours():
    assert _fit_sine({0: 1.0, 1: 2.0}) is None


def test_fit_sine_returns_finite_coeffs():
    hm = {h: math.sin(2 * math.pi * h / 24) for h in range(24)}
    result = _fit_sine(hm)
    assert result is not None
    assert all(math.isfinite(c) for c in result)


# --- _eval_sine ---

def test_eval_sine_constant():
    assert abs(_eval_sine(0.0, 0.0, 0.0, 5.0) - 5.0) < 1e-9
    assert abs(_eval_sine(12.0, 0.0, 0.0, 5.0) - 5.0) < 1e-9


def test_eval_sine_pure_sine():
    # A=1, B=0, C=0, t=6 -> sin(2pi*6/24) = sin(pi/2) = 1.0
    assert abs(_eval_sine(6.0, 1.0, 0.0, 0.0) - 1.0) < 1e-9


# --- _eval_piecewise ---

def test_eval_piecewise_single_entry_returns_none():
    assert _eval_piecewise(6.0, {12: 20.0}) is None


def test_eval_piecewise_midpoint():
    hm = {0: 10.0, 12: 20.0}
    assert abs(_eval_piecewise(6.0, hm) - 15.0) < 1e-9


def test_eval_piecewise_exact_lower_key():
    hm = {0: 10.0, 12: 20.0}
    assert abs(_eval_piecewise(0.0, hm) - 10.0) < 1e-9


def test_eval_piecewise_exact_upper_key():
    # t=12: last pair wraps h1=0+24=24; frac=0 -> returns v0=20.0
    hm = {0: 10.0, 12: 20.0}
    assert abs(_eval_piecewise(12.0, hm) - 20.0) < 1e-9


# --- _eval_asymmetric ---

def test_eval_asymmetric_equal_values_rise_len_zero():
    # two-element hm with identical values: Python's max() returns the first key
    # when values tie, so t_min==t_max==0, rise_len=(0-0)%24=0 -> returns v_min
    # (a single-element hm would hit the len<2 guard and return None instead)
    assert abs(_eval_asymmetric(6.0, {0: 10.0, 12: 10.0}) - 10.0) < 1e-9


def test_eval_asymmetric_at_trough():
    hm = {0: 5.0, 12: 15.0}
    # t_min=0 (v=5), t_max=12 (v=15), rise_len=12
    # t=0: t_rel=0, result = 5 + 10*(1-cos(0))/2 = 5.0
    assert abs(_eval_asymmetric(0.0, hm) - 5.0) < 1e-9


def test_eval_asymmetric_at_peak():
    hm = {0: 5.0, 12: 15.0}
    # t=12: t_rel=12, not < rise_len=12, t_fall=0
    # result = 15 + (5-15)*(1-cos(0))/2 = 15.0
    assert abs(_eval_asymmetric(12.0, hm) - 15.0) < 1e-9


# --- _solar_peak_hour ---

def test_solar_peak_hour_mid_latitude():
    ts = int(datetime(2024, 6, 15, 12, 0, 0).timestamp())
    result = _solar_peak_hour(45.0, ts)
    assert result == 14.0
