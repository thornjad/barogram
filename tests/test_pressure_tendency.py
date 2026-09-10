import math

import datetime as dt
from models.pressure_tendency import (
    _find_nearest_ts,
    _zambretti_category,
    _gauss_solve,
    _poly_fit,
    _poly_eval,
    _poly_tendency_rate,
    _exp_weights,
    _wind_sector,
    _zambretti_forecast,
)


# --- _find_nearest_ts ---

def test_find_nearest_ts_empty():
    assert _find_nearest_ts([], 100) is None


def test_find_nearest_ts_exact_match():
    assert _find_nearest_ts([100], 100) == 100


def test_find_nearest_ts_equidistant():
    result = _find_nearest_ts([90, 110], 100)
    assert result in [90, 110]


def test_find_nearest_ts_closest():
    assert _find_nearest_ts([90, 105, 120], 100) == 105


def test_find_nearest_ts_boundary_inclusive():
    # |700 - 100| = 600 <= max_delta=600 -> returns 100
    assert _find_nearest_ts([100], 700, max_delta=600) == 100


def test_find_nearest_ts_past_boundary():
    # |701 - 100| = 601 > max_delta=600 -> None
    assert _find_nearest_ts([100], 701, max_delta=600) is None


# --- _zambretti_category ---

def test_zambretti_rapid_rise():
    assert _zambretti_category(2.0) == "rapid_rise"


def test_zambretti_rapid_rise_boundary():
    assert _zambretti_category(1.6) == "rapid_rise"


def test_zambretti_slow_rise():
    assert _zambretti_category(0.5) == "slow_rise"


def test_zambretti_slow_rise_boundary():
    assert _zambretti_category(0.1) == "slow_rise"


def test_zambretti_steady_zero():
    assert _zambretti_category(0.0) == "steady"


def test_zambretti_steady_small_negative():
    assert _zambretti_category(-0.05) == "steady"


def test_zambretti_slow_fall_boundary():
    assert _zambretti_category(-0.1) == "slow_fall"


def test_zambretti_slow_fall():
    assert _zambretti_category(-0.5) == "slow_fall"


def test_zambretti_rapid_fall_boundary():
    assert _zambretti_category(-1.6) == "rapid_fall"


def test_zambretti_rapid_fall():
    assert _zambretti_category(-2.0) == "rapid_fall"


# --- _gauss_solve ---

def test_gauss_solve_identity():
    result = _gauss_solve([[1, 0], [0, 1]], [3, 4])
    assert result is not None
    assert abs(result[0] - 3.0) < 1e-9
    assert abs(result[1] - 4.0) < 1e-9


def test_gauss_solve_known_system():
    # 2x + y = 5, x + 3y = 10 -> x=1, y=3
    result = _gauss_solve([[2, 1], [1, 3]], [5, 10])
    assert result is not None
    assert abs(result[0] - 1.0) < 1e-9
    assert abs(result[1] - 3.0) < 1e-9


def test_gauss_solve_singular():
    assert _gauss_solve([[1, 1], [1, 1]], [2, 2]) is None


# --- _poly_fit + _poly_eval round-trip ---

def test_poly_fit_eval_linear():
    # y = t + 1: t=[-1,0,1] -> y=[0,1,2]
    coefs = _poly_fit([-1.0, 0.0, 1.0], [0.0, 1.0, 2.0], degree=1)
    assert coefs is not None
    assert abs(_poly_eval(coefs, 2.0) - 3.0) < 1e-6


def test_poly_fit_eval_quadratic():
    # y = t^2: t=[-1,0,1,2] -> y=[1,0,1,4]
    coefs = _poly_fit([-1.0, 0.0, 1.0, 2.0], [1.0, 0.0, 1.0, 4.0], degree=2)
    assert coefs is not None
    assert abs(_poly_eval(coefs, 3.0) - 9.0) < 1e-6


def test_poly_fit_underdetermined():
    # 1 point for degree-1 fit -> singular normal equations -> None
    assert _poly_fit([0.0], [5.0], degree=1) is None


# --- _poly_tendency_rate ---

def test_poly_tendency_rate_extracts_a1():
    assert abs(_poly_tendency_rate([5.0, 2.0, 0.3]) - 2.0) < 1e-9


def test_poly_tendency_rate_constant():
    assert abs(_poly_tendency_rate([5.0]) - 0.0) < 1e-9


# --- _exp_weights ---

def test_exp_weights_at_zero():
    result = _exp_weights([0.0], half_life_h=1.0)
    assert abs(result[0] - 1.0) < 1e-9


def test_exp_weights_half_life():
    # at t=-1 with half_life=1, weight = exp(-log2) = 0.5
    result = _exp_weights([-1.0], half_life_h=1.0)
    assert abs(result[0] - 0.5) < 1e-9


def test_exp_weights_older_is_lower():
    result = _exp_weights([-2.0, 0.0], half_life_h=1.0)
    assert result[0] < result[1]


# --- _apply_mean_reversion ---

from models.pressure_tendency import _apply_mean_reversion, _REVERSION_LAMBDA


def test_apply_mean_reversion_at_mean_is_identity():
    result = _apply_mean_reversion(993.0, 993.0, 24)
    assert abs(result - 993.0) < 1e-9


def test_apply_mean_reversion_reduces_positive_deviation():
    raw, mean, lead = 1400.0, 993.0, 24
    result = _apply_mean_reversion(raw, mean, lead)
    assert mean < result < raw


def test_apply_mean_reversion_reduces_negative_deviation():
    raw, mean, lead = 800.0, 993.0, 24
    result = _apply_mean_reversion(raw, mean, lead)
    assert raw < result < mean


def test_apply_mean_reversion_formula():
    raw, mean, lead = 1200.0, 993.0, 12
    expected = mean + (raw - mean) * math.exp(-_REVERSION_LAMBDA * lead)
    assert abs(_apply_mean_reversion(raw, mean, lead) - expected) < 1e-9


def test_apply_mean_reversion_short_lead_retains_more():
    raw, mean = 1200.0, 993.0
    r6 = _apply_mean_reversion(raw, mean, 6)
    r24 = _apply_mean_reversion(raw, mean, 24)
    assert r6 > r24


# --- _wind_sector ---

def test_wind_sector_due_north():
    assert _wind_sector(0) == 0


def test_wind_sector_due_south():
    assert _wind_sector(180) == 8


def test_wind_sector_wraps_past_360():
    assert _wind_sector(350) == 0


# --- _zambretti_forecast (real Zambretti algorithm) ---

def test_zambretti_forecast_steady_no_wind_settled_fine():
    letter, desc = _zambretti_forecast(1030.81, 1, None, 0.0)
    assert letter == "A"
    assert desc == "Settled fine"


def test_zambretti_forecast_extreme_low_falling_is_stormy():
    # far below the falling-branch reference pressure -> Z, "Stormy, much rain".
    # this is the exact failure mode of the old 5-bucket classifier: a rapid 3h
    # drop from a normal/high baseline used to hit this same letter regardless
    # of the absolute pressure level.
    letter, desc = _zambretti_forecast(970.0, 1, None, -2.0)
    assert letter == "Z"
    assert desc == "Stormy, much rain"


def test_zambretti_forecast_high_pressure_rapid_fall_is_not_stormy():
    # a fast wobble near a high baseline (1035 -> 1033 hPa in 3h, rate -0.67 hPa/h)
    # must NOT read "stormy" -- this is the bug the real algorithm fixes.
    letter, desc = _zambretti_forecast(1033.0, 1, None, -0.67)
    assert letter != "Z"
    assert desc != "Stormy, much rain"


def test_zambretti_forecast_wind_direction_changes_letter():
    # same pressure/trend, only wind direction differs
    north_letter, _ = _zambretti_forecast(1030.5, 1, 0, 0.0)    # wind from due north
    south_letter, _ = _zambretti_forecast(1030.5, 1, 180, 0.0)  # wind from due south
    assert north_letter != south_letter


def test_zambretti_forecast_growing_season_changes_letter():
    winter_letter, _ = _zambretti_forecast(1027.7, 1, None, 0.5)  # January
    summer_letter, _ = _zambretti_forecast(1027.7, 7, None, 0.5)  # July, growing season
    assert winter_letter != summer_letter


def test_zambretti_forecast_south_hemisphere_flips_wind_sector():
    # north=False shifts the wind sector by 180 degrees before indexing
    north_letter, _ = _zambretti_forecast(1030.5, 1, 0, 0.0, north=True)
    south_letter, _ = _zambretti_forecast(1030.5, 1, 0, 0.0, north=False)
    assert north_letter != south_letter


def test_zambretti_forecast_clamps_to_lut_range():
    # absurdly high pressure on the rising branch must clamp to index 0, not error
    letter, _ = _zambretti_forecast(1200.0, 1, None, 5.0)
    assert letter == "A"


# --- _zambretti_anchor_ts ---

from models.pressure_tendency import _zambretti_anchor_ts


def test_zambretti_anchor_before_today_anchor_uses_yesterday():
    # 09:00 UTC is before today's 15:12 UTC anchor -> falls back to yesterday's
    now_ts = int(dt.datetime(2026, 9, 10, 9, 0, tzinfo=dt.timezone.utc).timestamp())
    anchor = _zambretti_anchor_ts(now_ts)
    expected = int(dt.datetime(2026, 9, 9, 15, 12, tzinfo=dt.timezone.utc).timestamp())
    assert anchor == expected


def test_zambretti_anchor_after_today_anchor_uses_today():
    # 16:00 UTC is after today's 15:12 UTC anchor -> uses today's
    now_ts = int(dt.datetime(2026, 9, 10, 16, 0, tzinfo=dt.timezone.utc).timestamp())
    anchor = _zambretti_anchor_ts(now_ts)
    expected = int(dt.datetime(2026, 9, 10, 15, 12, tzinfo=dt.timezone.utc).timestamp())
    assert anchor == expected


def test_zambretti_anchor_exactly_at_anchor_uses_today():
    now_ts = int(dt.datetime(2026, 9, 10, 15, 12, tzinfo=dt.timezone.utc).timestamp())
    assert _zambretti_anchor_ts(now_ts) == now_ts


def test_zambretti_anchor_is_always_within_24h_in_the_past():
    now_ts = int(dt.datetime(2026, 1, 15, 12, 34, tzinfo=dt.timezone.utc).timestamp())
    anchor = _zambretti_anchor_ts(now_ts)
    assert 0 <= now_ts - anchor < 24 * 3600
