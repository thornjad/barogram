import math

from models.pressure_tendency import (
    _find_nearest_ts,
    _zambretti_category,
    _gauss_solve,
    _poly_fit,
    _poly_eval,
    _poly_tendency_rate,
    _exp_weights,
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
