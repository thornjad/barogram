import math

import barogram


# --- _huber ---

def test_huber_below_delta():
    """Below delta: quadratic (= 0.5 * e^2)."""
    assert abs(barogram._huber(2.0, 5.0) - 0.5 * 4.0) < 1e-12


def test_huber_at_delta():
    """At delta: both branches equal, so the function is continuous."""
    quadratic = 0.5 * 5.0 * 5.0
    linear = 5.0 * (5.0 - 0.5 * 5.0)
    assert abs(quadratic - linear) < 1e-12
    assert abs(barogram._huber(5.0, 5.0) - quadratic) < 1e-12


def test_huber_above_delta():
    """Above delta: linear (= delta * (|e| - 0.5 * delta))."""
    e, d = 10.0, 5.0
    expected = d * (abs(e) - 0.5 * d)
    assert abs(barogram._huber(e, d) - expected) < 1e-12


def test_huber_negative_error():
    """_huber is symmetric: _huber(-e, d) == _huber(e, d)."""
    assert abs(barogram._huber(-3.0, 5.0) - barogram._huber(3.0, 5.0)) < 1e-12


def test_huber_zero_error():
    assert barogram._huber(0.0, 5.0) == 0.0


# --- _mean_huber ---

def test_mean_huber_single_value():
    e, d = 2.0, 5.0
    assert abs(barogram._mean_huber([e], d) - barogram._huber(e, d)) < 1e-12


def test_mean_huber_multiple_values():
    errors = [1.0, 3.0, 7.0]
    delta = 5.0
    expected = sum(barogram._huber(e, delta) for e in errors) / 3
    assert abs(barogram._mean_huber(errors, delta) - expected) < 1e-12


def test_mean_huber_all_zeros():
    assert barogram._mean_huber([0.0, 0.0, 0.0], 5.0) == 0.0
