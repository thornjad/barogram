import models._similarity as sim


# --- norm_sigmas ---

def test_norm_sigmas_skips_zero_variance():
    candidates = [{"air_temp": 20.0}, {"air_temp": 20.0}]
    sigmas = sim.norm_sigmas(candidates, ["air_temp"])
    assert sigmas["air_temp"] is None


def test_norm_sigmas_real_variance():
    candidates = [{"air_temp": 18.0}, {"air_temp": 22.0}]
    sigmas = sim.norm_sigmas(candidates, ["air_temp"])
    assert sigmas["air_temp"] is not None and sigmas["air_temp"] > 0


def test_norm_sigmas_circular_feature_gets_fixed_sigma():
    """wind_direction gets a fixed sigma regardless of the data's own spread,
    since population std dev doesn't map cleanly onto a 0-360 wraparound."""
    candidates = [{"wind_direction": 10.0}, {"wind_direction": 350.0}]
    sigmas = sim.norm_sigmas(candidates, ["wind_direction"])
    assert sigmas["wind_direction"] == sim._WIND_DIR_SIGMA


# --- distance: circular wraparound ---

def test_distance_wind_direction_wraparound_is_small():
    """359 degrees and 1 degree are 2 degrees apart on a compass, not 358."""
    current = {"wind_direction": 359.0}
    candidate = {"wind_direction": 1.0}
    sigmas = {"wind_direction": sim._WIND_DIR_SIGMA}
    d = sim.distance(current, candidate, ["wind_direction"], sigmas)
    expected = 2.0 / sim._WIND_DIR_SIGMA
    assert d is not None
    assert abs(d - expected) < 1e-9


def test_distance_wind_direction_non_wraparound_matches_plain_diff():
    current = {"wind_direction": 90.0}
    candidate = {"wind_direction": 100.0}
    sigmas = {"wind_direction": sim._WIND_DIR_SIGMA}
    d = sim.distance(current, candidate, ["wind_direction"], sigmas)
    expected = 10.0 / sim._WIND_DIR_SIGMA
    assert d is not None
    assert abs(d - expected) < 1e-9


def test_distance_non_circular_feature_uses_plain_difference():
    current = {"air_temp": 20.0}
    candidate = {"air_temp": 15.0}
    sigmas = {"air_temp": 2.0}
    d = sim.distance(current, candidate, ["air_temp"], sigmas)
    assert abs(d - 2.5) < 1e-9  # (20-15)/2.0


# --- distance: general behavior (mirrors analog.py's own prior coverage) ---

def test_distance_identical_returns_zero():
    obs = {"air_temp": 20.0, "dew_point": 12.0}
    sigmas = {"air_temp": 2.0, "dew_point": 1.0}
    d = sim.distance(obs, obs, ["air_temp", "dew_point"], sigmas)
    assert d == 0.0


def test_distance_skips_none_sigma():
    current = {"air_temp": 20.0, "dew_point": 12.0}
    candidate = {"air_temp": 25.0, "dew_point": 12.0}
    sigmas = {"air_temp": None, "dew_point": 1.0}
    d = sim.distance(current, candidate, ["air_temp", "dew_point"], sigmas)
    assert d == 0.0  # air_temp skipped entirely, dew_point contributes nothing (equal)


def test_distance_all_none_sigmas_returns_none():
    current = {"air_temp": 20.0}
    candidate = {"air_temp": 25.0}
    sigmas = {"air_temp": None}
    assert sim.distance(current, candidate, ["air_temp"], sigmas) is None


def test_distance_none_obs_value_skips_dimension():
    current = {"air_temp": None, "dew_point": 12.0}
    candidate = {"air_temp": 25.0, "dew_point": 12.0}
    sigmas = {"air_temp": 2.0, "dew_point": 1.0}
    d = sim.distance(current, candidate, ["air_temp", "dew_point"], sigmas)
    assert d == 0.0


def test_distance_feature_weight_doubles_contribution():
    current = {"air_temp": 20.0}
    candidate = {"air_temp": 22.0}
    sigmas = {"air_temp": 2.0}
    d1 = sim.distance(current, candidate, ["air_temp"], sigmas, weights=[1.0])
    d2 = sim.distance(current, candidate, ["air_temp"], sigmas, weights=[4.0])
    assert abs(d2 - d1 * 2.0) < 1e-9  # sqrt(4x) = 2x


# --- select_k_nearest ---

def test_select_k_nearest_returns_k_sorted():
    cands = [(3.0, "c"), (1.0, "a"), (2.0, "b"), (None, "skip")]
    result = sim.select_k_nearest(cands, 2)
    assert [c for _, c in result] == ["a", "b"]


def test_select_k_nearest_fewer_than_k():
    cands = [(1.0, "a")]
    result = sim.select_k_nearest(cands, 5)
    assert len(result) == 1


def test_select_k_nearest_all_none():
    cands = [(None, "a"), (None, "b")]
    assert sim.select_k_nearest(cands, 5) == []
