import pytest
from models.surface_signs import (
    _angular_diff,
    _convective_category,
    _dp_trend_category,
    _solar_cloud_category,
    _wind_rotation_category,
)


# --- _angular_diff ---

def test_angular_diff_basic_clockwise():
    assert _angular_diff(90, 180) == 90.0


def test_angular_diff_basic_counterclockwise():
    assert _angular_diff(180, 90) == -90.0


def test_angular_diff_wrap_over_north_veering():
    # 350° → 10° is +20° clockwise (veering)
    assert _angular_diff(350, 10) == pytest.approx(20.0)


def test_angular_diff_wrap_over_north_backing():
    # 10° → 350° is -20° (backing)
    assert _angular_diff(10, 350) == pytest.approx(-20.0)


def test_angular_diff_exactly_180():
    assert _angular_diff(0, 180) == 180.0


def test_angular_diff_just_past_180_is_negative():
    # 0° → 181° is the short way round: -179°
    assert _angular_diff(0, 181) == pytest.approx(-179.0)


def test_angular_diff_no_change():
    assert _angular_diff(270, 270) == 0.0


# --- _wind_rotation_category ---

def _make_wind_obs(ts, direction, speed=3.0):
    return {"timestamp": ts, "wind_direction": direction, "wind_avg": speed}


def test_wind_rotation_empty_window():
    assert _wind_rotation_category([]) is None


def test_wind_rotation_all_low_wind():
    obs = [_make_wind_obs(0, 180, speed=1.0), _make_wind_obs(3600, 200, speed=0.5)]
    assert _wind_rotation_category(obs) is None


def test_wind_rotation_only_one_qualifying():
    obs = [
        _make_wind_obs(0, 180, speed=0.5),   # below threshold
        _make_wind_obs(3600, 200, speed=3.0), # only qualifying row
    ]
    assert _wind_rotation_category(obs) is None


def test_wind_rotation_veering():
    obs = [_make_wind_obs(0, 180), _make_wind_obs(3600, 220)]  # +40° clockwise
    assert _wind_rotation_category(obs) == "veering"


def test_wind_rotation_backing():
    obs = [_make_wind_obs(0, 220), _make_wind_obs(3600, 180)]  # -40° counterclockwise
    assert _wind_rotation_category(obs) == "backing"


def test_wind_rotation_steady():
    obs = [_make_wind_obs(0, 200), _make_wind_obs(3600, 205)]  # only +5°
    assert _wind_rotation_category(obs) == "steady"


def test_wind_rotation_wrap_veering():
    obs = [_make_wind_obs(0, 350), _make_wind_obs(3600, 20)]  # +30° across north
    assert _wind_rotation_category(obs) == "veering"


def test_wind_rotation_none_direction_skipped():
    obs = [
        {"timestamp": 0, "wind_direction": None, "wind_avg": 5.0},
        _make_wind_obs(1800, 180),
        _make_wind_obs(3600, 220),
    ]
    assert _wind_rotation_category(obs) == "veering"


# --- _dp_trend_category ---

def _make_dp_obs(temp, dp):
    return {"air_temp": temp, "dew_point": dp, "timestamp": 0}


def test_dp_trend_narrowing():
    # spread now = 2, spread past = 5, delta = -3 → narrowing
    assert _dp_trend_category(_make_dp_obs(15, 13), _make_dp_obs(20, 15)) == "narrowing"


def test_dp_trend_widening():
    # spread now = 8, spread past = 3, delta = +5 → widening
    assert _dp_trend_category(_make_dp_obs(20, 12), _make_dp_obs(15, 12)) == "widening"


def test_dp_trend_steady():
    # spread now = 5, spread past = 5.2, delta = -0.2 → steady
    assert _dp_trend_category(_make_dp_obs(20, 15), _make_dp_obs(20, 14.8)) == "steady"


def test_dp_trend_none_obs_now():
    assert _dp_trend_category(None, _make_dp_obs(20, 15)) is None


def test_dp_trend_none_obs_past():
    assert _dp_trend_category(_make_dp_obs(20, 15), None) is None


def test_dp_trend_none_air_temp():
    assert _dp_trend_category(
        {"air_temp": None, "dew_point": 10.0, "timestamp": 0},
        _make_dp_obs(20, 15),
    ) is None


def test_dp_trend_none_dew_point():
    assert _dp_trend_category(
        {"air_temp": 20.0, "dew_point": None, "timestamp": 0},
        _make_dp_obs(20, 15),
    ) is None


# --- _solar_cloud_category ---

def _make_solar_obs(sr, ts=43200):  # default ts: noon-ish
    return {"solar_radiation": sr, "timestamp": ts}


def test_solar_cloud_none_radiation():
    assert _solar_cloud_category(_make_solar_obs(None), {}) is None


def test_solar_cloud_below_floor():
    assert _solar_cloud_category(_make_solar_obs(3.0), {}) is None


def test_solar_cloud_missing_climo_key():
    assert _solar_cloud_category(_make_solar_obs(400.0), {}) is None


def test_solar_cloud_heavy():
    import datetime
    ts = int(datetime.datetime(2024, 7, 1, 12, 0).timestamp())
    climo = {(7, 12): 800.0}
    obs = {"solar_radiation": 100.0, "timestamp": ts}  # deficit = 0.875
    assert _solar_cloud_category(obs, climo) == "heavy_cloud"


def test_solar_cloud_partial():
    import datetime
    ts = int(datetime.datetime(2024, 7, 1, 12, 0).timestamp())
    climo = {(7, 12): 800.0}
    obs = {"solar_radiation": 480.0, "timestamp": ts}  # deficit = 0.4
    assert _solar_cloud_category(obs, climo) == "partial_cloud"


def test_solar_cloud_clear():
    import datetime
    ts = int(datetime.datetime(2024, 7, 1, 12, 0).timestamp())
    climo = {(7, 12): 800.0}
    obs = {"solar_radiation": 720.0, "timestamp": ts}  # deficit = 0.1
    assert _solar_cloud_category(obs, climo) == "clear"


# --- _convective_category ---

def _make_conv_obs(lightning=0, precip=0.0, ts=0):
    return {"lightning_count": lightning, "precip_accum_day": precip, "timestamp": ts}


def test_convective_lightning_priority_over_precip():
    window = [_make_conv_obs(lightning=2, precip=5.0)]
    obs_1h = _make_conv_obs(precip=0.0)
    obs_now = _make_conv_obs(precip=5.0)
    assert _convective_category(window, obs_1h, obs_now) == "lightning"


def test_convective_precip_only():
    window = [_make_conv_obs(lightning=0)]
    obs_1h = _make_conv_obs(precip=1.0)
    obs_now = _make_conv_obs(precip=3.0)  # delta = 2.0 mm/h > threshold
    assert _convective_category(window, obs_1h, obs_now) == "precip"


def test_convective_dry():
    window = [_make_conv_obs(lightning=0)]
    obs_1h = _make_conv_obs(precip=1.0)
    obs_now = _make_conv_obs(precip=1.1)  # delta = 0.1 mm/h < threshold
    assert _convective_category(window, obs_1h, obs_now) == "dry"


def test_convective_none_lightning_treated_as_zero():
    window = [{"lightning_count": None, "precip_accum_day": 0.0, "timestamp": 0}]
    obs_1h = _make_conv_obs(precip=0.0)
    obs_now = _make_conv_obs(precip=0.0)
    assert _convective_category(window, obs_1h, obs_now) == "dry"


def test_convective_midnight_reset_no_false_precip():
    # precip_accum_day resets at midnight; delta should be clamped to 0
    window = [_make_conv_obs(lightning=0)]
    obs_1h = _make_conv_obs(precip=15.0)  # before midnight, high accumulation
    obs_now = _make_conv_obs(precip=0.0)  # after midnight, reset to 0
    # delta = max(0, 0.0 - 15.0) = 0 → should not trigger precip
    assert _convective_category(window, obs_1h, obs_now) == "dry"


def test_convective_no_obs_1h_ago():
    window = [_make_conv_obs(lightning=0)]
    obs_now = _make_conv_obs(precip=5.0)
    assert _convective_category(window, None, obs_now) == "dry"
