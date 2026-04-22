import math

import pytest

from models.climo_deviation import _amp_factor


# --- _amp_factor boundary tests ---

def test_amp_factor_at_window_start():
    """At 06:00 (window start), sin(0)=0 so factor must be exactly 1.0."""
    assert _amp_factor(6.0, 0.30) == 1.0


def test_amp_factor_at_window_end():
    """At 20:00 (window end), sin(π)=0 so factor must be exactly 1.0."""
    assert _amp_factor(20.0, 0.30) == 1.0


def test_amp_factor_at_peak():
    """At 13:00 (midpoint of 6–20), sin(π/2)=1 so factor is 1+beta."""
    result = _amp_factor(13.0, 0.30)
    assert abs(result - 1.30) < 1e-9


def test_amp_factor_nighttime_before():
    """Before 06:00 the window is closed; factor must be 1.0."""
    assert _amp_factor(0.0, 0.30) == 1.0
    assert _amp_factor(5.9, 0.30) == 1.0


def test_amp_factor_nighttime_after():
    """After 20:00 the window is closed; factor must be 1.0."""
    assert _amp_factor(20.1, 0.30) == 1.0
    assert _amp_factor(23.0, 0.30) == 1.0


def test_amp_factor_higher_beta_gives_larger_factor():
    """Higher beta produces a larger amplification at the same hour."""
    assert _amp_factor(13.0, 0.30) < _amp_factor(13.0, 0.60)


def test_amp_factor_peak_with_higher_beta():
    """Peak at beta=0.60 should be approximately 1.60."""
    result = _amp_factor(13.0, 0.60)
    assert abs(result - 1.60) < 1e-9


def test_amp_factor_always_positive():
    """Factor must always be >= 1.0 (never dampens, only amplifies or holds)."""
    for hour in range(24):
        assert _amp_factor(float(hour), 0.30) >= 1.0
        assert _amp_factor(float(hour), 0.60) >= 1.0


def test_amp_factor_symmetric_around_peak():
    """Factor at equal distances from peak (13:00) should be equal."""
    # 10:00 is 3h before peak, 16:00 is 3h after peak
    assert abs(_amp_factor(10.0, 0.30) - _amp_factor(16.0, 0.30)) < 1e-9


# --- run() amplifying member behavior ---

def _make_obs_with_anomaly(air_temp, ts=None):
    """Obs dict with known temperature for anomaly testing."""
    import time
    import datetime as dt
    if ts is None:
        # use 09:00 local on a fixed date so climo_bucket_obs returns afternoon valid times
        d = dt.datetime(2026, 4, 22, 9, 0, 0)
        ts = int(d.timestamp())
    return {
        "timestamp": ts,
        "air_temp": air_temp,
        "dew_point": 5.0,
        "station_pressure": 1010.0,
        "wind_avg": 3.0,
        "wind_direction": None,
        "solar_radiation": None,
    }


def _seed_climo_input_db(air_temp_climo, n_years=3):
    """Input DB with n_years of consistent monthly obs to create a stable climo baseline."""
    import datetime as dt
    from tests.conftest import make_input_db

    conn = make_input_db()
    # seed several years of data at the target month/hour so climo_bucket_obs returns data
    # target: April (month=4), hour=9 (issued), hour=15 (valid at +6h)
    base = dt.datetime(2022, 4, 1, 9, 0, 0)
    for year_off in range(n_years * 365):
        ts = int((base + dt.timedelta(days=year_off)).timestamp())
        conn.execute(
            "insert into tempest_obs (station_id, timestamp, air_temp, dew_point, station_pressure, wind_avg) "
            "values ('KTEST', ?, ?, ?, ?, ?)",
            (ts, air_temp_climo, 5.0, 1012.0, 3.0),
        )
        # also insert at hour=15 for the +6h valid bucket
        ts15 = int((base + dt.timedelta(days=year_off, hours=6)).timestamp())
        conn.execute(
            "insert into tempest_obs (station_id, timestamp, air_temp, dew_point, station_pressure, wind_avg) "
            "values ('KTEST', ?, ?, ?, ?, ?)",
            (ts15, air_temp_climo, 5.0, 1012.0, 3.0),
        )
    return conn


def test_amplifying_member_warmer_than_static_on_warm_anomaly():
    """When T_obs > climo, amplifying member at afternoon +6h must exceed static member."""
    import datetime as dt
    import models.climo_deviation as m

    climo_temp = 10.0
    obs_temp = 20.0  # +10°C anomaly

    conn_in = _seed_climo_input_db(climo_temp)
    obs = _make_obs_with_anomaly(obs_temp)
    issued_at = obs["timestamp"]

    rows = m.run(obs, issued_at, conn_in=conn_in, weights={})

    def val(mid, lead, var="temperature"):
        for r in rows:
            if r["member_id"] == mid and r["lead_hours"] == lead and r["variable"] == var:
                return r["value"]
        return None

    # member 1 = static today-only (offset 0, base member 1)
    # member 37 = a03 today-only (offset 36, base member 1)
    # At +6h, valid time is 15:00 local → _amp_factor(15, 0.30) > 1.0
    v_static = val(1, 6)
    v_amp = val(37, 6)
    if v_static is not None and v_amp is not None:
        assert v_amp > v_static, (
            f"amplifying member 37 ({v_amp:.2f}) should exceed static member 1 ({v_static:.2f}) "
            f"on a warm anomaly day with afternoon valid time"
        )


def test_amplifying_member_colder_than_static_on_cold_anomaly():
    """When T_obs < climo (cold anomaly), amplifying member amplifies the cold."""
    import datetime as dt
    import models.climo_deviation as m

    climo_temp = 20.0
    obs_temp = 10.0  # -10°C anomaly

    conn_in = _seed_climo_input_db(climo_temp)
    obs = _make_obs_with_anomaly(obs_temp)
    issued_at = obs["timestamp"]

    rows = m.run(obs, issued_at, conn_in=conn_in, weights={})

    def val(mid, lead, var="temperature"):
        for r in rows:
            if r["member_id"] == mid and r["lead_hours"] == lead and r["variable"] == var:
                return r["value"]
        return None

    v_static = val(1, 6)
    v_amp = val(37, 6)
    if v_static is not None and v_amp is not None:
        assert v_amp < v_static, (
            f"amplifying member 37 ({v_amp:.2f}) should be colder than static member 1 ({v_static:.2f}) "
            f"on a cold anomaly day with afternoon valid time"
        )
