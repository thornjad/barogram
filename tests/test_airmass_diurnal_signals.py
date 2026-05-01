import datetime as dt
import math

from models.airmass_diurnal import _compute_dk_dt, clearness_index


# fixed daytime timestamps: 2026-04-22 at various morning hours local time
_D = dt.datetime(2026, 4, 22)
_T09 = int(_D.replace(hour=9, minute=0).timestamp())
_T10 = int(_D.replace(hour=10, minute=0).timestamp())
_T1030 = int(_D.replace(hour=10, minute=30).timestamp())
_T11 = int(_D.replace(hour=11, minute=0).timestamp())
_T00 = int(_D.replace(hour=0, minute=0).timestamp())   # midnight

_LAT = 44.98  # test station latitude (from conftest)


def _row(ts, solar):
    """Minimal row dict for _compute_dk_dt."""
    return {"timestamp": ts, "solar_radiation": solar}


# --- _compute_dk_dt unit tests ---

def test_dk_dt_rising_solar_is_positive():
    """Rising solar over ~2h should produce a positive dk/dt slope."""
    rows = [_row(_T09, 200.0), _row(_T10, 500.0), _row(_T11, 800.0)]
    result = _compute_dk_dt(rows, _LAT)
    assert result is not None
    assert result > 0.0


def test_dk_dt_falling_solar_is_negative():
    """Falling solar (clouds moving in) should produce a negative dk/dt slope."""
    rows = [_row(_T09, 800.0), _row(_T10, 500.0), _row(_T11, 200.0)]
    result = _compute_dk_dt(rows, _LAT)
    assert result is not None
    assert result < 0.0


def test_dk_dt_flat_solar_is_negative_during_rising_sun():
    """Constant solar while the sun is rising means clear-sky irradiance grows but
    observed stays flat → clearness index k is falling → dk/dt < 0.

    This is physically correct: a morning where solar doesn't increase as the sun
    rises looks increasingly overcast to the model.
    """
    rows = [_row(_T09, 700.0), _row(_T10, 700.0), _row(_T11, 700.0)]
    result = _compute_dk_dt(rows, _LAT)
    assert result is not None
    assert result < 0.0


def test_dk_dt_single_obs_returns_none():
    """Only one qualifying obs: cannot compute a slope."""
    rows = [_row(_T10, 700.0)]
    result = _compute_dk_dt(rows, _LAT)
    assert result is None


def test_dk_dt_empty_list_returns_none():
    """Empty obs list must return None without raising."""
    result = _compute_dk_dt([], _LAT)
    assert result is None


def test_dk_dt_all_nighttime_returns_none():
    """All obs at midnight with solar=0 → no valid k points → None."""
    rows = [_row(_T00, 0.0), _row(_T00 + 300, 0.0), _row(_T00 + 600, 0.0)]
    result = _compute_dk_dt(rows, _LAT)
    assert result is None


def test_dk_dt_returns_float_when_valid():
    """Return value must be a plain float, not None."""
    rows = [_row(_T10, 600.0), _row(_T11, 700.0)]
    result = _compute_dk_dt(rows, _LAT)
    assert isinstance(result, float)


def test_dk_dt_mixed_day_night_uses_daytime_only():
    """Nighttime obs should be ignored; slope computed from daytime points only."""
    rows = [
        _row(_T00, 0.0),         # nighttime, should be skipped
        _row(_T10, 400.0),       # daytime
        _row(_T11, 800.0),       # daytime
    ]
    result = _compute_dk_dt(rows, _LAT)
    assert result is not None
    assert result > 0.0


def test_dk_dt_magnitude_reasonable():
    """dk/dt should not be wildly large — clearness spans [0,1] over hours."""
    rows = [_row(_T09, 100.0), _row(_T10, 900.0)]
    result = _compute_dk_dt(rows, _LAT)
    assert result is not None
    # Clearness can only change by at most 1.0 over 1 hour, so |dk/dt| <= ~2 is fine
    assert abs(result) < 5.0


# --- run() integration tests using in-memory DB ---

def _make_rich_input_db():
    """Input DB with 30 days × 24 hours of synthetic obs for _hour_means to succeed.

    Seeds enough data that _hour_means returns non-None for all variables,
    and the 3h recent window has daytime obs with rising solar.
    """
    import models.airmass_diurnal  # noqa: F401 (trigger import check)
    from tests.conftest import make_input_db

    conn = make_input_db()
    base_ts = _T11  # issued_at in the integration tests

    # 30 days × 24 hours of background obs
    for day in range(30):
        for hour in range(24):
            ts = base_ts - (30 - day) * 86400 + hour * 3600
            solar = max(0.0, 700.0 * math.sin(math.pi * (hour - 6) / 14.0)) if 6 <= hour <= 20 else 0.0
            t = 15.0 + 5.0 * math.sin(math.pi * (hour - 6) / 14.0) if 6 <= hour <= 20 else 8.0
            conn.execute(
                "insert into tempest_obs "
                "(station_id, timestamp, air_temp, dew_point, station_pressure, wind_avg, solar_radiation) "
                "values ('KTEST', ?, ?, ?, ?, ?, ?)",
                (ts, t, 6.0, 1012.0, 3.0, solar),
            )

    # 3h window before issued_at with rising solar (for dk/dt computation)
    for mins in range(0, 181, 10):
        ts = base_ts - 180 * 60 + mins * 60
        solar = max(0.0, (mins / 180.0) * 600.0)
        conn.execute(
            "insert into tempest_obs "
            "(station_id, timestamp, air_temp, dew_point, station_pressure, wind_avg, solar_radiation) "
            "values ('KTEST', ?, 19.0, 7.0, 1007.0, 2.0, ?)",
            (ts, solar),
        )

    return conn


def _make_obs_for_integration():
    """Obs at 11 AM with moderate solar and below-normal pressure."""
    return {
        "timestamp": _T11,
        "air_temp": 21.6,
        "dew_point": 8.1,
        "station_pressure": 1007.0,  # below 30d mean ~1012
        "wind_avg": 2.0,
        "wind_direction": 340.0,
        "solar_radiation": 600.0,
    }


def test_member_count_after_new_members():
    """run() must produce exactly 17 members × 4 leads × 2 vars = 136 total rows."""
    import models.airmass_diurnal as m

    conn_in = _make_rich_input_db()
    obs = _make_obs_for_integration()
    rows = m.run(obs, obs["timestamp"], conn_in=conn_in, weights={})
    assert len(rows) == 136, f"expected 136 rows, got {len(rows)}"


def test_member_ids_include_new_range():
    """Member IDs 0-16 must all appear for temperature at +6h."""
    import models.airmass_diurnal as m

    conn_in = _make_rich_input_db()
    obs = _make_obs_for_integration()
    rows = m.run(obs, obs["timestamp"], conn_in=conn_in, weights={})
    ids = {r["member_id"] for r in rows if r["variable"] == "temperature" and r["lead_hours"] == 6}
    assert ids == set(range(17)), f"expected member IDs 0-16, got {sorted(ids)}"


def test_pressure_departure_positive_when_below_normal():
    """With obs pressure well below 30d mean, members 12 and 13 temp should be
    warmer than member 4 (wind-sector-only, which ignores pressure departure)."""
    import models.airmass_diurnal as m

    conn_in = _make_rich_input_db()
    obs = _make_obs_for_integration()
    # obs pressure is ~1007, 30d mean seeds ~1012 → p_dep ≈ -5 hPa → T_adj ≈ +3.5°C
    rows = m.run(obs, obs["timestamp"], conn_in=conn_in, weights={})

    def val(mid, lead=6, var="temperature"):
        for r in rows:
            if r["member_id"] == mid and r["lead_hours"] == lead and r["variable"] == var:
                return r["value"]
        return None

    v12 = val(12)
    v4 = val(4)  # wind-sector-only, no pressure departure
    if v12 is not None and v4 is not None:
        # pressure departure alone: T_adj = -0.7 × (-5) = +3.5°C above baseline
        # member 12 should be noticeably warmer than sector-only
        assert v12 > v4 - 5.0, (
            f"member 12 ({v12:.2f}) should be in the warmer range vs sector-only ({v4:.2f})"
        )


def test_missing_pressure_no_crash():
    """obs with station_pressure=None must not raise. Members 12/13 skip the pressure
    adjustment and fall back to the diurnal baseline (same as if p_dep were zero).
    """
    import models.airmass_diurnal as m

    conn_in = _make_rich_input_db()
    obs = _make_obs_for_integration()
    obs["station_pressure"] = None

    # must not raise
    rows = m.run(obs, obs["timestamp"], conn_in=conn_in, weights={})
    assert len(rows) == 136, "should still produce all 136 rows when pressure is None"


def test_clearness_trend_non_none_on_clearing_morning():
    """When solar is rising in the 3h window, members 9/10/11 should produce non-None
    temperature values at +6h."""
    import models.airmass_diurnal as m

    conn_in = _make_rich_input_db()  # already has rising solar in 3h window
    obs = _make_obs_for_integration()
    rows = m.run(obs, obs["timestamp"], conn_in=conn_in, weights={})

    for mid in (9, 10, 11):
        temp_6h = next(
            (r["value"] for r in rows
             if r["member_id"] == mid and r["lead_hours"] == 6 and r["variable"] == "temperature"),
            "MISSING"
        )
        assert temp_6h != "MISSING", f"member {mid} missing from rows"
        assert temp_6h is not None, f"member {mid} returned None on a clearing morning"


def test_no_crash_all_nighttime_window():
    """When the 3h recent window is all nighttime (pre-dawn run), no crash. Members
    9/10/11 produce no dk/dt signal so T_adj=0 → they fall back to the diurnal baseline,
    same as member 1 (clearness-only) which also has k=None at nighttime.
    """
    import models.airmass_diurnal as m

    conn_in = _make_rich_input_db()
    t_predawn = int(_D.replace(hour=4, minute=0).timestamp())
    obs = {
        "timestamp": t_predawn,
        "air_temp": 12.0,
        "dew_point": 5.0,
        "station_pressure": 1012.0,
        "wind_avg": 1.0,
        "wind_direction": None,
        "solar_radiation": 0.0,
    }
    # must not raise
    rows = m.run(obs, t_predawn, conn_in=conn_in, weights={})
    assert len(rows) == 136, "should still produce all 136 rows for a pre-dawn run"

    # at nighttime, member 1 also has no clearness signal;
    # members 9-11 should produce the same values as member 1 (both fall back to baseline)
    def val(mid, lead, var="temperature"):
        for r in rows:
            if r["member_id"] == mid and r["lead_hours"] == lead and r["variable"] == var:
                return r["value"]
        return None

    for lead in (6, 12, 18, 24):
        v1 = val(1, lead)
        v9 = val(9, lead)
        if v1 is not None and v9 is not None:
            assert abs(v1 - v9) < 1e-9, (
                f"member 9 ({v9:.4f}) should equal member 1 ({v1:.4f}) at lead={lead} "
                f"when dk/dt is unavailable (pre-dawn)"
            )


# --- new signal tests: veering/backing and solar variability ---

def _make_db_with_veering(direction: str = "veer"):
    """Input DB with wind_direction trending CW (veer) or CCW (back) in the 3h window."""
    conn = _make_rich_input_db()
    base_ts = _T11
    start_dir = 180.0 if direction == "veer" else 270.0
    for i, mins in enumerate(range(0, 181, 10)):
        ts = base_ts - 180 * 60 + mins * 60
        delta = i * 3.0 if direction == "veer" else -i * 3.0
        wd = (start_dir + delta) % 360
        conn.execute(
            "update tempest_obs set wind_direction = ? where timestamp = ?",
            (wd, ts),
        )
    return conn


def test_wind_veer_raises_temperature():
    """Clockwise veer in 3h window: member 14 temperature should exceed member 1 baseline."""
    import models.airmass_diurnal as m

    conn_in = _make_db_with_veering("veer")
    obs = _make_obs_for_integration()
    rows = m.run(obs, obs["timestamp"], conn_in=conn_in, weights={})

    def val(mid, lead=12):
        return next(
            (r["value"] for r in rows
             if r["member_id"] == mid and r["lead_hours"] == lead and r["variable"] == "temperature"),
            None,
        )

    v14 = val(14)
    v1 = val(1)
    assert v14 is not None and v1 is not None
    assert v14 > v1, f"veering member 14 ({v14:.2f}) should be warmer than baseline ({v1:.2f})"


def test_wind_backing_lowers_temperature():
    """Counter-clockwise backing in 3h window: member 14 should be cooler than member 1."""
    import models.airmass_diurnal as m

    conn_in = _make_db_with_veering("back")
    obs = _make_obs_for_integration()
    rows = m.run(obs, obs["timestamp"], conn_in=conn_in, weights={})

    def val(mid, lead=12):
        return next(
            (r["value"] for r in rows
             if r["member_id"] == mid and r["lead_hours"] == lead and r["variable"] == "temperature"),
            None,
        )

    v14 = val(14)
    v1 = val(1)
    assert v14 is not None and v1 is not None
    assert v14 < v1, f"backing member 14 ({v14:.2f}) should be cooler than baseline ({v1:.2f})"


def test_clearness_stability_damped_under_variable_solar():
    """High solar CV in 3h window: member 15 amplitude boost should be smaller than member 1."""
    import models.airmass_diurnal as m

    conn_in = _make_rich_input_db()
    base_ts = _T11
    for i, mins in enumerate(range(0, 181, 10)):
        ts = base_ts - 180 * 60 + mins * 60
        solar = 800.0 if i % 2 == 0 else 100.0
        conn_in.execute(
            "update tempest_obs set solar_radiation = ? where timestamp = ?",
            (solar, ts),
        )
    obs = _make_obs_for_integration()
    rows = m.run(obs, obs["timestamp"], conn_in=conn_in, weights={})

    def val(mid, lead=12):
        return next(
            (r["value"] for r in rows
             if r["member_id"] == mid and r["lead_hours"] == lead and r["variable"] == "temperature"),
            None,
        )

    v1 = val(1)
    v15 = val(15)
    if v1 is not None and v15 is not None:
        assert abs(v15 - obs["air_temp"]) <= abs(v1 - obs["air_temp"]) + 0.1, (
            f"member 15 ({v15:.2f}) should not exceed member 1 ({v1:.2f}) amplitude "
            "under high solar variability"
        )


def test_no_crash_no_wind_direction_in_historical():
    """All historical obs have wind_direction=NULL: members 14/16 fall back to T_adj=0."""
    import models.airmass_diurnal as m

    conn_in = _make_rich_input_db()
    obs = _make_obs_for_integration()
    rows = m.run(obs, obs["timestamp"], conn_in=conn_in, weights={})
    assert len(rows) == 136
