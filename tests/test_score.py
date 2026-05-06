import time

import score
from score import _precip_occurred
from tests.conftest import make_input_db, make_output_db

_NOW = int(time.time())
_PAST = _NOW - 7200       # 2 hours ago — picked up by score query
_YEAR_AGO = _NOW - 86400 * 365


def _insert_forecast(conn_out, variable="temperature", value=20.0, valid_at=None):
    if valid_at is None:
        valid_at = _PAST
    conn_out.execute(
        """
        insert into forecasts
            (model_id, model, member_id, issued_at, valid_at, lead_hours, variable, value)
        values (1, 'persistence', 0, ?, ?, 6, ?, ?)
        """,
        (_PAST - 3600, valid_at, variable, value),
    )
    return conn_out.execute("select last_insert_rowid()").fetchone()[0]


def _insert_obs(conn_in, ts, air_temp=18.5, dew_point=10.0,
                station_pressure=1013.0, wind_avg=3.0, precip_accum_day=None):
    conn_in.execute(
        """
        insert into tempest_obs
            (station_id, timestamp, air_temp, dew_point,
             station_pressure, wind_avg, wind_gust, wind_direction,
             precip_accum_day, solar_radiation, uv_index, lightning_count)
        values ('KTEST', ?, ?, ?, ?, ?, null, null, ?, null, null, null)
        """,
        (ts, air_temp, dew_point, station_pressure, wind_avg, precip_accum_day),
    )


def test_happy_path():
    conn_in = make_input_db()
    conn_out = make_output_db()
    _insert_obs(conn_in, _PAST, air_temp=18.5)
    _insert_forecast(conn_out, "temperature", 20.0, _PAST)

    result = score.run(conn_in, conn_out)

    assert result == {"scored": 1, "skipped": 0}
    row = conn_out.execute("select error, mae, scored_at from forecasts").fetchone()
    assert abs(row["error"] - 1.5) < 1e-9
    assert abs(row["mae"] - 1.5) < 1e-9
    assert row["scored_at"] is not None


def test_negative_error():
    conn_in = make_input_db()
    conn_out = make_output_db()
    _insert_obs(conn_in, _PAST, air_temp=20.0)
    _insert_forecast(conn_out, "temperature", 18.5, _PAST)

    score.run(conn_in, conn_out)

    row = conn_out.execute("select error, mae from forecasts").fetchone()
    assert abs(row["error"] - (-1.5)) < 1e-9
    assert abs(row["mae"] - 1.5) < 1e-9


def test_forecast_value_none_is_skipped():
    conn_in = make_input_db()
    conn_out = make_output_db()
    _insert_forecast(conn_out, "temperature", None, _PAST)

    result = score.run(conn_in, conn_out)

    assert result == {"scored": 0, "skipped": 1}


def test_no_obs_within_window_is_skipped():
    conn_in = make_input_db()
    conn_out = make_output_db()
    # no obs inserted anywhere near _YEAR_AGO
    _insert_forecast(conn_out, "temperature", 20.0, _YEAR_AGO)

    result = score.run(conn_in, conn_out)

    assert result == {"scored": 0, "skipped": 1}


def test_already_scored_not_counted():
    conn_in = make_input_db()
    conn_out = make_output_db()
    _insert_obs(conn_in, _PAST, air_temp=18.5)
    fid = _insert_forecast(conn_out, "temperature", 20.0, _PAST)
    conn_out.execute("update forecasts set scored_at = 1 where id = ?", (fid,))

    result = score.run(conn_in, conn_out)

    assert result == {"scored": 0, "skipped": 0}


def test_multiple_forecasts():
    conn_in = make_input_db()
    conn_out = make_output_db()
    _insert_obs(conn_in, _PAST, air_temp=18.5, dew_point=10.0, station_pressure=1013.0)
    for variable in ("temperature", "dewpoint", "pressure"):
        _insert_forecast(conn_out, variable, 20.0, _PAST)

    result = score.run(conn_in, conn_out)

    assert result["scored"] == 3


def test_variable_column_mapping():
    # each barogram variable must map to the correct tempest_obs column
    mapping = [
        ("temperature", {"air_temp": 20.0}, 20.0),
        ("dewpoint",    {"dew_point": 10.0}, 10.0),
        ("pressure",    {"station_pressure": 1013.0}, 1013.0),
    ]
    for variable, obs_kwargs, expected_obs in mapping:
        conn_in = make_input_db()
        conn_out = make_output_db()
        _insert_obs(conn_in, _PAST, **{
            "air_temp": obs_kwargs.get("air_temp", 0.0),
            "dew_point": obs_kwargs.get("dew_point", 0.0),
            "station_pressure": obs_kwargs.get("station_pressure", 0.0),
            "wind_avg": obs_kwargs.get("wind_avg", 0.0),
        })
        _insert_forecast(conn_out, variable, 0.0, _PAST)
        score.run(conn_in, conn_out)
        row = conn_out.execute("select observed from forecasts").fetchone()
        assert abs(row["observed"] - expected_obs) < 1e-9, \
            f"variable {variable!r} mapped to wrong obs column"


def test_score_precip_prob_rain():
    """precip_prob mae stores Brier score (squared error) when it rained."""
    conn_in = make_input_db()
    conn_out = make_output_db()
    _insert_obs(conn_in, _PAST - 600, precip_accum_day=0.0)
    _insert_obs(conn_in, _PAST + 600, precip_accum_day=1.0)
    _insert_forecast(conn_out, "precip_prob", 0.8, _PAST)
    score.run(conn_in, conn_out)
    row = conn_out.execute("select observed, mae from forecasts").fetchone()
    assert row["observed"] == 1.0
    assert abs(row["mae"] - 0.04) < 1e-6  # (0.8 - 1.0)^2


def test_score_precip_prob_no_rain():
    """precip_prob mae stores Brier score (squared error) when dry."""
    conn_in = make_input_db()
    conn_out = make_output_db()
    _insert_obs(conn_in, _PAST - 600, precip_accum_day=0.0)
    _insert_obs(conn_in, _PAST + 600, precip_accum_day=0.0)
    _insert_forecast(conn_out, "precip_prob", 0.4, _PAST)
    score.run(conn_in, conn_out)
    row = conn_out.execute("select observed, mae from forecasts").fetchone()
    assert row["observed"] == 0.0
    assert abs(row["mae"] - 0.16) < 1e-6  # (0.4 - 0.0)^2


# --- _precip_occurred ---

_DAY1 = 1_700_000_000          # 2023-11-14 (UTC); same local date in any ±12h tz
_DAY2 = _DAY1 + 86_400         # 24h later — always a different local date


def _obs(ts, accum):
    return {"timestamp": ts, "precip_accum_day": accum}


def test_precip_occurred_above_threshold():
    assert _precip_occurred(_obs(_DAY1, 0.0), _obs(_DAY1, 0.2)) == 1.0


def test_precip_occurred_below_threshold():
    assert _precip_occurred(_obs(_DAY1, 0.0), _obs(_DAY1, 0.05)) == 0.0


def test_precip_occurred_exactly_at_threshold():
    # threshold is > 0.1, so exactly 0.1mm returns 0.0
    assert _precip_occurred(_obs(_DAY1, 0.0), _obs(_DAY1, 0.1)) == 0.0


def test_precip_occurred_negative_delta_returns_zero():
    # gauge reset or correction — max(0, negative) treated as dry
    assert _precip_occurred(_obs(_DAY1, 5.0), _obs(_DAY1, 0.0)) == 0.0


def test_precip_occurred_midnight_crossing_returns_none():
    assert _precip_occurred(_obs(_DAY1, 0.0), _obs(_DAY2, 1.0)) is None


def test_precip_occurred_none_pre_obs():
    assert _precip_occurred(None, _obs(_DAY1, 1.0)) is None


def test_precip_occurred_none_post_obs():
    assert _precip_occurred(_obs(_DAY1, 0.0), None) is None


def test_precip_occurred_null_precip_column():
    assert _precip_occurred(_obs(_DAY1, None), _obs(_DAY1, 1.0)) is None


# --- _find_nearest_obs window boundary ---

from score import _find_nearest_obs, _build_obs_index


def _make_obs_index(ts_list):
    rows = [{"timestamp": ts, "air_temp": 20.0, "dew_point": 10.0,
             "station_pressure": 1013.0} for ts in ts_list]
    return _build_obs_index(rows)


def test_find_nearest_obs_exactly_at_boundary():
    sorted_ts, obs_by_ts = _make_obs_index([_PAST])
    assert _find_nearest_obs(sorted_ts, obs_by_ts, _PAST + 1800) is not None
    assert _find_nearest_obs(sorted_ts, obs_by_ts, _PAST - 1800) is not None


def test_find_nearest_obs_one_second_past_boundary():
    sorted_ts, obs_by_ts = _make_obs_index([_PAST])
    assert _find_nearest_obs(sorted_ts, obs_by_ts, _PAST + 1801) is None
    assert _find_nearest_obs(sorted_ts, obs_by_ts, _PAST - 1801) is None


def test_find_nearest_obs_empty_index():
    sorted_ts, obs_by_ts = _make_obs_index([])
    assert _find_nearest_obs(sorted_ts, obs_by_ts, _PAST) is None


# --- NULL obs column ---

def test_unknown_variable_is_skipped():
    conn_in = make_input_db()
    conn_out = make_output_db()
    _insert_obs(conn_in, _PAST, air_temp=18.5)
    _insert_forecast(conn_out, "humidity", 50.0, _PAST)

    result = score.run(conn_in, conn_out)

    assert result == {"scored": 0, "skipped": 1}


def test_null_obs_column_is_skipped():
    conn_in = make_input_db()
    conn_out = make_output_db()
    # insert obs where air_temp is NULL
    conn_in.execute(
        """
        insert into tempest_obs
            (station_id, timestamp, air_temp, dew_point,
             station_pressure, wind_avg, wind_gust, wind_direction,
             precip_accum_day, solar_radiation, uv_index, lightning_count)
        values ('KTEST', ?, null, 10.0, 1013.0, 3.0, null, null, null, null, null, null)
        """,
        (_PAST,),
    )
    _insert_forecast(conn_out, "temperature", 20.0, _PAST)

    result = score.run(conn_in, conn_out)

    assert result == {"scored": 0, "skipped": 1}
