import time

import score
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
    for _ in range(3):
        _insert_obs(conn_in, _PAST, air_temp=18.5)
        _insert_forecast(conn_out, "temperature", 20.0, _PAST)

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
