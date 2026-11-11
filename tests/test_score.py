import time

import score
from score import _build_obs_index, _find_nearest_obs
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


# --- calibration counting ---

def _insert_labeled(conn_out, conf_bin, variable="temperature", value=20.0, member_id=1, lead=6):
    conn_out.execute(
        """
        insert into forecasts
            (model_id, model, member_id, issued_at, valid_at, lead_hours, variable, value, conf_bin)
        values (1, 'persistence', ?, ?, ?, ?, ?, ?, ?)
        """,
        (member_id, _PAST - 3600, _PAST, lead, variable, value, conf_bin),
    )


def _counts(conn_out):
    return {
        (r["variable"], r["lead_hours"], r["n_bin"], r["form_bin"], r["score_bin"]): (r["hits"], r["total"])
        for r in conn_out.execute("select * from calib_counts")
    }


def test_scoring_a_labeled_row_tallies_a_hit_once():
    import models._calibration as cal
    conn_in = make_input_db()
    conn_out = make_output_db()
    _insert_obs(conn_in, _PAST, air_temp=20.0)
    _insert_labeled(conn_out, cal.pack(1, 2, 7), value=20.5)   # error 0.5, inside the 1.11 band

    score.run(conn_in, conn_out)
    score.run(conn_in, conn_out)   # a second pass must not count it again

    assert _counts(conn_out) == {("temperature", 6, 1, 2, 7): (1, 1)}


def test_scoring_a_labeled_miss_tallies_total_without_hit():
    import models._calibration as cal
    conn_in = make_input_db()
    conn_out = make_output_db()
    _insert_obs(conn_in, _PAST, air_temp=20.0)
    _insert_labeled(conn_out, cal.pack(0, 1, 3), value=25.0)   # error 5.0, outside the band

    score.run(conn_in, conn_out)

    assert _counts(conn_out) == {("temperature", 6, 0, 1, 3): (0, 1)}


def test_unlabeled_rows_add_nothing_to_the_tally():
    conn_in = make_input_db()
    conn_out = make_output_db()
    _insert_obs(conn_in, _PAST, air_temp=20.0)
    _insert_labeled(conn_out, None, value=20.0)

    score.run(conn_in, conn_out)

    assert _counts(conn_out) == {}


def test_aggregate_row_counts_under_its_reserved_cell():
    import models._calibration as cal
    conn_in = make_input_db()
    conn_out = make_output_db()
    _insert_obs(conn_in, _PAST, air_temp=20.0)
    _insert_labeled(conn_out, cal.aggregate_bin(4), value=20.2, member_id=0)

    score.run(conn_in, conn_out)

    assert _counts(conn_out) == {("temperature", 6, 3, 1, 4): (1, 1)}


def test_overlapping_scorers_count_a_row_once():
    """Two scorers that both selected the same unscored row: only the one whose update
    actually changed the row may add to the tally."""
    import db
    import models._calibration as cal
    conn_out = make_output_db()
    _insert_labeled(conn_out, cal.pack(1, 1, 5), value=20.0)
    row_id = conn_out.execute("select id from forecasts").fetchone()["id"]
    scored = {"id": row_id, "observed": 20.0, "error": 0.0, "mae": 0.0, "scored_at": _NOW,
              "calib": {"variable": "temperature", "lead_hours": 6, "n_bin": 1, "form_bin": 1,
                        "score_bin": 5, "day": 1, "hit": 1}}

    db.update_scored_forecasts(conn_out, [scored])
    db.update_scored_forecasts(conn_out, [scored])

    assert _counts(conn_out) == {("temperature", 6, 1, 1, 5): (1, 1)}
