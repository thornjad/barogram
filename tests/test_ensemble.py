import math

import models.ensemble as ens
from tests.conftest import make_obs, make_output_db

_ISSUED_AT = 1_700_000_000
_VALID_AT = _ISSUED_AT + 6 * 3600


def _seed(conn, model_id, model_name, variable, value, lead_hours=6):
    conn.execute(
        """
        insert into forecasts
            (model_id, model, member_id, issued_at, valid_at, lead_hours, variable, value)
        values (?, ?, 0, ?, ?, ?, ?, ?)
        """,
        (model_id, model_name, _ISSUED_AT, _VALID_AT, lead_hours, variable, value),
    )


def test_empty_output_returns_empty():
    conn = make_output_db()
    obs = make_obs()
    result = ens.run(obs, _ISSUED_AT, conn_out=conn)
    assert result == []


def test_equal_weight_mean_two_models():
    conn = make_output_db()
    _seed(conn, 1, "persistence", "temperature", 10.0)
    _seed(conn, 2, "climatological_mean", "temperature", 20.0)
    obs = make_obs()

    rows = ens.run(obs, _ISSUED_AT, conn_out=conn, weights=None)

    mean_row = next(r for r in rows if r["member_id"] == 0)
    assert abs(mean_row["value"] - 15.0) < 1e-9


def test_weighted_mean():
    conn = make_output_db()
    _seed(conn, 1, "persistence", "temperature", 10.0)
    _seed(conn, 2, "climatological_mean", "temperature", 20.0)
    obs = make_obs()
    # weight model 1 at 0.25, model 2 at 0.75 -> expected mean = 17.5
    weights = {(1, "temperature", 6): 0.25, (2, "temperature", 6): 0.75}

    rows = ens.run(obs, _ISSUED_AT, conn_out=conn, weights=weights)

    mean_row = next(r for r in rows if r["member_id"] == 0)
    assert abs(mean_row["value"] - 17.5) < 1e-9


def test_member_rows_emitted():
    conn = make_output_db()
    _seed(conn, 1, "persistence", "temperature", 10.0)
    _seed(conn, 2, "climatological_mean", "temperature", 20.0)
    obs = make_obs()

    rows = ens.run(obs, _ISSUED_AT, conn_out=conn)

    member_ids = {r["member_id"] for r in rows}
    assert 0 in member_ids
    assert 1 in member_ids
    assert 2 in member_ids


def test_single_model_produces_mean_equal_to_value():
    conn = make_output_db()
    _seed(conn, 1, "persistence", "temperature", 15.0)
    obs = make_obs()

    rows = ens.run(obs, _ISSUED_AT, conn_out=conn)

    mean_row = next(r for r in rows if r["member_id"] == 0)
    assert abs(mean_row["value"] - 15.0) < 1e-9


def test_spread_is_zero_for_single_model():
    conn = make_output_db()
    _seed(conn, 1, "persistence", "temperature", 15.0)
    obs = make_obs()

    rows = ens.run(obs, _ISSUED_AT, conn_out=conn)

    mean_row = next(r for r in rows if r["member_id"] == 0)
    assert mean_row["spread"] == 0.0


def test_spread_correct_for_two_models():
    conn = make_output_db()
    _seed(conn, 1, "persistence", "temperature", 10.0)
    _seed(conn, 2, "climatological_mean", "temperature", 20.0)
    obs = make_obs()

    rows = ens.run(obs, _ISSUED_AT, conn_out=conn)

    mean_row = next(r for r in rows if r["member_id"] == 0)
    # population std dev of [10, 20] around mean 15 = 5.0
    assert abs(mean_row["spread"] - 5.0) < 1e-9


def test_model_id_is_100():
    conn = make_output_db()
    _seed(conn, 1, "persistence", "temperature", 15.0)
    obs = make_obs()

    rows = ens.run(obs, _ISSUED_AT, conn_out=conn)

    assert all(r["model_id"] == 100 for r in rows)


def test_model_name_is_barogram_ensemble():
    conn = make_output_db()
    _seed(conn, 1, "persistence", "temperature", 15.0)
    obs = make_obs()

    rows = ens.run(obs, _ISSUED_AT, conn_out=conn)

    assert all(r["model"] == "barogram_ensemble" for r in rows)


def test_missing_variable_in_one_model_excluded():
    # diurnal_curve omits pressure; the ensemble should still produce results
    # for pressure from the models that do cover it
    conn = make_output_db()
    _seed(conn, 1, "persistence", "pressure", 1013.0)
    # model 6 (diurnal_curve) has no pressure row — not seeded
    obs = make_obs()

    rows = ens.run(obs, _ISSUED_AT, conn_out=conn)

    pressure_mean = next(
        (r for r in rows if r["member_id"] == 0 and r["variable"] == "pressure"), None
    )
    assert pressure_mean is not None
    assert abs(pressure_mean["value"] - 1013.0) < 1e-9


def test_required_keys_present():
    conn = make_output_db()
    _seed(conn, 1, "persistence", "temperature", 15.0)
    obs = make_obs()

    rows = ens.run(obs, _ISSUED_AT, conn_out=conn)

    required = {"model_id", "model", "issued_at", "valid_at", "lead_hours", "variable", "value"}
    for row in rows:
        missing = required - row.keys()
        assert not missing, f"row missing keys: {missing}"
