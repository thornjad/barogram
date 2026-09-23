import models.ensemble_bias_correction as ebc
from tests.conftest import make_obs, make_output_db

_ISSUED_AT = 1_700_000_000
_VALID_AT = _ISSUED_AT + 6 * 3600


def _seed_base(conn, model_id, model_name, variable, value, lead_hours=6):
    conn.execute(
        """
        insert into forecasts
            (model_id, model, member_id, issued_at, valid_at, lead_hours, variable, value)
        values (?, ?, 0, ?, ?, ?, ?, ?)
        """,
        (model_id, model_name, _ISSUED_AT, _VALID_AT, lead_hours, variable, value),
    )


def _seed_ensemble_history(conn, variable, value, observed, issued_at, lead_hours=6):
    valid_at = issued_at + lead_hours * 3600
    error = value - observed
    conn.execute(
        """
        insert into forecasts
            (model_id, model, member_id, issued_at, valid_at, lead_hours, variable,
             value, observed, error, mae, scored_at)
        values (100, 'barogram_ensemble', 0, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (issued_at, valid_at, lead_hours, variable, value, observed, error, abs(error), issued_at + 1),
    )


def test_empty_output_returns_empty():
    conn = make_output_db()
    obs = make_obs()
    assert ebc.run(obs, _ISSUED_AT, conn_out=conn) == []


def test_no_history_applies_zero_correction():
    conn = make_output_db()
    _seed_base(conn, 1, "persistence", "temperature", 10.0)
    _seed_base(conn, 2, "climatological_mean", "temperature", 20.0)
    obs = make_obs()

    rows = ebc.run(obs, _ISSUED_AT, conn_out=conn)

    row = next(r for r in rows if r["variable"] == "temperature")
    assert abs(row["value"] - 15.0) < 1e-9  # equal-weight raw blend, no bias yet


def test_learned_bias_shifts_value():
    conn = make_output_db()
    _seed_base(conn, 1, "persistence", "temperature", 10.0)
    _seed_base(conn, 2, "climatological_mean", "temperature", 20.0)
    # model 100's own past runs consistently ran +2.0 hot at this cell
    for i, ts in enumerate(_ISSUED_AT - (d * 86400) for d in range(1, 6)):
        _seed_ensemble_history(conn, "temperature", value=17.0, observed=15.0, issued_at=ts)
    obs = make_obs()

    rows = ebc.run(obs, _ISSUED_AT, conn_out=conn)

    row = next(r for r in rows if r["variable"] == "temperature")
    # raw blend 15.0, minus learned bias +2.0 -> 13.0
    assert abs(row["value"] - 13.0) < 1e-9


def test_model_id_is_101():
    conn = make_output_db()
    _seed_base(conn, 1, "persistence", "temperature", 10.0)
    obs = make_obs()

    rows = ebc.run(obs, _ISSUED_AT, conn_out=conn)

    assert all(r["model_id"] == 101 for r in rows)


def test_required_keys_present():
    conn = make_output_db()
    _seed_base(conn, 1, "persistence", "temperature", 10.0)
    obs = make_obs()

    rows = ebc.run(obs, _ISSUED_AT, conn_out=conn)

    required = {"model_id", "model", "issued_at", "valid_at", "lead_hours", "variable", "value"}
    for row in rows:
        missing = required - row.keys()
        assert not missing, f"row missing keys: {missing}"
