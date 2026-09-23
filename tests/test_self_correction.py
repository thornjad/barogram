import models._self_correction as sc
from tests.conftest import make_output_db

_ISSUED_AT = 1_700_000_000


def _seed_history(conn, model_id, variable, lead_hours, value, observed, issued_at):
    valid_at = issued_at + lead_hours * 3600
    error = value - observed
    conn.execute(
        """
        insert into forecasts
            (model_id, model, member_id, issued_at, valid_at, lead_hours, variable,
             value, observed, error, mae, scored_at)
        values (?, 'test_model', 0, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (model_id, issued_at, valid_at, lead_hours, variable, value, observed, error,
         abs(error), issued_at + 1),
    )


def test_none_raw_value_returns_none():
    conn = make_output_db()
    assert sc.corrected_value(conn, 4, "temperature", 6, None, _ISSUED_AT) is None


def test_none_conn_out_returns_none():
    assert sc.corrected_value(None, 4, "temperature", 6, 20.0, _ISSUED_AT) is None


def test_too_few_samples_returns_none():
    conn = make_output_db()
    for d in range(1, sc._MIN_SAMPLES):  # one short of the threshold
        _seed_history(conn, 4, "temperature", 6, value=17.0, observed=15.0,
                      issued_at=_ISSUED_AT - d * 86400)

    assert sc.corrected_value(conn, 4, "temperature", 6, 20.0, _ISSUED_AT) is None


def test_learned_bias_shifts_value():
    conn = make_output_db()
    # model's own history at this cell consistently ran +2.0 hot
    for d in range(1, sc._MIN_SAMPLES + 3):
        _seed_history(conn, 4, "temperature", 6, value=17.0, observed=15.0,
                      issued_at=_ISSUED_AT - d * 86400)

    result = sc.corrected_value(conn, 4, "temperature", 6, 20.0, _ISSUED_AT)

    assert abs(result - 18.0) < 1e-9  # 20.0 raw - 2.0 learned bias


def test_history_scoped_to_own_model_and_cell():
    conn = make_output_db()
    # history for a different model_id, a different variable, and a different
    # lead_hours must not leak into this cell's bias
    for d in range(1, sc._MIN_SAMPLES + 3):
        issued = _ISSUED_AT - d * 86400
        _seed_history(conn, 5, "temperature", 6, value=17.0, observed=15.0, issued_at=issued)
        _seed_history(conn, 4, "dewpoint", 6, value=17.0, observed=15.0, issued_at=issued)
        _seed_history(conn, 4, "temperature", 12, value=17.0, observed=15.0, issued_at=issued)

    assert sc.corrected_value(conn, 4, "temperature", 6, 20.0, _ISSUED_AT) is None
