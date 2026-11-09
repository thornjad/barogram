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


def _register_raw_mix(conn, model_id):
    conn.execute(
        "insert or ignore into members (model_id, member_id, name) values (?, -1, 'raw_mix')",
        (model_id,),
    )


def _seed_member_history(conn, model_id, member_id, bias, issued_at):
    valid_at = issued_at + 6 * 3600
    conn.execute(
        """
        insert into forecasts
            (model_id, model, member_id, issued_at, valid_at, lead_hours, variable,
             value, observed, error, mae, scored_at)
        values (?, 'test_model', ?, ?, ?, 6, 'temperature', ?, 15.0, ?, ?, ?)
        """,
        (model_id, member_id, issued_at, valid_at, 15.0 + bias, bias, abs(bias), issued_at + 1),
    )


def test_registered_model_learns_from_raw_mix_not_member_zero():
    # the final member 0 already contains the correction, so it must not teach it
    conn = make_output_db()
    _register_raw_mix(conn, 1)
    for d in range(1, sc._MIN_SAMPLES + 3):
        issued = _ISSUED_AT - d * 86400
        _seed_member_history(conn, 1, -1, 2.0, issued)  # raw mix ran +2.0 hot
        _seed_member_history(conn, 1, 0, 5.0, issued)   # final member 0 history

    result = sc.corrected_value(conn, 1, "temperature", 6, 20.0, _ISSUED_AT)

    assert abs(result - 18.0) < 1e-9


def test_unregistered_model_still_learns_from_member_zero():
    conn = make_output_db()
    for d in range(1, sc._MIN_SAMPLES + 3):
        issued = _ISSUED_AT - d * 86400
        _seed_member_history(conn, 2, 0, 3.0, issued)

    result = sc.corrected_value(conn, 2, "temperature", 6, 20.0, _ISSUED_AT)

    assert abs(result - 17.0) < 1e-9
