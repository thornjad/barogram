import db
from tests.conftest import make_output_db

_ISSUED = 1_700_000_000


def _insert(conn, model_id, name, issued_at, lead, variable, value, *, member_id=0, valid_at=None,
            confidence=None, spread=None, error=None, scored=False):
    valid_at = issued_at + lead * 3600 if valid_at is None else valid_at
    conn.execute(
        """
        insert into forecasts
            (model_id, model, member_id, issued_at, valid_at, lead_hours, variable, value,
             spread, confidence, observed, error, mae, scored_at)
        values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, null, ?, ?, ?)
        """,
        (model_id, name, member_id, issued_at, valid_at, lead, variable, value, spread, confidence,
         error, abs(error) if error is not None else None, valid_at + 60 if scored else None),
    )


def test_meta_inputs_returns_requested_ids_and_types_with_confidence_and_spread():
    conn = make_output_db()
    _insert(conn, 100, "barogram_ensemble", _ISSUED, 6, "temperature", 20.0, confidence=0.4, spread=1.5)
    _insert(conn, 202, "external_corrected", _ISSUED, 6, "temperature", 21.0, confidence=0.5)
    _insert(conn, 1, "persistence", _ISSUED, 6, "pressure", 1010.0, confidence=0.1)
    _insert(conn, 2, "climatological_mean", _ISSUED, 6, "pressure", 1012.0)
    _insert(conn, 200, "nws", _ISSUED, 6, "temperature", 22.0)

    rows = db.meta_inputs(conn, _ISSUED, _ISSUED, model_ids=(100, 202), model_types=("base",))

    by_model = {r["model_id"]: r for r in rows}
    assert set(by_model) == {100, 202, 1, 2}
    assert by_model[100]["spread"] == 1.5
    assert by_model[100]["confidence"] == 0.4
    assert by_model[202]["model"] == "external_corrected"
    assert by_model[1]["variable"] == "pressure"


def test_meta_inputs_works_with_either_argument_empty():
    conn = make_output_db()
    _insert(conn, 202, "external_corrected", _ISSUED, 6, "temperature", 21.0)
    _insert(conn, 1, "persistence", _ISSUED, 6, "temperature", 20.0)

    only_ids = db.meta_inputs(conn, _ISSUED, _ISSUED, model_ids=(202,))
    only_types = db.meta_inputs(conn, _ISSUED, _ISSUED, model_types=("base",))
    neither = db.meta_inputs(conn, _ISSUED, _ISSUED)

    assert [r["model_id"] for r in only_ids] == [202]
    assert [r["model_id"] for r in only_types] == [1]
    assert neither == []


def test_meta_inputs_honors_the_issued_range_and_member_zero_only():
    conn = make_output_db()
    _insert(conn, 202, "external_corrected", _ISSUED - 4 * 3600, 6, "temperature", 20.0)
    _insert(conn, 202, "external_corrected", _ISSUED, 6, "temperature", 21.0)
    _insert(conn, 202, "external_corrected", _ISSUED + 3600, 6, "temperature", 22.0)
    _insert(conn, 202, "external_corrected", _ISSUED, 6, "temperature", 23.0, member_id=3)

    rows = db.meta_inputs(conn, _ISSUED - 4 * 3600, _ISSUED, model_ids=(202,))

    assert sorted(r["value"] for r in rows) == [20.0, 21.0]


def test_member_zero_history_returns_scored_member_zero_rows_for_one_variable():
    conn = make_output_db()
    _insert(conn, 202, "external_corrected", _ISSUED, 6, "temperature", 21.0, error=1.0, scored=True, spread=0.5)
    _insert(conn, 202, "external_corrected", _ISSUED, 6, "dewpoint", 15.0, error=2.0, scored=True)
    _insert(conn, 202, "external_corrected", _ISSUED + 3600, 6, "temperature", 22.0)  # unscored
    _insert(conn, 202, "external_corrected", _ISSUED, 6, "temperature", 23.0, member_id=2, error=3.0, scored=True)
    _insert(conn, 200, "nws", _ISSUED, 6, "temperature", 24.0, error=4.0, scored=True)

    rows = db.member_zero_history(conn, 202, "temperature", 0)

    assert len(rows) == 1
    row = rows[0]
    assert (row["issued_at"], row["lead_hours"], row["error"], row["spread"]) == (_ISSUED, 6, 1.0, 0.5)
    assert row["valid_at"] == _ISSUED + 6 * 3600


def test_member_zero_history_honors_the_valid_at_cutoff():
    conn = make_output_db()
    _insert(conn, 202, "external_corrected", _ISSUED, 6, "temperature", 21.0, error=1.0, scored=True)
    _insert(conn, 202, "external_corrected", _ISSUED + 86400, 6, "temperature", 21.0, error=2.0, scored=True)

    rows = db.member_zero_history(conn, 202, "temperature", _ISSUED + 6 * 3600 + 1)

    assert [r["error"] for r in rows] == [2.0]
