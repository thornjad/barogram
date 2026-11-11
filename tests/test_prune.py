import time

import db
from tests.conftest import make_output_db

_NOW = int(time.time())
_OLD = _NOW - 40 * 86400   # past the 30-day default cutoff
_RECENT = _NOW - 5 * 86400  # inside the 30-day default cutoff


def _insert(conn, valid_at, scored_at, value=20.0, spread=1.0, observed=18.5, confidence=0.7):
    conn.execute(
        """
        insert into forecasts
            (model_id, model, member_id, issued_at, valid_at, lead_hours, variable,
             value, spread, observed, confidence, scored_at, error, mae)
        values (1, 'persistence', 0, ?, ?, 6, 'temperature', ?, ?, ?, ?, ?, 1.5, 1.5)
        """,
        (valid_at - 3600, valid_at, value, spread, observed, confidence, scored_at),
    )
    return conn.execute("select last_insert_rowid()").fetchone()[0]


def _row(conn, row_id):
    return conn.execute(
        "select value, spread, observed, confidence, error, mae, scored_at from forecasts where id = ?",
        (row_id,),
    ).fetchone()


def test_prunes_old_scored_row():
    conn = make_output_db()
    row_id = _insert(conn, _OLD, scored_at=_OLD)

    n = db.prune_old_forecast_details(conn, _NOW - 30 * 86400)

    assert n == 1
    row = _row(conn, row_id)
    assert row["value"] is None
    assert row["spread"] is None
    assert row["observed"] is None
    assert row["confidence"] is None
    # error/mae/scored_at are never touched
    assert row["error"] == 1.5
    assert row["mae"] == 1.5
    assert row["scored_at"] == _OLD


def test_prunes_row_where_only_confidence_remains():
    """value/spread/observed already pruned, but confidence wasn't yet — the
    where-guard must include confidence, not just the null-out list, or this
    row would never be revisited."""
    conn = make_output_db()
    row_id = _insert(conn, _OLD, scored_at=_OLD, value=None, spread=None, observed=None, confidence=0.7)

    n = db.prune_old_forecast_details(conn, _NOW - 30 * 86400)

    assert n == 1
    row = _row(conn, row_id)
    assert row["confidence"] is None


def test_leaves_recent_row_alone():
    conn = make_output_db()
    row_id = _insert(conn, _RECENT, scored_at=_RECENT)

    n = db.prune_old_forecast_details(conn, _NOW - 30 * 86400)

    assert n == 0
    row = _row(conn, row_id)
    assert row["value"] == 20.0
    assert row["spread"] == 1.0
    assert row["observed"] == 18.5


def test_leaves_unscored_row_alone_even_if_old():
    """An old, never-scored row is presumed still pending, not dead."""
    conn = make_output_db()
    row_id = _insert(conn, _OLD, scored_at=None)

    n = db.prune_old_forecast_details(conn, _NOW - 30 * 86400)

    assert n == 0
    row = _row(conn, row_id)
    assert row["value"] == 20.0
    assert row["observed"] == 18.5


def test_already_pruned_row_not_recounted():
    conn = make_output_db()
    row_id = _insert(conn, _OLD, scored_at=_OLD)
    db.prune_old_forecast_details(conn, _NOW - 30 * 86400)

    n = db.prune_old_forecast_details(conn, _NOW - 30 * 86400)

    assert n == 0
    row = _row(conn, row_id)
    assert row["value"] is None


def test_incremental_vacuum_runs_without_error():
    conn = make_output_db()
    db.incremental_vacuum(conn)
    db.incremental_vacuum(conn, pages=10)
