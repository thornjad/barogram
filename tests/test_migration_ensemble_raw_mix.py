import shutil
import sqlite3
import time
from pathlib import Path

import pytest

import db

_MIGRATIONS = Path(__file__).resolve().parent.parent / "migrations"
_DAY = 86400


def _conn(tmp_path):
    for f in _MIGRATIONS.glob("[0-9][0-9][0-9]_*.sql"):
        if f.name < "069_":
            shutil.copy(f, tmp_path / f.name)
    conn = sqlite3.connect(":memory:", isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("pragma foreign_keys=on")
    db.run_migrations(conn, tmp_path)
    return conn


def _row(conn, member_id, issued_at, value, confidence=0.5, observed=None, pruned=False):
    scored = observed is not None
    conn.execute(
        """
        insert into forecasts
            (model_id, model, member_id, issued_at, valid_at, lead_hours, variable, value,
             confidence, observed, scored_at, error, mae)
        values (100, 'barogram_ensemble', ?, ?, ?, 6, 'temperature', ?, ?, ?, ?, ?, ?)
        """,
        (member_id, issued_at, issued_at + 6 * 3600, None if pruned else value,
         None if pruned else confidence, observed, issued_at + 7 * 3600 if scored else None,
         value - observed if scored else None, abs(value - observed) if scored else None),
    )


def _weight(conn, member_id, sector, weight):
    conn.execute(
        "insert into weights (model_id, member_id, variable, lead_hours, sector, weight, updated_at) "
        "values (100, ?, 'temperature', 6, ?, ?, 1)", (member_id, sector, weight))


def _apply(conn, tmp_path):
    shutil.copy(_MIGRATIONS / "069_ensemble_raw_mix_backfill.sql", tmp_path / "069_x.sql")
    db.run_migrations(conn, tmp_path)


def _seed_run(conn, issued_at, observed):
    # members 1 and 2 blend to 14; member 101 (the self-correction) and member 0 differ
    _row(conn, 1, issued_at, 10.0)
    _row(conn, 2, issued_at, 20.0)
    _row(conn, 101, issued_at, 50.0)
    _row(conn, 0, issued_at, 17.0, observed=observed)


def _sector_of(ts):
    from models._utils import _sector
    return _sector(ts)


def test_raw_blend_is_rebuilt_from_member_rows_without_the_self_correction(tmp_path):
    conn = _conn(tmp_path)
    issued = int(time.time()) - 5 * _DAY
    _seed_run(conn, issued, observed=15.0)
    s = _sector_of(issued + 6 * 3600)
    _weight(conn, 1, s, 0.25)
    _weight(conn, 2, s, 0.75)
    _weight(conn, 101, s, 0.5)

    _apply(conn, tmp_path)

    row = conn.execute("select * from forecasts where model_id = 100 and member_id = -1").fetchone()
    # 0.25 * 0.5 * 10 + 0.75 * 0.5 * 20 over 0.25 * 0.5 + 0.75 * 0.5
    assert row["value"] == pytest.approx((0.125 * 10 + 0.375 * 20) / 0.5)
    assert row["observed"] == 15.0
    assert row["error"] == pytest.approx(row["value"] - 15.0)
    assert row["mae"] == pytest.approx(abs(row["value"] - 15.0))
    assert row["scored_at"] is not None


def test_equal_mean_when_no_member_has_a_weight(tmp_path):
    conn = _conn(tmp_path)
    issued = int(time.time()) - 5 * _DAY
    _seed_run(conn, issued, observed=15.0)

    _apply(conn, tmp_path)

    assert conn.execute("select value from forecasts where member_id = -1").fetchone()[0] == 15.0


def test_runs_with_pruned_member_values_are_skipped(tmp_path):
    conn = _conn(tmp_path)
    old = int(time.time()) - 50 * _DAY
    _row(conn, 1, old, 10.0, pruned=True)
    _row(conn, 2, old, 20.0, pruned=True)
    _row(conn, 0, old, 17.0, observed=15.0)

    _apply(conn, tmp_path)

    assert conn.execute("select count(*) from forecasts where member_id = -1").fetchone()[0] == 0


def test_unscored_runs_get_a_row_that_can_be_scored_later(tmp_path):
    conn = _conn(tmp_path)
    issued = int(time.time()) - 3600
    _seed_run(conn, issued, observed=None)

    _apply(conn, tmp_path)

    row = conn.execute("select * from forecasts where member_id = -1").fetchone()
    assert row["observed"] is None and row["scored_at"] is None and row["error"] is None


def test_existing_raw_mix_rows_and_member_zero_are_left_alone(tmp_path):
    conn = _conn(tmp_path)
    issued = int(time.time()) - 5 * _DAY
    _seed_run(conn, issued, observed=15.0)
    _row(conn, -1, issued, 99.0)   # a row a live run already wrote for this cell
    before_zero = tuple(conn.execute("select * from forecasts where member_id = 0").fetchone())

    _apply(conn, tmp_path)

    assert conn.execute("select value from forecasts where member_id = -1").fetchone()[0] == 99.0
    assert tuple(conn.execute("select * from forecasts where member_id = 0").fetchone()) == before_zero
