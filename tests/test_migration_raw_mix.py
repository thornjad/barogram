import shutil
import sqlite3
import time
from pathlib import Path

import db

_MIGRATIONS = Path(__file__).resolve().parent.parent / "migrations"
_COLUMNS = ("model_id", "model", "member_id", "issued_at", "valid_at", "lead_hours",
            "variable", "value", "spread", "confidence", "observed", "scored_at", "error", "mae")
_DAY = 86400


def _conn_before_068(tmp_path):
    for f in _MIGRATIONS.glob("[0-9][0-9][0-9]_*.sql"):
        if not f.name.startswith("068_"):
            shutil.copy(f, tmp_path / f.name)
    conn = sqlite3.connect(":memory:", isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("pragma foreign_keys=on")
    db.run_migrations(conn, tmp_path)
    return conn


def _insert(conn, model_id, member_id, issued_at, value, scored=True):
    conn.execute(
        """
        insert into forecasts
            (model_id, model, member_id, issued_at, valid_at, lead_hours, variable,
             value, spread, confidence, observed, scored_at, error, mae)
        values (?, 'm', ?, ?, ?, 6, 'temperature', ?, 0.5, 0.4, ?, ?, ?, ?)
        """,
        (model_id, member_id, issued_at, issued_at + 6 * 3600, value,
         15.0 if scored else None, issued_at + 7 * 3600 if scored else None,
         value - 15.0 if scored else None, abs(value - 15.0) if scored else None),
    )


def _rows(conn, member_id, model_id=None):
    where = "member_id = ?" + (" and model_id = ?" if model_id is not None else "")
    args = (member_id,) + ((model_id,) if model_id is not None else ())
    return conn.execute(
        f"select {', '.join(_COLUMNS)} from forecasts where {where} order by model_id, issued_at",
        args,
    ).fetchall()


def _apply_068(conn, tmp_path):
    shutil.copy(_MIGRATIONS / "068_raw_mix_member.sql", tmp_path / "068_raw_mix_member.sql")
    db.run_migrations(conn, tmp_path)


def _seed(conn, now):
    # model 4 and 5 have a self_correction member; model 1 (persistence) has none
    conn.execute("insert or ignore into members (model_id, member_id, name) "
                 "values (100, 101, 'ensemble_bias_correction')")
    for model_id in (4, 5, 1, 100):
        _insert(conn, model_id, 0, now - 10 * _DAY, 20.0)             # inside the 60 day window
        _insert(conn, model_id, 0, now - 12 * _DAY, 21.0, scored=False)
        _insert(conn, model_id, 0, now - 90 * _DAY, 22.0)             # outside the window
        _insert(conn, model_id, 7, now - 10 * _DAY, 19.0)             # a non-zero member


def test_models_with_a_self_correction_member_get_raw_mix_registered(tmp_path):
    conn = _conn_before_068(tmp_path)
    _seed(conn, int(time.time()))

    _apply_068(conn, tmp_path)

    registered = {r[0] for r in conn.execute(
        "select model_id from members where member_id = -1 and name = 'raw_mix'")}
    assert {4, 5, 100} <= registered
    assert 1 not in registered


def test_ensemble_member_101_is_renamed_self_correction(tmp_path):
    conn = _conn_before_068(tmp_path)
    _seed(conn, int(time.time()))

    _apply_068(conn, tmp_path)

    name = conn.execute(
        "select name from members where model_id = 100 and member_id = 101").fetchone()[0]
    assert name == "self_correction"


def test_raw_mix_copies_the_last_60_days_of_member_zero(tmp_path):
    conn = _conn_before_068(tmp_path)
    now = int(time.time())
    _seed(conn, now)

    _apply_068(conn, tmp_path)

    copied = _rows(conn, -1, 4)
    expected = [r for r in _rows(conn, 0, 4) if r["issued_at"] >= now - 60 * _DAY]
    assert len(copied) == 2
    for c, e in zip(copied, expected):
        assert tuple(c)[3:] == tuple(e)[3:]
        assert c["member_id"] == -1 and e["member_id"] == 0


def test_raw_mix_is_not_backfilled_for_the_ensemble_or_unregistered_models(tmp_path):
    conn = _conn_before_068(tmp_path)
    _seed(conn, int(time.time()))

    _apply_068(conn, tmp_path)

    assert _rows(conn, -1, 100) == []
    assert _rows(conn, -1, 1) == []


def test_original_rows_are_left_untouched(tmp_path):
    conn = _conn_before_068(tmp_path)
    _seed(conn, int(time.time()))
    before = [tuple(r) for r in _rows(conn, 0)] + [tuple(r) for r in _rows(conn, 7)]

    _apply_068(conn, tmp_path)

    after = [tuple(r) for r in _rows(conn, 0)] + [tuple(r) for r in _rows(conn, 7)]
    assert after == before


def test_applying_the_statements_again_adds_nothing(tmp_path):
    conn = _conn_before_068(tmp_path)
    _seed(conn, int(time.time()))
    _apply_068(conn, tmp_path)
    count = conn.execute("select count(*) from forecasts").fetchone()[0]
    members = conn.execute("select count(*) from members").fetchone()[0]

    conn.executescript((_MIGRATIONS / "068_raw_mix_member.sql").read_text())

    assert conn.execute("select count(*) from forecasts").fetchone()[0] == count
    assert conn.execute("select count(*) from members").fetchone()[0] == members
