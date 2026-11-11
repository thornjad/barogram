import shutil
import sqlite3
from pathlib import Path

import db

_MIGRATIONS = Path(__file__).resolve().parent.parent / "migrations"
_DAY = 86400


def _conn_before_070(tmp_path):
    for f in _MIGRATIONS.glob("[0-9][0-9][0-9]_*.sql"):
        if f.name < "070_":
            shutil.copy(f, tmp_path / f.name)
    conn = sqlite3.connect(":memory:", isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("pragma foreign_keys=on")
    db.run_migrations(conn, tmp_path)
    return conn


def test_070_applies_on_db_that_already_holds_forecast_rows(tmp_path):
    conn = _conn_before_070(tmp_path)
    conn.execute(
        """
        insert into forecasts
            (model_id, model, member_id, issued_at, valid_at, lead_hours, variable, value, confidence)
        values (1, 'persistence', 0, 1000, 4600, 1, 'temperature', 20.0, 0.5)
        """
    )
    shutil.copy(_MIGRATIONS / "070_confidence_calibration.sql", tmp_path / "070_confidence_calibration.sql")

    db.run_migrations(conn, tmp_path)

    cols = {r["name"] for r in conn.execute("pragma table_info(forecasts)")}
    assert "conf_bin" in cols
    row = conn.execute("select value, confidence, conf_bin from forecasts").fetchone()
    assert (row["value"], row["confidence"], row["conf_bin"]) == (20.0, 0.5, None)
    tables = {r["name"] for r in conn.execute("select name from sqlite_master where type = 'table'")}
    assert "calib_counts" in tables


def test_071_renames_the_tally_column_and_keeps_its_rows(tmp_path):
    for f in _MIGRATIONS.glob("[0-9][0-9][0-9]_*.sql"):
        if f.name < "071_":
            shutil.copy(f, tmp_path / f.name)
    conn = sqlite3.connect(":memory:", isolation_level=None)
    conn.row_factory = sqlite3.Row
    db.run_migrations(conn, tmp_path)
    conn.execute("insert into calib_counts (variable, lead_hours, n_bin, form_bin, score_bin, day, hits, total) "
                 "values ('temperature', 6, 1, 1, 4, 10, 3, 5)")
    shutil.copy(_MIGRATIONS / "071_calib_counts_raw_bin.sql", tmp_path / "071_calib_counts_raw_bin.sql")

    db.run_migrations(conn, tmp_path)

    cols = [r["name"] for r in conn.execute("pragma table_info(calib_counts)")]
    assert "raw_bin" in cols and "score_bin" not in cols
    row = conn.execute("select raw_bin, hits, total from calib_counts").fetchone()
    assert (row["raw_bin"], row["hits"], row["total"]) == (4, 3, 5)


def test_072_clears_aggregate_stamps_and_cells_but_nothing_else(tmp_path):
    for f in _MIGRATIONS.glob("[0-9][0-9][0-9]_*.sql"):
        if f.name < "072_":
            shutil.copy(f, tmp_path / f.name)
    conn = sqlite3.connect(":memory:", isolation_level=None)
    conn.row_factory = sqlite3.Row
    db.run_migrations(conn, tmp_path)
    recent, old = 1_762_800_000, 1_000_000
    for member, issued, conf_bin in ((0, recent, 104), (1, recent, 42), (0, old, 104)):
        conn.execute(
            "insert into forecasts (model_id, model, member_id, issued_at, valid_at, lead_hours, variable, "
            "value, conf_bin) values (1, 'm', ?, ?, ?, 6, 'temperature', 20.0, ?)",
            (member, issued, issued + 21600, conf_bin))
    conn.execute("insert into calib_counts values ('temperature', 6, 3, 1, 4, 10, 1, 1)")
    conn.execute("insert into calib_counts values ('temperature', 6, 1, 1, 4, 10, 1, 1)")
    shutil.copy(_MIGRATIONS / "072_drop_aggregate_calibration.sql", tmp_path / "072_drop_aggregate_calibration.sql")

    db.run_migrations(conn, tmp_path)

    stamps = {(r["member_id"], r["issued_at"]): r["conf_bin"] for r in conn.execute("select * from forecasts")}
    assert stamps == {(0, recent): None, (1, recent): 42, (0, old): 104}   # only recent aggregate stamps cleared
    assert [r["n_bin"] for r in conn.execute("select n_bin from calib_counts")] == [1]


def test_073_drops_the_tally_and_conf_bin_but_keeps_every_forecast(tmp_path):
    for f in _MIGRATIONS.glob("[0-9][0-9][0-9]_*.sql"):
        if f.name < "073_":
            shutil.copy(f, tmp_path / f.name)
    conn = sqlite3.connect(":memory:", isolation_level=None)
    conn.row_factory = sqlite3.Row
    db.run_migrations(conn, tmp_path)
    conn.execute("insert into forecasts (model_id, model, member_id, issued_at, valid_at, lead_hours, variable, "
                 "value, confidence, conf_bin) values (1, 'm', 0, 1000, 4600, 1, 'temperature', 20.0, 0.5, 42)")
    conn.execute("insert into calib_counts values ('temperature', 6, 1, 1, 4, 10, 1, 1)")
    shutil.copy(_MIGRATIONS / "073_remove_calibration_tally.sql", tmp_path / "073_remove_calibration_tally.sql")

    db.run_migrations(conn, tmp_path)

    assert "conf_bin" not in {r["name"] for r in conn.execute("pragma table_info(forecasts)")}
    assert "calib_counts" not in {r["name"] for r in conn.execute("select name from sqlite_master")}
    row = conn.execute("select value, confidence from forecasts").fetchone()
    assert (row["value"], row["confidence"]) == (20.0, 0.5)
