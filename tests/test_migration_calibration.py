import shutil
import sqlite3
from pathlib import Path

import db
from tests.conftest import make_output_db

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


def test_insert_forecasts_stores_conf_bin_and_defaults_to_null():
    conn = make_output_db()
    base = {"model_id": 1, "model": "persistence", "member_id": 0, "issued_at": 1000,
            "valid_at": 4600, "lead_hours": 1, "variable": "temperature", "value": 20.0}
    db.insert_forecasts(conn, [{**base, "conf_bin": 42}, {**base, "variable": "dewpoint", "value": 10.0}])

    rows = {r["variable"]: r["conf_bin"] for r in conn.execute("select variable, conf_bin from forecasts")}
    assert rows == {"temperature": 42, "dewpoint": None}


def _counts(conn, rows):
    conn.executemany(
        "insert into calib_counts (variable, lead_hours, n_bin, form_bin, score_bin, day, hits, total) "
        "values (?, ?, ?, ?, ?, ?, ?, ?)", rows)


def test_load_calib_counts_averages_per_day_and_counts_distinct_days():
    conn = make_output_db()
    _counts(conn, [
        # day 10: 1 of 1 hit; day 11: 0 of 99 -> per-day mean is 0.5, not 1/100
        ("temperature", 6, 1, 1, 5, 10, 1, 1),
        ("temperature", 6, 1, 1, 5, 11, 0, 99),
    ])
    fine, coarse = db.load_calib_counts(conn, since_day=0)
    days, p_hat = fine[("temperature", 6, 1, 1, 5)]
    assert days == 2
    assert abs(p_hat - 0.5) < 1e-9


def test_load_calib_counts_coarse_sums_leaf_bins_per_day_and_excludes_aggregates():
    conn = make_output_db()
    _counts(conn, [
        ("temperature", 6, 0, 0, 5, 10, 1, 2),   # leaf, day 10
        ("temperature", 6, 2, 1, 5, 10, 3, 4),   # leaf, same day: summed to 4 of 6
        ("temperature", 6, 3, 1, 5, 10, 9, 9),   # aggregate: never in coarse
    ])
    fine, coarse = db.load_calib_counts(conn, since_day=0)
    days, p_hat = coarse[("temperature", 6, 5)]
    assert days == 1
    assert abs(p_hat - 4 / 6) < 1e-9
    assert ("temperature", 6, 3, 1, 5) in fine


def test_load_calib_counts_window_drops_older_days():
    conn = make_output_db()
    _counts(conn, [
        ("temperature", 6, 1, 1, 5, 5, 1, 1),
        ("temperature", 6, 1, 1, 5, 400, 1, 1),
    ])
    fine, _ = db.load_calib_counts(conn, since_day=100)
    assert fine[("temperature", 6, 1, 1, 5)][0] == 1
