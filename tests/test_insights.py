import argparse
import json
import sys
from io import StringIO
from pathlib import Path

import pytest

import barogram as bg
from tests.conftest import make_output_db

_ISSUED = 1_700_000_000
_VALID_6 = _ISSUED + 6 * 3600
_VALID_24 = _ISSUED + 24 * 3600

_ENS_MODEL_ID = 100
_NWS_MODEL_ID = 200


def _args(fmt="json"):
    ns = argparse.Namespace(format=fmt)
    return ns


class _FakeConf:
    def __init__(self, output_db):
        self.output_db = str(output_db)
        self.input_db = "/nonexistent/input.db"


def _run_insights(conn, tmp_path, fmt="json"):
    """Write the in-memory DB to a temp file, run cmd_insights, capture stdout."""
    db_path = tmp_path / "barogram.db"
    import sqlite3
    disk = sqlite3.connect(str(db_path))
    conn.backup(disk)
    disk.close()

    conf = _FakeConf(db_path)
    args = _args(fmt)
    captured = StringIO()
    old_stdout = sys.stdout
    sys.stdout = captured
    try:
        bg.cmd_insights(args, conf)
    finally:
        sys.stdout = old_stdout
    return captured.getvalue().strip()


def _seed_ens_row(conn, var, value, spread=None, lead=6, scored=False):
    conn.execute(
        """
        insert into forecasts
            (model_id, model, member_id, issued_at, valid_at, lead_hours,
             variable, value, spread, observed, error, mae, scored_at)
        values (?, 'barogram_ensemble', 0, ?, ?, ?, ?, ?, ?,
                ?, ?, ?,
                ?)
        """,
        (
            _ENS_MODEL_ID, _ISSUED, _ISSUED + lead * 3600, lead,
            var, value, spread,
            value if scored else None,
            0.0 if scored else None,
            0.0 if scored else None,
            _ISSUED + 86400 if scored else None,
        ),
    )


def _seed_scored_row(conn, model_id, model_name, var, lead, mae, bias):
    conn.execute(
        """
        insert into forecasts
            (model_id, model, member_id, issued_at, valid_at, lead_hours,
             variable, value, observed, error, mae, scored_at)
        values (?, ?, 0, ?, ?, ?, ?, 0.0, 0.0, ?, ?, ?)
        """,
        (
            model_id, model_name, _ISSUED, _ISSUED + lead * 3600, lead,
            var, bias, mae, _ISSUED + 86400,
        ),
    )


# --- top-level keys ---

def test_top_level_keys_present(tmp_path):
    conn = make_output_db()
    _seed_ens_row(conn, "temperature", 20.0)
    _seed_scored_row(conn, _NWS_MODEL_ID, "nws", "temperature", 24, 1.0, 0.5)
    out = _run_insights(conn, tmp_path)
    data = json.loads(out)
    assert "generated_at" in data
    assert "n_scored_runs_alltime" in data
    assert "accuracy_window_runs" in data
    assert data["accuracy_window_runs"] == 10
    assert "ensemble_forecast" in data
    assert "model_accuracy" in data


# --- unit conversions ---

def test_temperature_absolute_conversion(tmp_path):
    conn = make_output_db()
    _seed_ens_row(conn, "temperature", 0.0)
    out = _run_insights(conn, tmp_path)
    data = json.loads(out)
    val = data["ensemble_forecast"]["leads"]["6"]["temperature"]
    assert abs(val - 32.0) < 0.01


def test_temperature_error_conversion_difference_only(tmp_path):
    conn = make_output_db()
    _seed_scored_row(conn, _NWS_MODEL_ID, "nws", "temperature", 24, 1.0, 0.0)
    out = _run_insights(conn, tmp_path)
    data = json.loads(out)
    mae = data["model_accuracy"]["nws"]["temperature"]["mae_24h"]
    assert abs(mae - 1.8) < 0.001, f"expected 1.8 (1.0 * 1.8), got {mae}"
    assert mae != pytest.approx(33.8), "must not add 32 to error values"



def test_pressure_unchanged(tmp_path):
    conn = make_output_db()
    _seed_ens_row(conn, "pressure", 1013.5)
    out = _run_insights(conn, tmp_path)
    data = json.loads(out)
    val = data["ensemble_forecast"]["leads"]["6"]["pressure"]
    assert abs(val - 1013.5) < 0.01


# --- missing data ---

def test_empty_db_returns_empty_object(tmp_path):
    conn = make_output_db()
    out = _run_insights(conn, tmp_path)
    assert out == "{}"


def test_missing_db_path_returns_empty_object(tmp_path):
    conf = _FakeConf(tmp_path / "nonexistent.db")
    args = _args()
    captured = StringIO()
    old_stdout = sys.stdout
    sys.stdout = captured
    try:
        bg.cmd_insights(args, conf)
    finally:
        sys.stdout = old_stdout
    assert captured.getvalue().strip() == "{}"


def test_no_scored_data_omits_model_accuracy(tmp_path):
    conn = make_output_db()
    # unscored row only
    conn.execute(
        """
        insert into forecasts
            (model_id, model, member_id, issued_at, valid_at, lead_hours,
             variable, value)
        values (200, 'nws', 0, ?, ?, 24, 'temperature', 20.0)
        """,
        (_ISSUED, _ISSUED + 86400),
    )
    out = _run_insights(conn, tmp_path)
    data = json.loads(out)
    assert data["model_accuracy"] == {}


def test_spread_omitted_when_null(tmp_path):
    conn = make_output_db()
    _seed_ens_row(conn, "temperature", 20.0, spread=None)
    out = _run_insights(conn, tmp_path)
    data = json.loads(out)
    lead = data["ensemble_forecast"]["leads"]["6"]
    assert "temperature_spread" not in lead


def test_spread_present_when_not_null(tmp_path):
    conn = make_output_db()
    _seed_ens_row(conn, "temperature", 20.0, spread=2.0)
    out = _run_insights(conn, tmp_path)
    data = json.loads(out)
    lead = data["ensemble_forecast"]["leads"]["6"]
    assert "temperature_spread" in lead
    assert abs(lead["temperature_spread"] - 2.0 * 1.8) < 0.01


def test_null_leads_for_missing_scored_data(tmp_path):
    conn = make_output_db()
    # only 24h scored data for nws
    _seed_scored_row(conn, _NWS_MODEL_ID, "nws", "temperature", 24, 1.0, 0.0)
    out = _run_insights(conn, tmp_path)
    data = json.loads(out)
    temp = data["model_accuracy"]["nws"]["temperature"]
    assert temp["mae_6h"] is None
    assert temp["mae_12h"] is None
    assert temp["mae_18h"] is None
    assert temp["mae_24h"] is not None


# --- table format ---

def test_table_format_nonempty(tmp_path):
    conn = make_output_db()
    _seed_ens_row(conn, "temperature", 20.0)
    _seed_scored_row(conn, _NWS_MODEL_ID, "nws", "temperature", 24, 1.0, 0.5)
    out = _run_insights(conn, tmp_path, fmt="table")
    assert len(out) > 0


def test_table_format_no_crash_empty(tmp_path):
    conn = make_output_db()
    # no forecast rows — should return "{}" via json, but table branch
    # shouldn't even be reached; verify no exception from missing DB
    conf = _FakeConf(tmp_path / "nonexistent.db")
    args = _args("table")
    captured = StringIO()
    old_stdout = sys.stdout
    sys.stdout = captured
    try:
        bg.cmd_insights(args, conf)
    finally:
        sys.stdout = old_stdout
    # empty DB prints {} regardless of format (early return before format check)
    assert captured.getvalue().strip() == "{}"
