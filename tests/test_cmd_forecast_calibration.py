import argparse
import sqlite3
import time

import pytest

import barogram as bg
import db
import models._calibration as calibration
import models._confidence as confidence
from tests.test_cmd_forecast_staleness import (
    _FakeConf, _make_input_db_on_disk, _make_output_db_on_disk,
)

_MODEL_ID = 1
_DAY = 86400


def _row(member_id, confidence_value, value=20.0, variable="temperature", lead=6):
    return {"model_id": _MODEL_ID, "member_id": member_id, "variable": variable, "lead_hours": lead,
            "value": value, "confidence": confidence_value}


class _Table:
    """Stands in for a CalibrationTable: returns a fixed rate for aggregate cells."""
    def __init__(self, rate):
        self.rate = rate
        self.calls = []

    def lookup(self, variable, lead_hours, n_bin, form_bin, score_bin):
        self.calls.append((variable, lead_hours, n_bin, form_bin, score_bin))
        return self.rate


# --- _stamp_calibration (pure) ---

def test_leaf_row_whose_confidence_matches_the_log_gets_its_bin():
    rows = [_row(3, 0.30)]
    log = {(_MODEL_ID, 3, "temperature", 6): (42, 0.30)}
    leaf, agg = bg._stamp_calibration(rows, log, _Table(None))
    assert (leaf, agg) == (1, 0)
    assert rows[0]["conf_bin"] == 42
    assert rows[0]["confidence"] == 0.30


def test_leaf_row_whose_confidence_changed_after_logging_stays_unlabeled():
    rows = [_row(3, 0.31)]
    log = {(_MODEL_ID, 3, "temperature", 6): (42, 0.30)}
    leaf, agg = bg._stamp_calibration(rows, log, _Table(None))
    assert (leaf, agg) == (0, 0)
    assert "conf_bin" not in rows[0]


def test_row_without_a_log_entry_and_not_member_zero_stays_unlabeled():
    rows = [_row(5, 0.30), _row(db.RAW_MIX_ID, 0.30)]
    leaf, agg = bg._stamp_calibration(rows, {}, _Table(0.9))
    assert (leaf, agg) == (0, 0)
    assert all("conf_bin" not in r for r in rows)


def test_member_zero_without_log_entry_is_an_aggregate_and_gets_recalibrated():
    rows = [_row(0, 0.45)]
    table = _Table(0.77)
    leaf, agg = bg._stamp_calibration(rows, {}, table)
    assert (leaf, agg) == (0, 1)
    assert rows[0]["conf_bin"] == calibration.aggregate_bin(4)
    assert rows[0]["confidence"] == 0.77
    assert table.calls == [("temperature", 6, calibration.AGGREGATE_N_BIN, calibration.AGGREGATE_FORM_BIN, 4)]


def test_aggregate_keeps_group_confidence_while_its_cell_is_inactive():
    rows = [_row(0, 0.45)]
    bg._stamp_calibration(rows, {}, _Table(None))
    assert rows[0]["confidence"] == 0.45
    assert rows[0]["conf_bin"] == calibration.aggregate_bin(4)


def test_leaf_member_zero_matching_the_log_is_labeled_as_a_leaf_not_an_aggregate():
    rows = [_row(0, 0.30)]
    log = {(_MODEL_ID, 0, "temperature", 6): (17, 0.30)}
    leaf, agg = bg._stamp_calibration(rows, log, _Table(0.99))
    assert (leaf, agg) == (1, 0)
    assert rows[0]["conf_bin"] == 17 and rows[0]["confidence"] == 0.30


def test_member_zero_rewritten_after_logging_is_treated_as_an_aggregate():
    """sc_blend replaces member 0's confidence with a group value; the leaf log entry no
    longer matches, so the row is a blend and is calibrated as one."""
    rows = [_row(0, 0.55)]
    log = {(_MODEL_ID, 0, "temperature", 6): (17, 0.30)}
    leaf, agg = bg._stamp_calibration(rows, log, _Table(None))
    assert (leaf, agg) == (0, 1)
    assert rows[0]["conf_bin"] == calibration.aggregate_bin(5)


def test_member_zero_without_value_or_confidence_is_skipped():
    rows = [_row(0, 0.5, value=None), _row(0, None)]
    assert bg._stamp_calibration(rows, {}, _Table(0.9)) == (0, 0)


def test_lookup_error_on_an_aggregate_keeps_the_group_confidence():
    class Broken:
        def lookup(self, *a):
            raise RuntimeError("boom")

    rows = [_row(0, 0.45)]
    assert bg._stamp_calibration(rows, {}, Broken()) == (0, 1)
    assert rows[0]["confidence"] == 0.45


# --- through cmd_forecast ---

class _FakeModel:
    MODEL_ID = _MODEL_ID
    MODEL_NAME = "fake_calibrated"
    NEEDS_MATCH_HISTORY = True
    matched_ts = None   # set by the test

    @classmethod
    def run(cls, obs, issued_at, **kwargs):
        valid_at = obs["timestamp"] + 6 * 3600
        base = {"model_id": _MODEL_ID, "model": cls.MODEL_NAME, "issued_at": issued_at,
                "valid_at": valid_at, "lead_hours": 6, "variable": "temperature"}
        leaf_conf = confidence.confidence_for_cell(
            kwargs["member_history"][1], "temperature", 6, kwargs["default_matches"])
        return [
            {**base, "member_id": 1, "value": 19.0, "confidence": leaf_conf},
            {**base, "member_id": 2, "value": 21.0, "confidence": 0.5},   # not via confidence_for_cell
            {**base, "member_id": 0, "value": 20.0, "spread": 1.0, "confidence": 0.45},
        ]


@pytest.fixture
def forecast_with_history(tmp_path, monkeypatch):
    monkeypatch.setattr(bg, "_sync_check", lambda: None)
    monkeypatch.setattr(bg.shutil, "which", lambda *_: None)
    monkeypatch.setattr(bg, "_MODELS", [_FakeModel])
    input_path = _make_input_db_on_disk(tmp_path, obs_age_sec=60)
    output_path = _make_output_db_on_disk(tmp_path)

    now = int(time.time())
    matched = now - 5 * _DAY
    conn = db.open_output_db(str(output_path))
    conn.execute("insert or ignore into members (model_id, member_id, name) values (?, 1, 'a')", (_MODEL_ID,))
    conn.execute("insert or ignore into members (model_id, member_id, name) values (?, 2, 'b')", (_MODEL_ID,))
    for d in range(1, 12):
        issued = now - d * _DAY if d != 5 else matched
        conn.execute(
            """
            insert into forecasts (model_id, model, member_id, issued_at, valid_at, lead_hours, variable,
                                   value, observed, error, mae, scored_at)
            values (?, 'fake_calibrated', 1, ?, ?, 6, 'temperature', 20.0, 20.0, 0.0, 2.0, ?)
            """, (_MODEL_ID, issued, issued + 6 * 3600, issued + 7 * 3600))
    conn.close()

    monkeypatch.setattr(bg._confidence, "find_default_matches", lambda conn_in, ts: [matched + 3600])
    monkeypatch.setattr(bg._confidence, "matched_day_spreads", lambda conn_in, m: {("temperature", 6): 6.0})

    def run():
        bg.cmd_forecast(argparse.Namespace(), _FakeConf(input_path, output_path))
        check = sqlite3.connect(str(output_path))
        check.row_factory = sqlite3.Row
        return {r["member_id"]: r for r in check.execute(
            "select member_id, confidence, conf_bin from forecasts "
            "where model_id = ? and issued_at = (select max(issued_at) from forecasts where model_id = ?)",
            (_MODEL_ID, _MODEL_ID))}

    return run


def test_forecast_stamps_leaf_rows_and_aggregate_rows_only(forecast_with_history, capsys):
    stored = forecast_with_history()

    assert stored[1]["conf_bin"] is not None and stored[1]["conf_bin"] < 100      # computed leaf
    assert stored[2]["conf_bin"] is None                                           # never went through confidence_for_cell
    assert stored[0]["conf_bin"] == calibration.aggregate_bin(4)                   # aggregate, group conf 0.45
    out = capsys.readouterr().out
    assert "labeled 1" in out and "aggregate 1" in out


def test_forecast_recalibrates_the_aggregate_once_its_cell_is_active(forecast_with_history, tmp_path):
    # the tally says: aggregate confidence bin 4, lead 6 temperature was right ~60% over 40 days
    conn = db.open_output_db(str(tmp_path / "barogram.db"))
    today = int(time.time()) // _DAY
    for d in range(40):
        conn.execute("insert into calib_counts values ('temperature', 6, 3, 1, 4, ?, 6, 10)", (today - 5 - d,))
    conn.close()

    stored = forecast_with_history()

    assert stored[0]["confidence"] == pytest.approx((0.6 * 40 + 1) / 42)


def test_row_without_member_id_is_treated_as_member_zero():
    """Single-member models may omit member_id; db.insert_forecasts defaults it to 0, so
    stamping must too (climatological_mean crashed here before)."""
    leaf = {"model_id": _MODEL_ID, "variable": "temperature", "lead_hours": 6, "value": 20.0, "confidence": 0.30}
    agg = {"model_id": _MODEL_ID, "variable": "dewpoint", "lead_hours": 6, "value": 10.0, "confidence": 0.45}
    log = {(_MODEL_ID, 0, "temperature", 6): (17, 0.30)}

    leaf_n, agg_n = bg._stamp_calibration([leaf, agg], log, _Table(None))

    assert (leaf_n, agg_n) == (1, 1)
    assert leaf["conf_bin"] == 17
    assert agg["conf_bin"] == calibration.aggregate_bin(4)


def test_leaf_member_zero_with_no_evidence_keeps_zero_and_is_not_an_aggregate():
    """persistence-style member 0: the leaf had nothing to judge (logged without a bin), so
    its 0.0 must survive an active aggregate cell and stay out of the tally."""
    rows = [_row(0, 0.0)]
    log = {(_MODEL_ID, 0, "temperature", 6): (None, 0.0)}
    table = _Table(0.2)
    assert bg._stamp_calibration(rows, log, table) == (0, 0)
    assert rows[0]["confidence"] == 0.0
    assert "conf_bin" not in rows[0]
    assert table.calls == []


def test_aggregate_lookup_error_is_reported_once_and_keeps_group_confidence(capsys):
    class Broken:
        def lookup(self, *a):
            raise RuntimeError("boom")

    confidence.set_calibration(None)   # resets the once-per-run report flag
    rows = [_row(0, 0.45), _row(0, 0.55, variable="dewpoint")]
    bg._stamp_calibration(rows, {}, Broken())
    assert [r["confidence"] for r in rows] == [0.45, 0.55]
    assert capsys.readouterr().err.count("calibration lookup failed") == 1
