import argparse
import sqlite3
import time

import pytest

import barogram as bg
import db
import models._confidence as confidence
from tests.test_cmd_forecast_staleness import (
    _FakeConf, _make_input_db_on_disk, _make_output_db_on_disk,
)

_MODEL_ID = 1
_DAY = 86400


def _row(member_id, confidence_value, value=20.0, variable="temperature", lead=6):
    return {"model_id": _MODEL_ID, "member_id": member_id, "variable": variable, "lead_hours": lead,
            "value": value, "confidence": confidence_value}


def _history(err=2.0):
    base = 10_000_000
    base -= base % _DAY
    matched = base + 5 * _DAY
    rows = [{"variable": "temperature", "lead_hours": 6, "issued_at": base + d * _DAY, "mae": 10.0}
            for d in range(10) if d != 5]
    rows.append({"variable": "temperature", "lead_hours": 6, "issued_at": matched + 3600, "mae": err})
    return confidence.CellHistory(rows), matched


# --- _member_zero_confidence ---

def test_member_zero_gets_its_own_confidence_not_the_members_average():
    hist, matched = _history()
    rows = [_row(0, 0.45)]
    bg._member_zero_confidence(rows, {0: hist}, [matched])
    own = confidence.confidence_for_cell(_history()[0], "temperature", 6, [matched])
    assert rows[0]["confidence"] == own != 0.45


def test_member_zero_with_no_history_of_its_own_gets_zero_not_the_average():
    rows = [_row(0, 0.45)]
    bg._member_zero_confidence(rows, {}, [123])
    assert rows[0]["confidence"] == 0.0


def test_member_zero_follows_its_own_record_not_the_other_members():
    good, matched = _history(err=0.1)
    bad, _ = _history(err=30.0)
    rows_good, rows_bad = [_row(0, 0.5)], [_row(0, 0.5)]
    bg._member_zero_confidence(rows_good, {0: good, 1: bad}, [matched])
    bg._member_zero_confidence(rows_bad, {0: bad, 1: good}, [matched])
    assert rows_good[0]["confidence"] > rows_bad[0]["confidence"]


def test_other_members_and_valueless_rows_are_untouched():
    hist, matched = _history()
    rows = [_row(3, 0.45), _row(0, 0.45, value=None)]
    bg._member_zero_confidence(rows, {0: hist}, [matched])
    assert [r["confidence"] for r in rows] == [0.45, 0.45]


def test_row_without_member_id_is_treated_as_member_zero():
    """single-member models may omit member_id; db.insert_forecasts defaults it to 0."""
    hist, matched = _history()
    row = {"model_id": _MODEL_ID, "variable": "temperature", "lead_hours": 6, "value": 20.0, "confidence": 0.9}
    bg._member_zero_confidence([row], {0: hist}, [matched])
    assert row["confidence"] == confidence.confidence_for_cell(hist, "temperature", 6, [matched])


def test_models_without_member_history_are_skipped():
    rows = [_row(0, 0.45)]
    bg._member_zero_confidence(rows, None, [123])
    assert rows[0]["confidence"] == 0.45


# --- through cmd_forecast ---

class _FakeModel:
    MODEL_ID = _MODEL_ID
    MODEL_NAME = "fake_confident"
    NEEDS_MATCH_HISTORY = True

    @classmethod
    def run(cls, obs, issued_at, **kwargs):
        valid_at = obs["timestamp"] + 6 * 3600
        base = {"model_id": _MODEL_ID, "model": cls.MODEL_NAME, "issued_at": issued_at,
                "valid_at": valid_at, "lead_hours": 6, "variable": "temperature"}
        leaf_conf = confidence.confidence_for_cell(
            kwargs["member_history"][1], "temperature", 6, kwargs["default_matches"])
        return [
            {**base, "member_id": 1, "value": 19.0, "confidence": leaf_conf},
            {**base, "member_id": 0, "value": 20.0, "spread": 1.0, "confidence": 0.45},   # the members' average
        ]


class _FakeStatesOwn(_FakeModel):
    MODEL_ID = 1
    STATES_OWN_MEMBER_ZERO_CONFIDENCE = True


@pytest.fixture
def forecast_with_history(tmp_path, monkeypatch):
    monkeypatch.setattr(bg, "_sync_check", lambda: None)
    monkeypatch.setattr(bg.shutil, "which", lambda *_: None)
    input_path = _make_input_db_on_disk(tmp_path, obs_age_sec=60)
    output_path = _make_output_db_on_disk(tmp_path)

    now = int(time.time())
    matched = now - 5 * _DAY
    conn = db.open_output_db(str(output_path))
    conn.execute("insert or ignore into members (model_id, member_id, name) values (?, 1, 'm')", (_MODEL_ID,))
    for mid in (0, 1):   # member 0 has its own scored history, the same shape as member 1's
        for d in range(1, 12):
            issued = now - d * _DAY if d != 5 else matched
            conn.execute(
                """
                insert into forecasts (model_id, model, member_id, issued_at, valid_at, lead_hours, variable,
                                       value, observed, error, mae, scored_at)
                values (?, 'fake_confident', ?, ?, ?, 6, 'temperature', 20.0, 20.0, 0.0, 2.0, ?)
                """, (_MODEL_ID, mid, issued, issued + 6 * 3600, issued + 7 * 3600))
    conn.close()

    monkeypatch.setattr(bg._confidence, "find_default_matches", lambda conn_in, ts: [(matched + 3600, 1.0)])
    monkeypatch.setattr(bg._confidence, "matched_day_spreads", lambda conn_in, m: {("temperature", 6): 6.0})

    def run(model):
        monkeypatch.setattr(bg, "_MODELS", [model])
        bg.cmd_forecast(argparse.Namespace(), _FakeConf(input_path, output_path))
        check = sqlite3.connect(str(output_path))
        check.row_factory = sqlite3.Row
        return {r["member_id"]: r["confidence"] for r in check.execute(
            "select member_id, confidence from forecasts "
            "where model_id = ? and issued_at = (select max(issued_at) from forecasts where model_id = ?)",
            (_MODEL_ID, _MODEL_ID))}

    return run


def test_forecast_gives_member_zero_its_own_confidence(forecast_with_history):
    stored = forecast_with_history(_FakeModel)
    assert stored[0] != 0.45                 # not the members' average
    assert stored[0] == stored[1]            # same history shape, same method


def test_a_model_that_states_its_own_member_zero_confidence_is_left_alone(forecast_with_history):
    assert forecast_with_history(_FakeStatesOwn)[0] == 0.45
