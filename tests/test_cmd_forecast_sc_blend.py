import argparse
import sqlite3
import sys
from io import StringIO

import pytest

import barogram as bg
import db
from tests.test_cmd_forecast_staleness import (
    _FakeConf, _make_input_db_on_disk, _make_output_db_on_disk,
)

_MODEL_ID = 1
_SC = 900


class _FakeModel:
    MODEL_ID = _MODEL_ID
    MODEL_NAME = "fake_registered"
    NEEDS_WEIGHTS = True

    @staticmethod
    def run(obs, issued_at, **kwargs):
        valid_at = obs["timestamp"] + 6 * 3600
        base = {"model_id": _MODEL_ID, "model": "fake_registered", "issued_at": issued_at,
                "valid_at": valid_at, "lead_hours": 6, "variable": "temperature",
                "confidence": 0.5}
        return [
            {**base, "member_id": 1, "value": 19.0},
            {**base, "member_id": 2, "value": 21.0},
            {**base, "member_id": 0, "value": 20.0, "spread": 1.0},
            {**base, "member_id": _SC, "value": 18.0},
        ]


@pytest.fixture
def run_forecast(tmp_path, monkeypatch):
    monkeypatch.setattr(bg, "_sync_check", lambda: None)
    monkeypatch.setattr(bg.shutil, "which", lambda *_: None)
    monkeypatch.setattr(bg, "_MODELS", [_FakeModel])
    input_path = _make_input_db_on_disk(tmp_path, obs_age_sec=60)
    output_path = _make_output_db_on_disk(tmp_path)

    conn = db.open_output_db(str(output_path))
    conn.execute("insert or ignore into members (model_id, member_id, name) "
                 "values (?, -1, 'raw_mix')", (_MODEL_ID,))
    conn.execute("insert or ignore into members (model_id, member_id, name) "
                 "values (?, ?, 'self_correction')", (_MODEL_ID, _SC))
    weights = {(m, "temperature", 6, sector): w
               for sector in range(4) for m, w in ((1, 0.4), (2, 0.4), (_SC, 0.2))}
    db.save_weights(conn, _MODEL_ID, weights, 1)
    conn.close()

    def run():
        bg.cmd_forecast(argparse.Namespace(), _FakeConf(input_path, output_path))
        check = sqlite3.connect(str(output_path))
        check.row_factory = sqlite3.Row
        return {r["member_id"]: r["value"] for r in check.execute(
            "select member_id, value from forecasts where model_id = ?", (_MODEL_ID,))}

    return run


def test_registered_model_gets_a_raw_mix_row_and_a_blended_member_zero(run_forecast):
    stored = run_forecast()

    assert stored[db.RAW_MIX_ID] == pytest.approx(20.0)
    assert stored[0] == pytest.approx(0.8 * 20.0 + 0.2 * 18.0)
    assert stored[_SC] == pytest.approx(18.0)


def test_blend_failure_still_inserts_the_stage_one_rows(run_forecast, monkeypatch):
    def boom(*args, **kwargs):
        raise RuntimeError("blend exploded")

    monkeypatch.setattr(bg.sc_blend, "apply", boom)
    captured = StringIO()
    old_stderr, sys.stderr = sys.stderr, captured
    try:
        stored = run_forecast()
    finally:
        sys.stderr = old_stderr

    assert stored[0] == pytest.approx(20.0)
    assert db.RAW_MIX_ID not in stored
    assert "fake_registered (stage two)" in captured.getvalue()
