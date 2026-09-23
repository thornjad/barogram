import argparse
import sqlite3
import sys
import time
from io import StringIO

import pytest

import barogram as bg
from tests.conftest import make_input_db, make_output_db


@pytest.fixture(autouse=True)
def _no_real_notifications(monkeypatch):
    """Never let a test spawn the real terminal-notifier -- it fires an actual
    macOS notification on whatever machine runs the suite, regardless of the
    fabricated obs age being tested."""
    monkeypatch.setattr(bg.shutil, "which", lambda *_: None)


def _make_input_db_on_disk(tmp_path, obs_age_sec: int):
    """On-disk wxlog-shaped DB (conftest's schema) with one obs row aged obs_age_sec.

    conftest.make_input_db()'s stations table lacks elevation/agl, which
    db.validate_schema() requires -- added here rather than in the shared
    fixture since no other test goes through validate_schema.
    """
    conn = make_input_db()
    conn.execute("alter table stations add column elevation real")
    conn.execute("alter table stations add column agl real")
    conn.execute(
        "update stations set elevation = 300.0, agl = 2.0 where station_id = 'KTEST'"
    )
    conn.execute(
        "insert into tempest_obs (station_id, timestamp, air_temp, dew_point, "
        "station_pressure, wind_avg, wind_gust, wind_direction, precip_accum_day, "
        "solar_radiation, uv_index, lightning_count) "
        "values ('KTEST', ?, 10.0, 5.0, 1013.0, 2.0, 3.0, 180.0, 0.0, 0.0, 0.0, 0)",
        (int(time.time()) - obs_age_sec,),
    )
    conn.commit()

    db_path = tmp_path / "wxlog.db"
    disk = sqlite3.connect(str(db_path))
    conn.backup(disk)
    disk.close()
    return db_path


def _make_output_db_on_disk(tmp_path):
    output_path = tmp_path / "barogram.db"
    disk = sqlite3.connect(str(output_path))
    make_output_db().backup(disk)
    disk.close()
    return output_path


class _FakeConf:
    def __init__(self, input_db, output_db):
        self.input_db = str(input_db)
        self.output_db = str(output_db)


def test_cmd_forecast_aborts_on_stale_obs(tmp_path, monkeypatch):
    input_path = _make_input_db_on_disk(tmp_path, obs_age_sec=2 * 3600)
    output_path = _make_output_db_on_disk(tmp_path)

    monkeypatch.setattr(bg, "_sync_check", lambda: None)
    conf = _FakeConf(input_path, output_path)

    captured = StringIO()
    old_stderr = sys.stderr
    sys.stderr = captured
    try:
        with pytest.raises(SystemExit) as exc_info:
            bg.cmd_forecast(argparse.Namespace(), conf)
    finally:
        sys.stderr = old_stderr

    assert exc_info.value.code != 0
    assert "old" in captured.getvalue().lower()

    check = sqlite3.connect(str(output_path))
    assert check.execute("select count(*) from forecasts").fetchone()[0] == 0


def test_cmd_forecast_runs_on_fresh_obs(tmp_path, monkeypatch):
    input_path = _make_input_db_on_disk(tmp_path, obs_age_sec=60)
    output_path = _make_output_db_on_disk(tmp_path)

    monkeypatch.setattr(bg, "_sync_check", lambda: None)
    conf = _FakeConf(input_path, output_path)

    bg.cmd_forecast(argparse.Namespace(), conf)

    check = sqlite3.connect(str(output_path))
    assert check.execute("select count(*) from forecasts").fetchone()[0] > 0
