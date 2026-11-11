import argparse

import barogram as bg
import db
from tests.test_cmd_forecast_staleness import _FakeConf, _make_output_db_on_disk


def _insert(conn, i, confidence, mae, variable="temperature"):
    conn.execute(
        """
        insert into forecasts (model_id, model, member_id, issued_at, valid_at, lead_hours, variable,
                               value, confidence, observed, error, mae, scored_at)
        values (1, 'persistence', 1, ?, ?, 6, ?, 20.0, ?, 20.0, 0.0, ?, 1)
        """,
        (1000 + i, 1000 + i + 6 * 3600, variable, confidence, mae),
    )


def _run(tmp_path, capsys, rows):
    out = _make_output_db_on_disk(tmp_path)
    conn = db.open_output_db(str(out))
    for i, (confidence, mae, *variable) in enumerate(rows):
        _insert(conn, i, confidence, mae, *variable)
    conn.close()
    bg.cmd_calibration(argparse.Namespace(), _FakeConf("unused", out))
    return capsys.readouterr().out


def test_report_gives_count_mean_stated_hit_rate_and_mae_per_band(tmp_path, capsys):
    # ten rows stated 0.85; nine landed inside the 1.11 band, one missed by 5
    rows = [(0.85, 0.5)] * 9 + [(0.85, 5.0)]
    out = _run(tmp_path, capsys, rows)

    line = next(l for l in out.splitlines() if l.strip().startswith("80-90%"))
    assert " 10 " in line and "0.850" in line and "0.900" in line and "0.950" in line   # mae (9*0.5+5)/10


def test_pressure_uses_its_own_hit_band(tmp_path, capsys):
    rows = [(0.25, 1.4, "pressure")] * 2 + [(0.25, 1.6, "pressure")] * 2   # 1.5 hPa band
    out = _run(tmp_path, capsys, rows)
    line = next(l for l in out.splitlines() if l.strip().startswith("20-30%"))
    assert "0.500" in line


def test_no_rows_with_confidence_prints_a_notice(tmp_path, capsys):
    out = _run(tmp_path, capsys, [])
    assert "no scored rows" in out
