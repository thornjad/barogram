import argparse

import barogram as bg
import db
from tests.test_cmd_forecast_staleness import _FakeConf, _make_output_db_on_disk


def _insert(conn, i, confidence, mae, conf_bin):
    conn.execute(
        """
        insert into forecasts (model_id, model, member_id, issued_at, valid_at, lead_hours, variable,
                               value, confidence, observed, error, mae, scored_at, conf_bin)
        values (1, 'persistence', 1, ?, ?, 6, 'temperature', 20.0, ?, 20.0, 0.0, ?, 1, ?)
        """,
        (1000 + i, 1000 + i + 6 * 3600, confidence, mae, conf_bin),
    )


def _run(tmp_path, capsys, rows):
    out = _make_output_db_on_disk(tmp_path)
    conn = db.open_output_db(str(out))
    for i, (confidence, mae, conf_bin) in enumerate(rows):
        _insert(conn, i, confidence, mae, conf_bin)
    conn.close()
    bg.cmd_calibration(argparse.Namespace(), _FakeConf("unused", out))
    return capsys.readouterr().out


def test_labeled_rows_report_stated_confidence_against_observed_hit_rate(tmp_path, capsys):
    # ten labeled rows stated 0.85; nine landed inside the 1.11 band, one missed
    rows = [(0.85, 0.5, 42)] * 9 + [(0.85, 5.0, 42)]
    out = _run(tmp_path, capsys, rows)

    line = next(l for l in out.splitlines() if l.strip().startswith("80-90%"))
    assert " 10 " in line and "0.850" in line and "0.900" in line


def test_unlabeled_rows_are_counted_separately_with_no_hit_rate(tmp_path, capsys):
    rows = [(0.25, 0.5, None)] * 5 + [(0.85, 0.5, 42)]
    out = _run(tmp_path, capsys, rows)

    unlabeled = out.split("unlabeled", 1)[1]
    line = next(l for l in unlabeled.splitlines() if l.strip().startswith("20-30%"))
    assert " 5" in line
    assert "0.900" not in line and "hit" not in line


def test_no_rows_with_confidence_prints_a_notice(tmp_path, capsys):
    out = _run(tmp_path, capsys, [])
    assert "no scored rows" in out
