"""db.ensemble_skill_daily: 11:00 Central daily snapshots, trailing-window pooling."""
import datetime
from zoneinfo import ZoneInfo

import db
from tests.conftest import make_output_db

_CT = ZoneInfo("America/Chicago")


def _epoch(y, m, d, h):
    return int(datetime.datetime(y, m, d, h, tzinfo=_CT).timestamp())


def _seed(conn, issued_at, model_id, mae):
    conn.execute(
        "insert into forecasts (model_id, model, member_id, issued_at, valid_at, lead_hours,"
        " variable, value, mae, scored_at) values (?, 'm', 0, ?, ?, 1, 'temperature', 1.0, ?, ?)",
        (model_id, issued_at, issued_at + 3600, mae, issued_at + 7200),
    )


def test_window_pools_and_snapshot_cutoff():
    conn = make_output_db()
    # climo mae 2.0, ensemble mae 1.0 -> 50% skill, one run per day at 10:00 CT
    for day in range(1, 6):
        t = _epoch(2026, 1, day, 10)
        _seed(conn, t, 2, 2.0)
        _seed(conn, t, 100, 1.0)
    # run after the 11:00 snapshot on Jan 5 belongs to Jan 6 -> excluded when now is Jan 5 12:00
    t = _epoch(2026, 1, 5, 12)
    _seed(conn, t, 2, 2.0)
    _seed(conn, t, 100, 2.0)
    out = db.ensemble_skill_daily(conn, _epoch(2026, 1, 5, 12), windows=(3,), key_leads=(1,))
    assert out["days"][0] == "2026-01-01" and out["days"][-1] == "2026-01-05"
    s = out["series"][3]
    assert s[:2] == [None, None]  # window not yet full
    assert s[2:] == [50.0, 50.0, 50.0]


def test_before_snapshot_uses_yesterday():
    conn = make_output_db()
    for day in (1, 2):
        t = _epoch(2026, 1, day, 10)
        _seed(conn, t, 2, 2.0)
        _seed(conn, t, 100, 1.0)
    out = db.ensemble_skill_daily(conn, _epoch(2026, 1, 2, 10), windows=(1,), key_leads=(1,))
    assert out["days"][-1] == "2026-01-01"
