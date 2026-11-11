import time

import db
import models._calibration as calibration
import score
from tests.conftest import make_input_db, make_output_db
from tests.test_score import _insert_obs

_DAY = 86400


def _seed(conn_in, conn_out, days, miss_every=None):
    now = int(time.time())
    for d in range(days):
        issued = now - (d + 2) * _DAY
        valid = issued + 6 * 3600
        _insert_obs(conn_in, valid, air_temp=20.0)
        miss = miss_every is not None and d % miss_every == 0
        conn_out.execute(
            """
            insert into forecasts (model_id, model, member_id, issued_at, valid_at, lead_hours,
                                   variable, value, conf_bin)
            values (1, 'persistence', 1, ?, ?, 6, 'temperature', ?, ?)
            """,
            (issued, valid, 30.0 if miss else 20.0, calibration.pack(1, 1, 5)),
        )


def test_labeled_rows_flow_from_scoring_into_a_calibrated_lookup():
    conn_in, conn_out = make_input_db(), make_output_db()
    _seed(conn_in, conn_out, days=16)

    assert score.run(conn_in, conn_out)["scored"] == 16
    table = calibration.build_table(*db.load_calib_counts(conn_out, since_day=0))

    value = table.lookup("temperature", 6, 1, 1, 5)
    assert value is not None
    assert abs(value - 17 / 18) < 1e-9          # 16 distinct days, every one a hit


def test_cell_stays_silent_until_it_has_enough_distinct_days():
    conn_in, conn_out = make_input_db(), make_output_db()
    _seed(conn_in, conn_out, days=calibration.MIN_DAYS - 1)

    score.run(conn_in, conn_out)
    table = calibration.build_table(*db.load_calib_counts(conn_out, since_day=0))

    assert table.lookup("temperature", 6, 1, 1, 5) is None


def test_misses_pull_the_measured_rate_down():
    conn_in, conn_out = make_input_db(), make_output_db()
    _seed(conn_in, conn_out, days=20, miss_every=2)    # half the days miss

    score.run(conn_in, conn_out)
    table = calibration.build_table(*db.load_calib_counts(conn_out, since_day=0))

    value = table.lookup("temperature", 6, 1, 1, 5)
    assert 0.4 < value < 0.6
