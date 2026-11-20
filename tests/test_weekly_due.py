import datetime

import barogram as bg


def _ts(y, m, d, h=0, mi=0):
    return datetime.datetime(y, m, d, h, mi).timestamp()


# 2026-11-16 is a monday
MON_0900 = _ts(2026, 11, 16, 9)
NEXT_MON_0900 = _ts(2026, 11, 23, 9)


def test_anchor_is_this_monday_after_nine():
    assert bg._weekly_anchor(_ts(2026, 11, 16, 11, 6)) == int(MON_0900)


def test_anchor_is_previous_monday_before_nine():
    assert bg._weekly_anchor(_ts(2026, 11, 16, 8, 59)) == int(_ts(2026, 11, 9, 9))


def test_anchor_midweek():
    assert bg._weekly_anchor(_ts(2026, 11, 19, 15)) == int(MON_0900)


def test_never_run_is_due():
    assert bg._weekly_job_due(None, MON_0900)


def test_not_due_after_run_this_week():
    assert not bg._weekly_job_due(str(int(MON_0900) + 60), _ts(2026, 11, 18, 12))


def test_not_due_before_monday_nine():
    last = str(int(_ts(2026, 11, 9, 9)))
    assert not bg._weekly_job_due(last, _ts(2026, 11, 16, 8, 59))


def test_due_at_monday_nine():
    last = str(int(_ts(2026, 11, 9, 9)))
    assert bg._weekly_job_due(last, MON_0900)


def test_late_catchup_does_not_shift_next_week():
    # missed 09:00, caught up at 15:00 monday
    last = str(int(_ts(2026, 11, 16, 15)))
    assert not bg._weekly_job_due(last, _ts(2026, 11, 16, 21))
    assert not bg._weekly_job_due(last, _ts(2026, 11, 23, 8, 59))
    assert bg._weekly_job_due(last, NEXT_MON_0900)
