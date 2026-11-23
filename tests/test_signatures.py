import datetime as dt

import pytest

from models._signatures import SolarEnvelope, kernel_matches, spearman


def _day_rows(day: dt.date, peak: float):
    return [{"timestamp": int(dt.datetime(day.year, day.month, day.day, h).timestamp()), "solar_radiation": peak}
            for h in range(8, 17)]


def _rows(peaks):
    start = dt.date(2026, 6, 1)
    out = []
    for i, p in enumerate(peaks):
        out += _day_rows(start + dt.timedelta(days=i), p)
    return out, start + dt.timedelta(days=len(peaks))


def test_envelope_ratio_uses_trailing_max():
    rows, today = _rows([800.0] * 10)
    env = SolarEnvelope(rows)
    ts = int(dt.datetime(today.year, today.month, today.day, 10).timestamp())
    assert env.ratio(ts, 200.0) == pytest.approx(0.25)


def test_envelope_ignores_the_day_itself_and_later():
    rows, today = _rows([800.0] * 10)
    rows += _day_rows(today, 5000.0)
    env = SolarEnvelope(rows)
    ts = int(dt.datetime(today.year, today.month, today.day, 10).timestamp())
    assert env.ceiling(ts) == 800.0


def test_envelope_needs_enough_days():
    rows, today = _rows([800.0] * 3)
    env = SolarEnvelope(rows)
    ts = int(dt.datetime(today.year, today.month, today.day, 10).timestamp())
    assert env.ratio(ts, 200.0) is None


def test_kernel_matches_identical_is_one_and_far_is_dropped():
    cur = {"timestamp": 100, "a": 1.0, "b": 2.0}
    cands = [{"timestamp": 1, "a": 1.0, "b": 2.0}, {"timestamp": 2, "a": 50.0, "b": 2.0},
             {"timestamp": 3, "a": None, "b": 2.0}]
    out = kernel_matches(cur, cands, {"a": 1.0, "b": 1.0})
    assert out == [(1, 1.0)]


def test_kernel_matches_wider_kernel_keeps_more():
    cur = {"timestamp": 100, "a": 0.0}
    cands = [{"timestamp": 1, "a": 2.0}]
    narrow = kernel_matches(cur, cands, {"a": 1.0}, width=0.5)
    wide = kernel_matches(cur, cands, {"a": 1.0}, width=2.0)
    assert narrow == []
    assert wide and wide[0][1] > 0.3


def test_spearman_perfect_and_constant():
    assert spearman([1, 2, 3, 4], [10, 20, 30, 40]) == pytest.approx(1.0)
    assert spearman([1, 2, 3, 4], [4, 3, 2, 1]) == pytest.approx(-1.0)
    assert spearman([1, 1, 1], [1, 2, 3]) is None
    assert spearman([1, 2], [1, 2]) is None


def test_spearman_averages_ties():
    assert spearman([0, 0, 1, 2], [3, 3, 2, 1]) == pytest.approx(-1.0)
