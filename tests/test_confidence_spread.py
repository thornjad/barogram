import math

import models._confidence as confidence
from tests.conftest import make_input_db
from tests.test_confidence import _DAY, _insert_obs, _row


def _one_match_history():
    base = 10_000_000
    base -= base % _DAY
    moment = base + 5 * _DAY
    history = [_row("temperature", 24, base + d * _DAY, 10.0) for d in range(10) if d != 5]
    history.append(_row("temperature", 24, moment, 4.0))
    return history, moment


def test_spread_none_matches_shared_spread():
    history, moment = _one_match_history()
    confidence.set_spread({("temperature", 24): 5.0})
    default = confidence.confidence_for_cell(history, "temperature", 24, [moment])
    assert confidence.confidence_for_cell(history, "temperature", 24, [moment], spread=None) == default
    assert default == confidence.confidence_for_cell(history, "temperature", 24, [moment], spread=5.0)


def test_spread_override_changes_q_and_ignores_shared():
    history, moment = _one_match_history()
    confidence.set_spread({("temperature", 24): 5.0})
    expected = math.exp(-2 * (4.0 / 2.0) ** 2) * confidence.evidence(1.0)
    assert abs(confidence.confidence_for_cell(history, "temperature", 24, [moment], spread=2.0) - expected) < 1e-12
    called = []

    def fn(variable, lead):
        called.append((variable, lead))
        return 2.0

    assert abs(confidence.confidence_for_cell(history, "temperature", 24, [moment], spread=fn) - expected) < 1e-12
    assert called == [("temperature", 24)]


def test_spread_override_needs_no_shared_spread_and_guards_hold():
    history, moment = _one_match_history()
    confidence.set_spread({})
    assert confidence.confidence_for_cell(history, "temperature", 24, [moment], spread=2.0) > 0.0
    assert confidence.confidence_for_cell(history, "temperature", 24, [moment], spread=0.0) == 0.0
    assert confidence.confidence_for_cell(history, "temperature", 24, [moment], spread=lambda v, l: None) == 0.0


def test_member_confidences_passes_spread_through():
    history, moment = _one_match_history()
    confidence.set_spread({("temperature", 24): 5.0})
    shared = confidence.member_confidences({1: history}, [moment], [1], "temperature", 24)
    own = confidence.member_confidences({1: history}, [moment], [1], "temperature", 24, spread=2.0)
    assert shared[1] == confidence.confidence_for_cell(history, "temperature", 24, [moment])
    assert own[1] == confidence.confidence_for_cell(history, "temperature", 24, [moment], spread=2.0)
    assert own[1] != shared[1]


def test_own_spread_is_population_stdev_of_outcomes_on_own_matches():
    conn = make_input_db()
    t0 = 10_000_000 - 10_000_000 % _DAY
    for ts, value in [(t0, 10.0), (t0 + _DAY, 16.0)]:
        _insert_obs(conn, ts + 6 * 3600, air_temp=value)
    fn = confidence.own_spread(conn, [t0, (t0 + _DAY, 0.7)])
    assert abs(fn("temperature", 6) - 3.0) < 1e-12   # population stdev of 10 and 16
    assert fn("temperature", 7) is None              # no observation near the target time
    assert fn("pressure", 6) == 0.0                  # constant outcome; confidence_for_cell guards it
