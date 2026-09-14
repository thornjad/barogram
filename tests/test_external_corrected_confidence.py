"""Confidence wiring for external_corrected.py: per-member confidence
attached to each of the 10 members' own rows, member_id=0's confidence as
a plain average over all 10, and _source_weights' both-known branch
shifting when one source's confidence is much better than the other's --
its three early-return branches and the zero-MAE guard stay exactly as
they were, unaffected by confidence."""
from types import SimpleNamespace

import models._confidence as _confidence
import models.external_corrected as ec_mod
from tests.conftest import make_input_db, make_obs, make_output_db

_ISSUED_AT = 1_700_000_000  # fixed epoch for determinism
_LEAD = 6
_VALID_AT = _ISSUED_AT + _LEAD * 3600


# ---------------------------------------------------------------------------
# _source_weights: the three early-return branches are unaffected by confidence
# ---------------------------------------------------------------------------

def test_both_unknown_branch_ignores_confidence():
    nw, tw = ec_mod._source_weights({}, {}, "temperature", _LEAD, 0, 0.95, 0.05)
    assert (nw, tw) == (0.5, 0.5)


def test_nws_unknown_branch_ignores_confidence():
    tempest_mae = {("temperature", _LEAD, 0): 1.0}
    nw, tw = ec_mod._source_weights({}, tempest_mae, "temperature", _LEAD, 0, 0.95, 0.05)
    assert (nw, tw) == (0.0, 1.0)


def test_tempest_unknown_branch_ignores_confidence():
    nws_mae = {("temperature", _LEAD, 0): 1.0}
    nw, tw = ec_mod._source_weights(nws_mae, {}, "temperature", _LEAD, 0, 0.05, 0.95)
    assert (nw, tw) == (1.0, 0.0)


# ---------------------------------------------------------------------------
# both-known branch: zero-MAE guard still doesn't raise with confidence multiplied in
# ---------------------------------------------------------------------------

def test_zero_mae_guard_does_not_raise_with_confidence():
    key = ("temperature", _LEAD, 0)
    nws_mae = {key: 0.0}
    tempest_mae = {key: 0.0}
    nw, tw = ec_mod._source_weights(nws_mae, tempest_mae, "temperature", _LEAD, 0, 0.9, 0.1)
    assert nw + tw == 1.0
    # both sides hit the 1e9 guard equally; confidence still tips the split
    assert nw > tw


# ---------------------------------------------------------------------------
# both-known branch: the split shifts when one source's confidence beats the other's
# ---------------------------------------------------------------------------

def test_source_weights_shift_with_skewed_hardcoded_confidence():
    key = ("temperature", _LEAD, 0)
    nws_mae = {key: 1.0}
    tempest_mae = {key: 1.0}
    nw_equal, tw_equal = ec_mod._source_weights(
        nws_mae, tempest_mae, "temperature", _LEAD, 0, 0.5, 0.5
    )
    nw_skewed, tw_skewed = ec_mod._source_weights(
        nws_mae, tempest_mae, "temperature", _LEAD, 0, 0.9, 0.1
    )
    assert nw_equal == 0.5
    assert nw_skewed > nw_equal
    assert tw_skewed < tw_equal


def test_source_weights_shift_from_computed_confidence():
    """Confidence built the same way analog/climo_deviation build it in their
    own tests: matched days with a much better MAE than the baseline days
    for nws, and no such improvement for tempest -- a test that skips the
    baseline days would accidentally always land on confidence 0.5."""
    matched_days = [10_000_000 + d * 86400 for d in range(10)]
    baseline_days = [10_000_000 + d * 86400 for d in range(50, 80)]  # not in default_matches

    def _history(matched_mae, baseline_mae):
        rows = [{"variable": "temperature", "lead_hours": _LEAD, "issued_at": ts, "mae": matched_mae}
                 for ts in matched_days]
        rows += [{"variable": "temperature", "lead_hours": _LEAD, "issued_at": ts, "mae": baseline_mae}
                 for ts in baseline_days]
        return rows

    member_history = {}
    for mid in ec_mod._NWS_MEMBERS:
        member_history[mid] = _history(matched_mae=0.1, baseline_mae=5.0)
    for mid in ec_mod._TEMPEST_MEMBERS:
        member_history[mid] = _history(matched_mae=10.0, baseline_mae=5.0)
    default_matches = matched_days

    confs = _confidence.member_confidences(
        member_history, default_matches, ec_mod._ALL_MEMBERS, "temperature", _LEAD
    )
    nws_confidence = _confidence.average_confidence([confs[m] for m in ec_mod._NWS_MEMBERS])
    tempest_confidence = _confidence.average_confidence([confs[m] for m in ec_mod._TEMPEST_MEMBERS])
    assert nws_confidence is not None and tempest_confidence is not None
    assert nws_confidence > tempest_confidence  # nws' matched-day error is much better

    key = ("temperature", _LEAD, 0)
    nws_mae = {key: 1.0}
    tempest_mae = {key: 1.0}
    nw_equal, tw_equal = ec_mod._source_weights(
        nws_mae, tempest_mae, "temperature", _LEAD, 0, 0.5, 0.5
    )
    nw_skewed, tw_skewed = ec_mod._source_weights(
        nws_mae, tempest_mae, "temperature", _LEAD, 0, nws_confidence, tempest_confidence
    )
    assert nw_skewed > nw_equal
    assert tw_skewed < tw_equal


# ---------------------------------------------------------------------------
# run()-level wiring: each of the 10 members gets its own confidence,
# member_id=0's confidence is the plain average over all 10
# ---------------------------------------------------------------------------

def test_run_attaches_per_member_confidence_and_averages_for_member_zero(monkeypatch):
    conn_in = make_input_db()
    conn_out = make_output_db()
    obs = make_obs(ts=_ISSUED_AT)

    canned = {_VALID_AT: {"temperature": 10.0, "dewpoint": 4.0}}
    monkeypatch.setattr(ec_mod, "_fetch_nws", lambda lat, lon: canned)
    monkeypatch.setattr(ec_mod, "_fetch_tempest", lambda station_id, token: canned)
    conf = SimpleNamespace(tempest_station_id="99999", tempest_token="fake-token")

    matched_days = [10_000_000 + d * 86400 for d in range(10)]
    baseline_days = [10_000_000 + d * 86400 for d in range(50, 80)]

    def _history(mid):
        matched_mae = 0.1 if mid % 2 == 0 else 5.0
        rows = [{"variable": "temperature", "lead_hours": _LEAD, "issued_at": ts, "mae": matched_mae}
                 for ts in matched_days]
        rows += [{"variable": "temperature", "lead_hours": _LEAD, "issued_at": ts, "mae": 5.0}
                 for ts in baseline_days]
        return rows

    member_history = {mid: _history(mid) for mid in ec_mod._ALL_MEMBERS}
    default_matches = matched_days

    rows = ec_mod.run(
        obs, _ISSUED_AT, conn_in=conn_in, conn_out=conn_out, conf=conf,
        member_history=member_history, default_matches=default_matches,
    )

    member_rows = [
        r for r in rows
        if r["member_id"] != 0 and r["lead_hours"] == _LEAD and r["variable"] == "temperature"
    ]
    assert len(member_rows) == 10
    assert all(r["confidence"] is not None for r in member_rows)
    # even-numbered members were given a much better matched-day MAE
    even_confidences = {r["confidence"] for r in member_rows if r["member_id"] % 2 == 0}
    odd_confidences = {r["confidence"] for r in member_rows if r["member_id"] % 2 == 1}
    assert min(even_confidences) > max(odd_confidences)

    zero_row = next(
        r for r in rows
        if r["member_id"] == 0 and r["lead_hours"] == _LEAD and r["variable"] == "temperature"
    )
    expected = _confidence.average_confidence([r["confidence"] for r in member_rows])
    assert zero_row["confidence"] == expected


def test_run_without_member_history_degrades_confidence_to_none(monkeypatch):
    """No member_history/default_matches (today's calling convention) leaves
    every confidence None, same as every other model in this plan."""
    conn_in = make_input_db()
    conn_out = make_output_db()
    obs = make_obs(ts=_ISSUED_AT)

    canned = {_VALID_AT: {"temperature": 10.0, "dewpoint": 4.0}}
    monkeypatch.setattr(ec_mod, "_fetch_nws", lambda lat, lon: canned)
    monkeypatch.setattr(ec_mod, "_fetch_tempest", lambda station_id, token: canned)
    conf = SimpleNamespace(tempest_station_id="99999", tempest_token="fake-token")

    rows = ec_mod.run(obs, _ISSUED_AT, conn_in=conn_in, conn_out=conn_out, conf=conf)

    assert len(rows) > 0
    assert all(r["confidence"] is None for r in rows)
