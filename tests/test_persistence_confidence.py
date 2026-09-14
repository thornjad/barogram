"""Confidence wiring for persistence.py (Pattern E: no combination step).

member_id=0 (raw current observation) and member_id=1 (trend_persistence) are
both real, independent forecasts here, not a combined aggregate -- each gets
its own confidence computed from its own member_history entry. Neither one's
VALUE may change under this plan; only confidence may differ."""
import models.persistence as persistence_mod
from tests.conftest import make_input_db, make_obs

_ISSUED_AT = 1_700_000_000  # fixed epoch for determinism
_LEAD = 6
_VARIABLE = "temperature"


def _history(matched_mae, baseline_mae, matched_days, baseline_days):
    rows = [{"variable": _VARIABLE, "lead_hours": _LEAD, "issued_at": ts, "mae": matched_mae}
            for ts in matched_days]
    rows += [{"variable": _VARIABLE, "lead_hours": _LEAD, "issued_at": ts, "mae": baseline_mae}
             for ts in baseline_days]
    return rows


def _mature_member_history():
    matched_days = [10_000_000 + d * 86400 for d in range(10)]
    baseline_days = [10_000_000 + d * 86400 for d in range(50, 80)]  # not in default_matches
    return {
        0: _history(matched_mae=0.2, baseline_mae=5.0, matched_days=matched_days, baseline_days=baseline_days),
        1: _history(matched_mae=0.2, baseline_mae=5.0, matched_days=matched_days, baseline_days=baseline_days),
    }, matched_days


def _row(rows, member_id):
    return next(
        r for r in rows
        if r["member_id"] == member_id and r["lead_hours"] == _LEAD and r["variable"] == _VARIABLE
    )


def test_values_unchanged_with_or_without_confidence():
    conn_in = make_input_db()
    obs = make_obs(ts=_ISSUED_AT)
    member_history, default_matches = _mature_member_history()

    rows_without = persistence_mod.run(obs, _ISSUED_AT, conn_in=conn_in)
    rows_with = persistence_mod.run(
        obs, _ISSUED_AT, conn_in=conn_in,
        member_history=member_history, default_matches=default_matches,
    )

    by_key_without = {(r["member_id"], r["lead_hours"], r["variable"]): r["value"] for r in rows_without}
    by_key_with = {(r["member_id"], r["lead_hours"], r["variable"]): r["value"] for r in rows_with}

    assert by_key_without.keys() == by_key_with.keys()
    for key, value in by_key_without.items():
        assert by_key_with[key] == value, f"value changed for {key}: {value!r} -> {by_key_with[key]!r}"


def test_confidence_present_with_real_member_history():
    conn_in = make_input_db()
    obs = make_obs(ts=_ISSUED_AT)
    member_history, default_matches = _mature_member_history()

    rows = persistence_mod.run(
        obs, _ISSUED_AT, conn_in=conn_in,
        member_history=member_history, default_matches=default_matches,
    )

    row_0 = _row(rows, 0)
    row_1 = _row(rows, 1)
    assert row_0["confidence"] is not None
    assert row_1["confidence"] is not None


def test_confidence_none_without_member_history():
    conn_in = make_input_db()
    obs = make_obs(ts=_ISSUED_AT)

    rows = persistence_mod.run(obs, _ISSUED_AT, conn_in=conn_in)

    row_0 = _row(rows, 0)
    row_1 = _row(rows, 1)
    assert row_0["confidence"] is None
    assert row_1["confidence"] is None
