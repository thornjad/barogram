"""Confidence wiring for climatological_mean.py (Pattern E: no combination
step). Its only row (member_id=0, the implicit default) is climatological_mean
model's temperature/dewpoint reference and must not change VALUE under this
plan; only confidence may differ."""
import models.climatological_mean as climo_mean_mod
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
    return {0: _history(matched_mae=0.2, baseline_mae=5.0, matched_days=matched_days, baseline_days=baseline_days)}, matched_days


def _row(rows):
    return next(r for r in rows if r["lead_hours"] == _LEAD and r["variable"] == _VARIABLE)


def test_values_unchanged_with_or_without_confidence():
    conn_in = make_input_db()
    obs = make_obs(ts=_ISSUED_AT)
    member_history, default_matches = _mature_member_history()

    rows_without = climo_mean_mod.run(obs, _ISSUED_AT, conn_in=conn_in)
    rows_with = climo_mean_mod.run(
        obs, _ISSUED_AT, conn_in=conn_in,
        member_history=member_history, default_matches=default_matches,
    )

    by_key_without = {(r["lead_hours"], r["variable"]): r["value"] for r in rows_without}
    by_key_with = {(r["lead_hours"], r["variable"]): r["value"] for r in rows_with}

    assert by_key_without.keys() == by_key_with.keys()
    for key, value in by_key_without.items():
        assert by_key_with[key] == value, f"value changed for {key}: {value!r} -> {by_key_with[key]!r}"


def test_confidence_present_with_real_member_history():
    conn_in = make_input_db()
    obs = make_obs(ts=_ISSUED_AT)
    member_history, default_matches = _mature_member_history()

    rows = climo_mean_mod.run(
        obs, _ISSUED_AT, conn_in=conn_in,
        member_history=member_history, default_matches=default_matches,
    )

    assert _row(rows)["confidence"] is not None


def test_confidence_none_without_member_history():
    conn_in = make_input_db()
    obs = make_obs(ts=_ISSUED_AT)

    rows = climo_mean_mod.run(obs, _ISSUED_AT, conn_in=conn_in)

    assert _row(rows)["confidence"] is None
