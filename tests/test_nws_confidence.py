"""Confidence wiring for nws.py (Pattern E: no combination step). Its only
row (member_id=0) must not change VALUE under this plan; only confidence
may differ. Network access is mocked via a patched _fetch, matching
tests/test_models_contract.py's own pattern for external models."""
from unittest.mock import patch

import models.nws as nws_mod
from tests.conftest import make_obs

_ISSUED_AT = 1_700_000_000  # fixed epoch for determinism
_LEAD = 6
_VARIABLE = "temperature"
_LOCATION = (44.98, -93.27)

_CANNED = {
    _ISSUED_AT + h * 3600: {"temperature": 20.0, "dewpoint": 12.0}
    for h in range(1, 25)
}


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


def _run(**kwargs):
    obs = make_obs(ts=_ISSUED_AT)
    with patch.object(nws_mod, "_fetch", return_value=_CANNED):
        return nws_mod.run(obs, _ISSUED_AT, location=_LOCATION, **kwargs)


def test_values_unchanged_with_or_without_confidence():
    member_history, default_matches = _mature_member_history()

    rows_without = _run()
    rows_with = _run(member_history=member_history, default_matches=default_matches)

    by_key_without = {(r["lead_hours"], r["variable"]): r["value"] for r in rows_without}
    by_key_with = {(r["lead_hours"], r["variable"]): r["value"] for r in rows_with}

    assert by_key_without.keys() == by_key_with.keys()
    for key, value in by_key_without.items():
        assert by_key_with[key] == value, f"value changed for {key}: {value!r} -> {by_key_with[key]!r}"


def test_confidence_present_with_real_member_history():
    member_history, default_matches = _mature_member_history()

    rows = _run(member_history=member_history, default_matches=default_matches)

    assert _row(rows)["confidence"] is not None


def test_confidence_none_without_member_history():
    rows = _run()

    assert _row(rows)["confidence"] is None
