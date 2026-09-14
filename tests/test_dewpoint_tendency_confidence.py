"""Confidence wiring for dewpoint_tendency.py: real weights + non-uniform
confidence should shift member_id=0's value versus weights alone, and a
missing weight should drop only that member, not the whole group.

A kinked dew-point trend (slow rise for the first 2h of the 3h window, then
a much faster rise in the final hour) makes each member's own window/
half-life mechanism fit a genuinely different slope: linear_1h only ever
sees the fast segment, linear_3h fits the whole kinked window unweighted,
and linear_3h_hl45 fits the same window but leans toward the recent (fast)
data via its half-life weighting.
"""
import models.dewpoint_tendency as dt_mod
from models._utils import _sector
from tests.conftest import make_input_db, make_obs

_ISSUED_AT = 1_650_000_000  # fixed epoch for determinism
_LEAD = 6
_VALID_AT = _ISSUED_AT + _LEAD * 3600


def _dew_point_at_offset(offset_min):
    """offset_min <= 0, minutes relative to issued_at. Slow rise out to -60min,
    then a much faster rise from -60min to 0 -- a real kink linear_1h alone sees."""
    if offset_min <= -60:
        return 10.0 + 0.01 * (offset_min + 180)
    return 11.2 + 0.05 * (offset_min + 60)


def _build_all_obs():
    offsets = list(range(-180, 1, 15))  # every 15 minutes across the 3h window
    return [
        {"timestamp": _ISSUED_AT + offset * 60, "dew_point": _dew_point_at_offset(offset)}
        for offset in offsets
    ]


def _dewpoint_value(rows, member_id):
    return next(
        r["value"] for r in rows
        if r["member_id"] == member_id and r["lead_hours"] == _LEAD and r["variable"] == "dewpoint"
    )


def test_members_differ():
    conn_in = make_input_db()
    all_obs = _build_all_obs()
    obs = make_obs(ts=_ISSUED_AT)

    rows = dt_mod.run(obs, _ISSUED_AT, conn_in=conn_in, all_obs=all_obs)
    v1 = _dewpoint_value(rows, 1)
    v2 = _dewpoint_value(rows, 2)
    v3 = _dewpoint_value(rows, 3)

    assert v1 is not None and v2 is not None and v3 is not None
    assert len({v1, v2, v3}) == 3


def test_confidence_shifts_value_versus_weights_alone():
    conn_in = make_input_db()
    all_obs = _build_all_obs()
    obs = make_obs(ts=_ISSUED_AT)
    sector = _sector(_VALID_AT)

    weights = {(1, "dewpoint", _LEAD, sector): 1.0,
               (2, "dewpoint", _LEAD, sector): 1.0}
    rows_no_confidence = dt_mod.run(obs, _ISSUED_AT, conn_in=conn_in, all_obs=all_obs, weights=weights)
    mean_no_confidence = _dewpoint_value(rows_no_confidence, 0)

    matched_days = [10_000_000 + d * 86400 for d in range(10)]
    baseline_days = [10_000_000 + d * 86400 for d in range(50, 80)]  # not in default_matches

    def _history(matched_mae, baseline_mae):
        rows = [{"variable": "dewpoint", "lead_hours": _LEAD, "issued_at": ts, "mae": matched_mae} for ts in matched_days]
        rows += [{"variable": "dewpoint", "lead_hours": _LEAD, "issued_at": ts, "mae": baseline_mae} for ts in baseline_days]
        return rows

    member_history = {
        1: _history(matched_mae=0.1, baseline_mae=5.0),
        2: _history(matched_mae=10.0, baseline_mae=5.0),
    }
    default_matches = matched_days

    rows_with_confidence = dt_mod.run(
        obs, _ISSUED_AT, conn_in=conn_in, all_obs=all_obs, weights=weights,
        member_history=member_history, default_matches=default_matches,
    )
    mean_with_confidence = _dewpoint_value(rows_with_confidence, 0)

    assert mean_with_confidence != mean_no_confidence


def test_missing_weight_drops_only_that_member():
    conn_in = make_input_db()
    all_obs = _build_all_obs()
    obs = make_obs(ts=_ISSUED_AT)
    sector = _sector(_VALID_AT)

    # every member except member 3 (linear_3h_hl45) gets a weight
    weights = {(1, "dewpoint", _LEAD, sector): 1.0,
               (2, "dewpoint", _LEAD, sector): 1.0}
    rows = dt_mod.run(obs, _ISSUED_AT, conn_in=conn_in, all_obs=all_obs, weights=weights)
    mean_row = next(r for r in rows if r["member_id"] == 0 and r["lead_hours"] == _LEAD and r["variable"] == "dewpoint")
    member_vals = {
        r["member_id"]: r["value"] for r in rows
        if r["lead_hours"] == _LEAD and r["variable"] == "dewpoint" and r["member_id"] != 0
        and r["value"] is not None
    }
    whole_group_average = sum(member_vals.values()) / len(member_vals)
    weighted_without_hl45 = sum(v for mid, v in member_vals.items() if mid != 3) / (len(member_vals) - 1)

    assert abs(mean_row["value"] - weighted_without_hl45) < 1e-6
    assert abs(mean_row["value"] - whole_group_average) > 1e-6
