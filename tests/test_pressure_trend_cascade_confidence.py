"""Confidence wiring for pressure_trend_cascade.py: real weights + non-uniform
confidence should shift member_id=0's value versus weights alone, and a
missing weight should drop only that member, not the whole group."""
import datetime

import models.pressure_trend_cascade as m
from models._utils import _sector
from tests.conftest import make_input_db, make_obs

_ISSUED_AT = int(datetime.datetime(2026, 6, 15, 12, 0, 0).timestamp())
_LEAD = 6
_VALID_AT = _ISSUED_AT + _LEAD * 3600


def _make_all_obs():
    """Long stable background (for p_mean / transfer-fn baselines) plus a
    steadily falling pressure in the most recent 6h, so linear_extrap
    (mean-reverted polynomial fit) and damped_extrap (raw analytic rate
    decay, no mean reversion) diverge."""
    all_obs = []
    for hrs_ago in range(400, 6, -1):
        ts = _ISSUED_AT - hrs_ago * 3600
        all_obs.append({"timestamp": ts, "station_pressure": 1013.0, "air_temp": 15.0, "dew_point": 10.0})
    for mins in range(0, 361, 15):
        ts = _ISSUED_AT - (360 - mins) * 60
        p = 1013.0 - (mins / 360.0) * 6.0
        all_obs.append({"timestamp": ts, "station_pressure": p, "air_temp": 15.0, "dew_point": 10.0})
    return all_obs


def _make_obs():
    obs = make_obs(ts=_ISSUED_AT)
    obs["station_pressure"] = 1007.0
    return obs


def test_linear_and_damped_members_differ():
    conn_in = make_input_db()
    all_obs = _make_all_obs()
    obs = _make_obs()
    rows = m.run(obs, _ISSUED_AT, conn_in=conn_in, all_obs=all_obs)

    def val(mid):
        return next(
            r["value"] for r in rows
            if r["member_id"] == mid and r["lead_hours"] == _LEAD and r["variable"] == "pressure"
        )

    v1, v3 = val(1), val(3)
    assert v1 is not None and v3 is not None
    assert v1 != v3


def test_confidence_shifts_value_versus_weights_alone():
    conn_in = make_input_db()
    all_obs = _make_all_obs()
    obs = _make_obs()
    sector = _sector(_VALID_AT)

    weights = {(1, "pressure", _LEAD, sector): 1.0, (3, "pressure", _LEAD, sector): 1.0}
    rows_no_confidence = m.run(obs, _ISSUED_AT, conn_in=conn_in, all_obs=all_obs, weights=weights)
    mean_no_confidence = next(
        r["value"] for r in rows_no_confidence
        if r["member_id"] == 0 and r["lead_hours"] == _LEAD and r["variable"] == "pressure"
    )

    matched_days = [10_000_000 + d * 86400 for d in range(10)]
    baseline_days = [10_000_000 + d * 86400 for d in range(50, 80)]  # not in default_matches

    def _history(matched_mae, baseline_mae):
        rows = [{"variable": "pressure", "lead_hours": _LEAD, "issued_at": ts, "mae": matched_mae} for ts in matched_days]
        rows += [{"variable": "pressure", "lead_hours": _LEAD, "issued_at": ts, "mae": baseline_mae} for ts in baseline_days]
        return rows

    member_history = {
        1: _history(matched_mae=0.1, baseline_mae=5.0),
        3: _history(matched_mae=10.0, baseline_mae=5.0),
    }
    default_matches = matched_days

    rows_with_confidence = m.run(
        obs, _ISSUED_AT, conn_in=conn_in, all_obs=all_obs, weights=weights,
        member_history=member_history, default_matches=default_matches,
    )
    mean_with_confidence = next(
        r["value"] for r in rows_with_confidence
        if r["member_id"] == 0 and r["lead_hours"] == _LEAD and r["variable"] == "pressure"
    )

    assert mean_with_confidence != mean_no_confidence


def test_missing_weight_drops_only_that_member():
    conn_in = make_input_db()
    all_obs = _make_all_obs()
    obs = _make_obs()
    sector = _sector(_VALID_AT)
    all_ids = m._ALL_MEMBER_IDS

    weights = {(mid, "pressure", _LEAD, sector): 1.0 for mid in all_ids if mid != 3}
    rows = m.run(obs, _ISSUED_AT, conn_in=conn_in, all_obs=all_obs, weights=weights)
    mean_row = next(r for r in rows if r["member_id"] == 0 and r["lead_hours"] == _LEAD and r["variable"] == "pressure")
    member_vals = {
        r["member_id"]: r["value"] for r in rows
        if r["lead_hours"] == _LEAD and r["variable"] == "pressure" and r["member_id"] != 0
        and r["value"] is not None
    }
    whole_group_average = sum(member_vals.values()) / len(member_vals)
    weighted_without_missing = sum(v for mid, v in member_vals.items() if mid != 3) / (len(member_vals) - 1)

    assert abs(mean_row["value"] - weighted_without_missing) < 1e-6
    assert abs(mean_row["value"] - whole_group_average) > 1e-6
