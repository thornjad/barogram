"""Confidence wiring for weighted_climatological_mean.py: real weights +
non-uniform confidence should shift member_id=0's value versus weights
alone, and a missing weight should drop only that member, not the whole
group."""
import datetime as dt

import models.weighted_climatological_mean as m
from models._utils import _sector
from tests.conftest import make_input_db, make_obs

_ISSUED_AT = int(dt.datetime(2026, 6, 15, 12, 0, 0).timestamp())
_LEAD = 6
_VALID_AT = _ISSUED_AT + _LEAD * 3600


def _seed_input_db():
    """Two obs in the same (month, hour) bucket as valid_at, at very different
    ages: a 'recent' one (age < 1 day) and a much older one (age >> 30 days),
    with distinct values, so members whose weight_fn favors recency differ
    from members that don't."""
    conn = make_input_db()
    valid_dt = dt.datetime.fromtimestamp(_VALID_AT)
    recent_dt = dt.datetime(valid_dt.year, valid_dt.month, valid_dt.day, valid_dt.hour, 0, 0)
    old_dt = dt.datetime(valid_dt.year - 6, valid_dt.month, valid_dt.day, valid_dt.hour, 0, 0)
    conn.execute(
        "insert into tempest_obs "
        "(station_id, timestamp, air_temp, dew_point, station_pressure, wind_avg) "
        "values ('KTEST', ?, 30.0, 15.0, 1013.0, 3.0)",
        (int(recent_dt.timestamp()),),
    )
    conn.execute(
        "insert into tempest_obs "
        "(station_id, timestamp, air_temp, dew_point, station_pressure, wind_avg) "
        "values ('KTEST', ?, 10.0, 5.0, 1013.0, 3.0)",
        (int(old_dt.timestamp()),),
    )
    return conn


def test_recency_weighted_members_differ():
    conn_in = _seed_input_db()
    obs = make_obs(ts=_ISSUED_AT)
    rows = m.run(obs, _ISSUED_AT, conn_in=conn_in)

    def val(mid):
        return next(
            r["value"] for r in rows
            if r["member_id"] == mid and r["lead_hours"] == _LEAD and r["variable"] == "temperature"
        )

    v1, v3 = val(1), val(3)  # "today-only" vs "month-only"
    assert v1 is not None and v3 is not None
    assert v1 != v3


def test_confidence_shifts_value_versus_weights_alone():
    conn_in = _seed_input_db()
    obs = make_obs(ts=_ISSUED_AT)
    sector = _sector(_VALID_AT)

    weights = {(1, "temperature", _LEAD, sector): 1.0,
               (3, "temperature", _LEAD, sector): 1.0}
    rows_no_confidence = m.run(obs, _ISSUED_AT, conn_in=conn_in, weights=weights)
    mean_no_confidence = next(
        r["value"] for r in rows_no_confidence
        if r["member_id"] == 0 and r["lead_hours"] == _LEAD and r["variable"] == "temperature"
    )

    matched_days = [10_000_000 + d * 86400 for d in range(10)]
    baseline_days = [10_000_000 + d * 86400 for d in range(50, 80)]  # not in default_matches

    def _history(matched_mae, baseline_mae):
        rows = [{"variable": "temperature", "lead_hours": _LEAD, "issued_at": ts, "mae": matched_mae} for ts in matched_days]
        rows += [{"variable": "temperature", "lead_hours": _LEAD, "issued_at": ts, "mae": baseline_mae} for ts in baseline_days]
        return rows

    member_history = {
        1: _history(matched_mae=0.1, baseline_mae=5.0),
        3: _history(matched_mae=10.0, baseline_mae=5.0),
    }
    default_matches = matched_days

    rows_with_confidence = m.run(
        obs, _ISSUED_AT, conn_in=conn_in, weights=weights,
        member_history=member_history, default_matches=default_matches,
    )
    mean_with_confidence = next(
        r["value"] for r in rows_with_confidence
        if r["member_id"] == 0 and r["lead_hours"] == _LEAD and r["variable"] == "temperature"
    )

    assert mean_with_confidence != mean_no_confidence


def test_missing_weight_drops_only_that_member():
    conn_in = _seed_input_db()
    obs = make_obs(ts=_ISSUED_AT)
    sector = _sector(_VALID_AT)
    all_ids = m._ALL_MEMBER_IDS

    weights = {(mid, "temperature", _LEAD, sector): 1.0 for mid in all_ids if mid != 3}
    rows = m.run(obs, _ISSUED_AT, conn_in=conn_in, weights=weights)
    mean_row = next(r for r in rows if r["member_id"] == 0 and r["lead_hours"] == _LEAD and r["variable"] == "temperature")
    member_vals = {
        r["member_id"]: r["value"] for r in rows
        if r["lead_hours"] == _LEAD and r["variable"] == "temperature" and r["member_id"] != 0
        and r["value"] is not None
    }
    whole_group_average = sum(member_vals.values()) / len(member_vals)
    weighted_without_missing = sum(v for mid, v in member_vals.items() if mid != 3) / (len(member_vals) - 1)

    assert abs(mean_row["value"] - weighted_without_missing) < 1e-6
    assert abs(mean_row["value"] - whole_group_average) > 1e-6
