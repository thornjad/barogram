"""Confidence wiring for dry_airmass_diurnal.py: real weights + non-uniform
confidence should shift member_id=0's value versus weights alone, and a
missing weight should drop only that member, not the whole group."""
import datetime
import math

import models.dry_airmass_diurnal as m
from models._utils import _sector
from tests.conftest import make_input_db, make_obs

_ISSUED_AT = int(datetime.datetime(2026, 6, 15, 12, 0, 0).timestamp())
_LEAD = 6
_VALID_AT = _ISSUED_AT + _LEAD * 3600


def _seed_input_db():
    """30 days of hourly obs (constant DD=5 baseline) so _hour_means succeeds,
    then a dry-airmass anomaly (DD=20) is layered onto just the last 24h so
    members with different lookback windows compute different mean_dd."""
    conn = make_input_db()
    for day in range(30):
        for hour in range(24):
            ts = _ISSUED_AT - (30 - day) * 86400 + hour * 3600
            t = 15.0 + 8.0 * math.sin(math.pi * (hour - 6) / 14.0) if 6 <= hour <= 20 else 10.0
            td = t - 5.0
            conn.execute(
                "insert into tempest_obs "
                "(station_id, timestamp, air_temp, dew_point, station_pressure, wind_avg) "
                "values ('KTEST', ?, ?, ?, 1013.0, 3.0)",
                (ts, t, td),
            )
    cutoff_24h = _ISSUED_AT - 24 * 3600
    conn.execute(
        "update tempest_obs set dew_point = air_temp - 20.0 where timestamp > ?",
        (cutoff_24h,),
    )
    return conn


def _make_obs():
    obs = make_obs(ts=_ISSUED_AT)
    obs["air_temp"] = 15.0
    obs["dew_point"] = -5.0  # DD=20, matching the recent anomaly
    return obs


def test_window_length_members_differ():
    conn_in = _seed_input_db()
    obs = _make_obs()
    rows = m.run(obs, _ISSUED_AT, conn_in=conn_in)

    def val(mid):
        return next(
            r["value"] for r in rows
            if r["member_id"] == mid and r["lead_hours"] == _LEAD and r["variable"] == "temperature"
        )

    v1, v2 = val(1), val(2)
    assert v1 is not None and v2 is not None
    assert v1 != v2


def test_confidence_shifts_value_versus_weights_alone():
    conn_in = _seed_input_db()
    obs = _make_obs()
    sector = _sector(_VALID_AT)

    weights = {(1, "temperature", _LEAD, sector): 1.0,
               (2, "temperature", _LEAD, sector): 1.0}
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
        2: _history(matched_mae=10.0, baseline_mae=5.0),
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
    obs = _make_obs()
    sector = _sector(_VALID_AT)
    all_ids = m._ALL_MEMBER_IDS

    weights = {
        (mid, "temperature", _LEAD, sector): 1.0
        for mid in all_ids if mid != 2
    }
    rows = m.run(obs, _ISSUED_AT, conn_in=conn_in, weights=weights)
    mean_row = next(r for r in rows if r["member_id"] == 0 and r["lead_hours"] == _LEAD and r["variable"] == "temperature")
    member_vals = {
        r["member_id"]: r["value"] for r in rows
        if r["lead_hours"] == _LEAD and r["variable"] == "temperature" and r["member_id"] != 0
        and r["value"] is not None
    }
    whole_group_average = sum(member_vals.values()) / len(member_vals)
    weighted_without_missing = sum(v for mid, v in member_vals.items() if mid != 2) / (len(member_vals) - 1)

    assert abs(mean_row["value"] - weighted_without_missing) < 1e-6
    assert abs(mean_row["value"] - whole_group_average) > 1e-6
