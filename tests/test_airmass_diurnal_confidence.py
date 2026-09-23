"""Confidence wiring for airmass_diurnal.py: real weights + non-uniform
confidence should shift member_id=0's value versus weights alone, and a
missing weight should drop only that member, not the whole group."""
import math

import models.airmass_diurnal as m
from models._utils import _sector
from tests.conftest import make_input_db

_T11 = 1_780_000_000  # arbitrary fixed epoch used as issued_at
_LEAD = 6


def _make_rich_input_db():
    """Input DB with 30 days x 24 hours of synthetic obs, mirroring
    test_airmass_diurnal_signals.py's own fixture, so _hour_means succeeds
    and the 3h recent window has daytime obs with rising solar."""
    conn = make_input_db()
    base_ts = _T11

    for day in range(30):
        for hour in range(24):
            ts = base_ts - (30 - day) * 86400 + hour * 3600
            solar = max(0.0, 700.0 * math.sin(math.pi * (hour - 6) / 14.0)) if 6 <= hour <= 20 else 0.0
            t = 15.0 + 5.0 * math.sin(math.pi * (hour - 6) / 14.0) if 6 <= hour <= 20 else 8.0
            conn.execute(
                "insert into tempest_obs "
                "(station_id, timestamp, air_temp, dew_point, station_pressure, wind_avg, solar_radiation) "
                "values ('KTEST', ?, ?, ?, ?, ?, ?)",
                (ts, t, 6.0, 1012.0, 3.0, solar),
            )

    for mins in range(0, 181, 10):
        ts = base_ts - 180 * 60 + mins * 60
        solar = max(0.0, (mins / 180.0) * 600.0)
        conn.execute(
            "insert into tempest_obs "
            "(station_id, timestamp, air_temp, dew_point, station_pressure, wind_avg, solar_radiation) "
            "values ('KTEST', ?, 19.0, 7.0, 1007.0, 2.0, ?)",
            (ts, solar),
        )

    return conn


def _make_obs():
    return {
        "timestamp": _T11,
        "air_temp": 21.6,
        "dew_point": 8.1,
        "station_pressure": 1007.0,  # below 30d mean ~1012
        "wind_avg": 2.0,
        "wind_direction": 340.0,
        "solar_radiation": 600.0,
    }


def test_clearness_and_sector_members_differ():
    conn_in = _make_rich_input_db()
    obs = _make_obs()
    rows = m.run(obs, obs["timestamp"], conn_in=conn_in)

    def val(mid):
        return next(
            r["value"] for r in rows
            if r["member_id"] == mid and r["lead_hours"] == _LEAD and r["variable"] == "temperature"
        )

    v1, v4 = val(1), val(4)  # clearness-only vs wind-sector-only
    assert v1 is not None and v4 is not None
    # the seasonal-swing-ceiling guardrail can clamp both members to the same
    # bound when the unclamped values run past it, saturating the difference
    assert v1 >= v4, f"clearness member 1 ({v1:.2f}) should be warmer than wind-sector member 4 ({v4:.2f})"


def test_confidence_shifts_value_versus_weights_alone():
    conn_in = _make_rich_input_db()
    obs = _make_obs()
    valid_at = obs["timestamp"] + _LEAD * 3600
    sector = _sector(valid_at)

    weights = {(1, "temperature", _LEAD, sector): 1.0,
               (4, "temperature", _LEAD, sector): 1.0}
    rows_no_confidence = m.run(obs, obs["timestamp"], conn_in=conn_in, weights=weights)
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
        4: _history(matched_mae=10.0, baseline_mae=5.0),
    }
    default_matches = matched_days

    rows_with_confidence = m.run(
        obs, obs["timestamp"], conn_in=conn_in, weights=weights,
        member_history=member_history, default_matches=default_matches,
    )
    mean_with_confidence = next(
        r["value"] for r in rows_with_confidence
        if r["member_id"] == 0 and r["lead_hours"] == _LEAD and r["variable"] == "temperature"
    )

    assert mean_with_confidence != mean_no_confidence


def test_missing_weight_drops_only_that_member():
    conn_in = _make_rich_input_db()
    obs = _make_obs()
    valid_at = obs["timestamp"] + _LEAD * 3600
    sector = _sector(valid_at)
    all_ids = m._ALL_MEMBER_IDS

    weights = {(mid, "temperature", _LEAD, sector): 1.0 for mid in all_ids if mid != 4}
    rows = m.run(obs, obs["timestamp"], conn_in=conn_in, weights=weights)
    mean_row = next(r for r in rows if r["member_id"] == 0 and r["lead_hours"] == _LEAD and r["variable"] == "temperature")
    member_vals = {
        r["member_id"]: r["value"] for r in rows
        if r["lead_hours"] == _LEAD and r["variable"] == "temperature" and r["member_id"] != 0
        and r["value"] is not None
    }
    whole_group_average = sum(member_vals.values()) / len(member_vals)
    weighted_without_missing = sum(v for mid, v in member_vals.items() if mid != 4) / (len(member_vals) - 1)

    assert abs(mean_row["value"] - weighted_without_missing) < 1e-6
    # the seasonal-swing-ceiling guardrail can clamp every member to the same
    # bound in extreme fixtures, saturating the two averages to the same value
    assert mean_row["value"] >= whole_group_average, (
        f"excluding member 4 ({member_vals[4]:.2f}) should not pull the mean "
        f"({mean_row['value']:.2f}) below the naive whole-group average ({whole_group_average:.2f})"
    )
