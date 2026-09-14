"""Confidence wiring for inverse_pressure_transfer.py: real weights +
non-uniform confidence should shift member_id=0's value versus weights
alone, and a missing weight should drop only that member, not the whole
group."""
import math

import db as dbmod
import models.inverse_pressure_transfer as ipt_mod
from models._utils import _sector
from tests.conftest import make_input_db, make_obs, make_output_db

_ISSUED_AT = 1_700_000_000  # fixed epoch for determinism
_LEAD = 6
_VALID_AT = _ISSUED_AT + _LEAD * 3600


def _synthetic_all_obs():
    """Hourly history with a sinusoidal wobble on top of a trend for
    temp/dewpoint/pressure -- a purely linear trend gives every fixed-lead
    delta the same constant value (zero variance), which _ols1 can't fit a
    slope against; the wobble gives real, varying deltas to regress on."""
    base_ts = 10_000_000
    return [
        {
            "timestamp": base_ts + i * 3600,
            "air_temp": 15.0 + i * 0.3 + 3.0 * math.sin(i * 0.7),
            "dew_point": 8.0 + i * 0.2 + 2.0 * math.sin(i * 0.7 + 0.3),
            "station_pressure": 1015.0 - i * 0.1 - 5.0 * math.sin(i * 0.7 + 0.1),
        }
        for i in range(90)
    ]


def _seed_sources(conn_out):
    # distinct temperature/dewpoint deltas from obs -> temp_only_inverse and
    # dewpoint_only_inverse infer genuinely different pressure values.
    dbmod.insert_forecasts(conn_out, [
        {"model_id": 1, "model": "persistence", "member_id": 0,
         "issued_at": _ISSUED_AT, "valid_at": _VALID_AT, "lead_hours": _LEAD,
         "variable": "temperature", "value": 25.0},
        {"model_id": 2, "model": "climatological_mean", "member_id": 0,
         "issued_at": _ISSUED_AT, "valid_at": _VALID_AT, "lead_hours": _LEAD,
         "variable": "dewpoint", "value": 9.0},
    ])


def _pressure_value(rows, member_id):
    return next(
        r["value"] for r in rows
        if r["member_id"] == member_id and r["lead_hours"] == _LEAD and r["variable"] == "pressure"
    )


def test_temp_and_dewpoint_inverse_members_differ():
    conn_in = make_input_db()
    conn_out = make_output_db()
    _seed_sources(conn_out)
    obs = make_obs(ts=_ISSUED_AT)

    rows = ipt_mod.run(obs, _ISSUED_AT, conn_in=conn_in, conn_out=conn_out, all_obs=_synthetic_all_obs())
    temp_val = _pressure_value(rows, 1)
    dew_val = _pressure_value(rows, 2)

    assert temp_val is not None and dew_val is not None
    assert temp_val != dew_val


def test_confidence_shifts_value_versus_weights_alone():
    conn_in = make_input_db()
    conn_out = make_output_db()
    _seed_sources(conn_out)
    obs = make_obs(ts=_ISSUED_AT)
    sector = _sector(_VALID_AT)

    weights = {(1, "pressure", _LEAD, sector): 1.0,
               (2, "pressure", _LEAD, sector): 1.0}
    rows_no_confidence = ipt_mod.run(
        obs, _ISSUED_AT, conn_in=conn_in, conn_out=conn_out, all_obs=_synthetic_all_obs(), weights=weights
    )
    mean_no_confidence = _pressure_value(rows_no_confidence, 0)

    matched_days = [10_000_000 + d * 86400 for d in range(10)]
    baseline_days = [10_000_000 + d * 86400 for d in range(50, 80)]  # not in default_matches

    def _history(matched_mae, baseline_mae):
        rows = [{"variable": "pressure", "lead_hours": _LEAD, "issued_at": ts, "mae": matched_mae} for ts in matched_days]
        rows += [{"variable": "pressure", "lead_hours": _LEAD, "issued_at": ts, "mae": baseline_mae} for ts in baseline_days]
        return rows

    member_history = {
        1: _history(matched_mae=0.1, baseline_mae=5.0),
        2: _history(matched_mae=10.0, baseline_mae=5.0),
    }
    default_matches = matched_days

    rows_with_confidence = ipt_mod.run(
        obs, _ISSUED_AT, conn_in=conn_in, conn_out=conn_out, all_obs=_synthetic_all_obs(), weights=weights,
        member_history=member_history, default_matches=default_matches,
    )
    mean_with_confidence = _pressure_value(rows_with_confidence, 0)

    assert mean_with_confidence != mean_no_confidence


def test_missing_weight_drops_only_that_member():
    conn_in = make_input_db()
    conn_out = make_output_db()
    _seed_sources(conn_out)
    obs = make_obs(ts=_ISSUED_AT)
    sector = _sector(_VALID_AT)

    # every member except member 1 (temp_only_inverse) gets a weight
    weights = {(2, "pressure", _LEAD, sector): 1.0,
               (3, "pressure", _LEAD, sector): 1.0}
    rows = ipt_mod.run(obs, _ISSUED_AT, conn_in=conn_in, conn_out=conn_out, all_obs=_synthetic_all_obs(), weights=weights)
    mean_row = next(r for r in rows if r["member_id"] == 0 and r["lead_hours"] == _LEAD and r["variable"] == "pressure")
    member_vals = {
        r["member_id"]: r["value"] for r in rows
        if r["lead_hours"] == _LEAD and r["variable"] == "pressure" and r["member_id"] != 0
        and r["value"] is not None
    }
    whole_group_average = sum(member_vals.values()) / len(member_vals)
    weighted_without_temp_only = sum(v for mid, v in member_vals.items() if mid != 1) / (len(member_vals) - 1)

    assert abs(mean_row["value"] - weighted_without_temp_only) < 1e-6
    assert abs(mean_row["value"] - whole_group_average) > 1e-6
