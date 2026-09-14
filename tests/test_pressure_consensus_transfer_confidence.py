"""Confidence wiring for pressure_consensus_transfer.py: real weights +
non-uniform confidence should shift member_id=0's value versus weights
alone, and a missing weight should drop only that member, not the whole
group."""
import db as dbmod
import models.pressure_consensus_transfer as pct_mod
from models._utils import _sector
from tests.conftest import make_input_db, make_obs, make_output_db

_ISSUED_AT = 1_700_000_000  # fixed epoch for determinism
_LEAD = 6
_VALID_AT = _ISSUED_AT + _LEAD * 3600


def _seed_sources(conn_out):
    # climatological_mean outranks persistence in _PRIORITY, so best_model_only
    # (member 3) picks its value while simple_mean_consensus (member 1) averages
    # both -- a real, non-uniform difference between the two members.
    dbmod.insert_forecasts(conn_out, [
        {"model_id": 1, "model": "persistence", "member_id": 0,
         "issued_at": _ISSUED_AT, "valid_at": _VALID_AT, "lead_hours": _LEAD,
         "variable": "pressure", "value": 1000.0},
        {"model_id": 2, "model": "climatological_mean", "member_id": 0,
         "issued_at": _ISSUED_AT, "valid_at": _VALID_AT, "lead_hours": _LEAD,
         "variable": "pressure", "value": 1020.0},
    ])


def _pressure_value(rows, member_id):
    return next(
        r["value"] for r in rows
        if r["member_id"] == member_id and r["lead_hours"] == _LEAD and r["variable"] == "pressure"
    )


def test_mean_and_best_model_members_differ():
    conn_in = make_input_db()
    conn_out = make_output_db()
    _seed_sources(conn_out)
    obs = make_obs(ts=_ISSUED_AT)

    rows = pct_mod.run(obs, _ISSUED_AT, conn_in=conn_in, conn_out=conn_out, all_obs=[])
    mean_val = _pressure_value(rows, 1)
    best_val = _pressure_value(rows, 3)

    assert mean_val is not None and best_val is not None
    assert mean_val != best_val


def test_confidence_shifts_value_versus_weights_alone():
    conn_in = make_input_db()
    conn_out = make_output_db()
    _seed_sources(conn_out)
    obs = make_obs(ts=_ISSUED_AT)
    sector = _sector(_VALID_AT)

    weights = {(1, "pressure", _LEAD, sector): 1.0,
               (3, "pressure", _LEAD, sector): 1.0}
    rows_no_confidence = pct_mod.run(
        obs, _ISSUED_AT, conn_in=conn_in, conn_out=conn_out, all_obs=[], weights=weights
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
        3: _history(matched_mae=10.0, baseline_mae=5.0),
    }
    default_matches = matched_days

    rows_with_confidence = pct_mod.run(
        obs, _ISSUED_AT, conn_in=conn_in, conn_out=conn_out, all_obs=[], weights=weights,
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

    # every member except member 3 (best_model_only) gets a weight
    weights = {(1, "pressure", _LEAD, sector): 1.0,
               (2, "pressure", _LEAD, sector): 1.0}
    rows = pct_mod.run(obs, _ISSUED_AT, conn_in=conn_in, conn_out=conn_out, all_obs=[], weights=weights)
    mean_row = next(r for r in rows if r["member_id"] == 0 and r["lead_hours"] == _LEAD and r["variable"] == "pressure")
    member_vals = {
        r["member_id"]: r["value"] for r in rows
        if r["lead_hours"] == _LEAD and r["variable"] == "pressure" and r["member_id"] != 0
        and r["value"] is not None
    }
    whole_group_average = sum(member_vals.values()) / len(member_vals)
    weighted_without_best = sum(v for mid, v in member_vals.items() if mid != 3) / (len(member_vals) - 1)

    assert abs(mean_row["value"] - weighted_without_best) < 1e-6
    assert abs(mean_row["value"] - whole_group_average) > 1e-6
