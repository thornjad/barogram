"""Confidence wiring for pressure_tendency.py: real weights + non-uniform
confidence should shift member_id=0's value versus weights alone, and a
missing weight should drop only that member, not the whole group."""
import models.pressure_tendency as pt_mod
from tests.conftest import make_input_db, make_obs

_ISSUED_AT = 1_700_000_000  # fixed epoch for determinism
_LEAD = 6
_VALID_AT = _ISSUED_AT + _LEAD * 3600
_VARIABLE = "pressure"

_LINEAR_1H = 2   # (2, "linear_1h", degree=1, window=1h, no half-life)
_LINEAR_6H = 4   # (4, "linear_6h", degree=1, window=6h, no half-life)

_ALL_MEMBER_IDS = [1] + [mid for mid, *_ in pt_mod._MEMBERS]


def _build_all_obs(issued_at):
    """Flat pressure out to -6h, then a sharp ramp in the final hour, so the
    1h-window and 6h-window linear-fit members genuinely diverge once
    extrapolated forward -- the 6h window dilutes the ramp with several hours
    of flat history the 1h window never sees."""
    def _row(ts, pressure):
        return {
            "timestamp": ts,
            "air_temp": 20.0,
            "dew_point": 12.0,
            "station_pressure": pressure,
            "wind_avg": 3.0,
            "wind_direction": None,
            "solar_radiation": None,
            "uv_index": None,
            "wind_gust": None,
            "lightning_count": None,
            "precip_accum_day": None,
        }

    obs_list = [_row(issued_at - h * 3600, 1013.0) for h in range(6, 1, -1)]
    obs_list.append(_row(issued_at - 3600, 1013.0))   # t = -1h
    obs_list.append(_row(issued_at - 1800, 1018.0))   # t = -0.5h
    obs_list.append(_row(issued_at, 1023.0))          # t = 0h (now)
    return obs_list


def test_linear_short_and_long_window_members_differ():
    conn_in = make_input_db()
    all_obs = _build_all_obs(_ISSUED_AT)
    obs = make_obs(ts=_ISSUED_AT)
    obs["station_pressure"] = 1013.0

    rows = pt_mod.run(obs, _ISSUED_AT, conn_in=conn_in, all_obs=all_obs)
    short_val = next(
        r["value"] for r in rows
        if r["member_id"] == _LINEAR_1H and r["lead_hours"] == _LEAD and r["variable"] == _VARIABLE
    )
    long_val = next(
        r["value"] for r in rows
        if r["member_id"] == _LINEAR_6H and r["lead_hours"] == _LEAD and r["variable"] == _VARIABLE
    )

    assert short_val is not None and long_val is not None
    assert short_val != long_val


def test_confidence_shifts_value_versus_weights_alone():
    conn_in = make_input_db()
    all_obs = _build_all_obs(_ISSUED_AT)
    obs = make_obs(ts=_ISSUED_AT)
    obs["station_pressure"] = 1013.0
    sector = pt_mod._sector(_VALID_AT)

    weights = {
        (_LINEAR_1H, _VARIABLE, _LEAD, sector): 1.0,
        (_LINEAR_6H, _VARIABLE, _LEAD, sector): 1.0,
    }
    rows_no_confidence = pt_mod.run(obs, _ISSUED_AT, conn_in=conn_in, all_obs=all_obs, weights=weights)
    mean_no_confidence = next(
        r["value"] for r in rows_no_confidence
        if r["member_id"] == 0 and r["lead_hours"] == _LEAD and r["variable"] == _VARIABLE
    )

    matched_days = [10_000_000 + d * 86400 for d in range(10)]
    baseline_days = [10_000_000 + d * 86400 for d in range(50, 80)]  # not in default_matches

    def _history(matched_mae, baseline_mae):
        rows = [{"variable": _VARIABLE, "lead_hours": _LEAD, "issued_at": ts, "mae": matched_mae} for ts in matched_days]
        rows += [{"variable": _VARIABLE, "lead_hours": _LEAD, "issued_at": ts, "mae": baseline_mae} for ts in baseline_days]
        return rows

    member_history = {
        _LINEAR_1H: _history(matched_mae=0.1, baseline_mae=5.0),
        _LINEAR_6H: _history(matched_mae=10.0, baseline_mae=5.0),
    }
    default_matches = matched_days

    rows_with_confidence = pt_mod.run(
        obs, _ISSUED_AT, conn_in=conn_in, all_obs=all_obs, weights=weights,
        member_history=member_history, default_matches=default_matches,
    )
    mean_with_confidence = next(
        r["value"] for r in rows_with_confidence
        if r["member_id"] == 0 and r["lead_hours"] == _LEAD and r["variable"] == _VARIABLE
    )

    assert mean_with_confidence != mean_no_confidence


def test_missing_weight_drops_only_that_member():
    conn_in = make_input_db()
    all_obs = _build_all_obs(_ISSUED_AT)
    obs = make_obs(ts=_ISSUED_AT)
    obs["station_pressure"] = 1013.0
    sector = pt_mod._sector(_VALID_AT)

    # every member except linear_6h gets a weight
    weights = {
        (mid, _VARIABLE, _LEAD, sector): 1.0
        for mid in _ALL_MEMBER_IDS if mid != _LINEAR_6H
    }
    rows = pt_mod.run(obs, _ISSUED_AT, conn_in=conn_in, all_obs=all_obs, weights=weights)
    mean_row = next(r for r in rows if r["member_id"] == 0 and r["lead_hours"] == _LEAD and r["variable"] == _VARIABLE)
    member_vals = {
        r["member_id"]: r["value"] for r in rows
        if r["lead_hours"] == _LEAD and r["variable"] == _VARIABLE and r["member_id"] != 0
        and r["value"] is not None
    }
    whole_group_average = sum(member_vals.values()) / len(member_vals)
    weighted_without_dropped = sum(
        v for mid, v in member_vals.items() if mid != _LINEAR_6H
    ) / (len(member_vals) - 1)

    assert abs(mean_row["value"] - weighted_without_dropped) < 1e-6
    assert abs(mean_row["value"] - whole_group_average) > 1e-6
