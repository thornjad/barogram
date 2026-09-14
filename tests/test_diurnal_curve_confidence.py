"""Confidence wiring for diurnal_curve.py: real weights + non-uniform
confidence should shift member_id=0's value versus weights alone, and a
missing weight should drop only that member, not the whole group."""
import datetime as dt

import models.diurnal_curve as dcv_mod
from tests.conftest import make_input_db, make_obs

_ISSUED_AT = 1_700_000_000  # fixed epoch for determinism
_LEAD = 6
_VALID_AT = _ISSUED_AT + _LEAD * 3600
_VARIABLE = "temperature"

# member_id 9 = sine, 30d lookback, anchor "none"; member_id 21 = piecewise,
# same lookback/anchor -- see _CURVES/_LOOKBACKS/_ANCHORS ordering in
# models/diurnal_curve.py (mid = c_idx*12 + l_idx*3 + a_idx + 1)
_SINE_30D_NONE = 9
_PIECEWISE_30D_NONE = 21


def _insert_obs(conn, ts, air_temp):
    conn.execute(
        """
        insert into tempest_obs
            (station_id, timestamp, air_temp, dew_point, station_pressure,
             wind_avg, wind_gust, wind_direction, precip_accum_day,
             solar_radiation, uv_index, lightning_count)
        values ('KTEST', ?, ?, 12.0, 1013.0, 3.0, null, null, 0.0, 0.0, 0.0, 0)
        """,
        (ts, air_temp),
    )


def _seed_triangle_wave(conn_in, issued_at, num_days=4):
    """Non-sinusoidal (triangle wave) hourly temps so the sine and piecewise
    curve fits genuinely diverge -- piecewise interpolates the triangle
    exactly, sine's smooth fit can't."""
    base = dt.datetime.fromtimestamp(issued_at)
    for day_offset in range(1, num_days + 1):
        day = base - dt.timedelta(days=day_offset)
        for hour in range(24):
            ts = int(day.replace(hour=hour, minute=0, second=0, microsecond=0).timestamp())
            _insert_obs(conn_in, ts, float(abs(hour - 12)))


def test_sine_and_piecewise_members_differ():
    conn_in = make_input_db()
    _seed_triangle_wave(conn_in, _ISSUED_AT)
    obs = make_obs(ts=_ISSUED_AT)

    rows = dcv_mod.run(obs, _ISSUED_AT, conn_in=conn_in)
    sine_val = next(
        r["value"] for r in rows
        if r["member_id"] == _SINE_30D_NONE and r["lead_hours"] == _LEAD and r["variable"] == _VARIABLE
    )
    piecewise_val = next(
        r["value"] for r in rows
        if r["member_id"] == _PIECEWISE_30D_NONE and r["lead_hours"] == _LEAD and r["variable"] == _VARIABLE
    )

    assert sine_val is not None and piecewise_val is not None
    assert sine_val != piecewise_val


def test_confidence_shifts_value_versus_weights_alone():
    conn_in = make_input_db()
    _seed_triangle_wave(conn_in, _ISSUED_AT)
    obs = make_obs(ts=_ISSUED_AT)
    sector = dcv_mod._sector(_VALID_AT)

    weights = {
        (_SINE_30D_NONE, _VARIABLE, _LEAD, sector): 1.0,
        (_PIECEWISE_30D_NONE, _VARIABLE, _LEAD, sector): 1.0,
    }
    rows_no_confidence = dcv_mod.run(obs, _ISSUED_AT, conn_in=conn_in, weights=weights)
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
        _SINE_30D_NONE: _history(matched_mae=0.1, baseline_mae=5.0),
        _PIECEWISE_30D_NONE: _history(matched_mae=10.0, baseline_mae=5.0),
    }
    default_matches = matched_days

    rows_with_confidence = dcv_mod.run(
        obs, _ISSUED_AT, conn_in=conn_in, weights=weights,
        member_history=member_history, default_matches=default_matches,
    )
    mean_with_confidence = next(
        r["value"] for r in rows_with_confidence
        if r["member_id"] == 0 and r["lead_hours"] == _LEAD and r["variable"] == _VARIABLE
    )

    assert mean_with_confidence != mean_no_confidence


def test_missing_weight_drops_only_that_member():
    conn_in = make_input_db()
    _seed_triangle_wave(conn_in, _ISSUED_AT)
    obs = make_obs(ts=_ISSUED_AT)
    sector = dcv_mod._sector(_VALID_AT)

    # every member except the piecewise member gets a weight
    weights = {
        (mid, _VARIABLE, _LEAD, sector): 1.0
        for mid in dcv_mod._ALL_MEMBER_IDS if mid != _PIECEWISE_30D_NONE
    }
    rows = dcv_mod.run(obs, _ISSUED_AT, conn_in=conn_in, weights=weights)
    mean_row = next(r for r in rows if r["member_id"] == 0 and r["lead_hours"] == _LEAD and r["variable"] == _VARIABLE)
    member_vals = {
        r["member_id"]: r["value"] for r in rows
        if r["lead_hours"] == _LEAD and r["variable"] == _VARIABLE and r["member_id"] != 0
        and r["value"] is not None
    }
    whole_group_average = sum(member_vals.values()) / len(member_vals)
    weighted_without_piecewise = sum(
        v for mid, v in member_vals.items() if mid != _PIECEWISE_30D_NONE
    ) / (len(member_vals) - 1)

    assert abs(mean_row["value"] - weighted_without_piecewise) < 1e-6
    assert abs(mean_row["value"] - whole_group_average) > 1e-6
