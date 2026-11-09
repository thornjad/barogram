"""trend_climo_blend (model 30): basis diversity, blend-weight decay, the
climo-only reference member, confidence-weighted combination, and
self-correction wiring.

A three-segment kinked temperature trend (slow -12h..-6h, moderate -6h..-1h,
steep -1h..now) makes the 3h/6h/12h trend windows fit genuinely different
slopes, the same technique test_dewpoint_tendency_confidence.py uses for its
own window members. Climo is seeded to a flat, hour-independent constant so
blend-decay assertions aren't muddied by the real diurnal cycle.
"""
import models.trend_climo_blend as tcb
from models._utils import _sector
from tests.conftest import make_input_db, make_obs, make_output_db

_ISSUED_AT = 1_700_000_000
_CLIMO_VALUE = {"temperature": 15.0, "dewpoint": 8.0}


def _seed_flat_climo(conn_in):
    """Enough obs at every hour, all identical, so climo_bucket_means returns
    the same constant regardless of which (month, hour) bucket a lead lands in."""
    base_ts = 10_000_000
    for day in range(35):
        for hour in range(24):
            ts = base_ts + day * 86400 + hour * 3600
            conn_in.execute(
                """insert into tempest_obs
                   (station_id, timestamp, air_temp, dew_point, station_pressure, wind_avg)
                   values ('KTEST', ?, ?, ?, 1013.0, 3.0)""",
                (ts, _CLIMO_VALUE["temperature"], _CLIMO_VALUE["dewpoint"]),
            )


def _temp_at_offset(offset_min):
    """offset_min <= 0. Slow drift to -6h, moderate to -1h, steep in the final hour."""
    if offset_min <= -360:
        return 0.0 + 0.002 * (offset_min + 720)
    if offset_min <= -60:
        return 0.72 + 0.01 * (offset_min + 360)
    return 3.72 + 0.05 * (offset_min + 60)


def _build_all_obs():
    offsets = list(range(-720, 1, 15))
    return [
        {
            "timestamp": _ISSUED_AT + offset * 60,
            "air_temp": _temp_at_offset(offset),
            "dew_point": 12.0,
        }
        for offset in offsets
    ]


def _seed_analog_candidates(conn_in, n=20):
    """n distinct historical days, same time-of-day as issued_at, temperature
    strictly increasing by 0.5 per day so K-nearest means are deterministic.
    dewpoint/pressure/wind match obs exactly so distance is driven by temp alone."""
    dt_local = __import__("datetime").datetime.fromtimestamp(_ISSUED_AT)
    for i in range(n):
        day_ts = _ISSUED_AT - (i + 2) * 86400
        day_dt = __import__("datetime").datetime.fromtimestamp(day_ts).replace(
            hour=dt_local.hour, minute=dt_local.minute, second=dt_local.second
        )
        ts = int(day_dt.timestamp())
        temp = 20.0 + (i + 1) * 0.5
        conn_in.execute(
            """insert into tempest_obs
               (station_id, timestamp, air_temp, dew_point, station_pressure, wind_avg)
               values ('KTEST', ?, ?, 12.0, 1013.2, 3.5)""",
            (ts, temp),
        )


def _value(rows, member_id, variable="temperature", lead=6):
    return next(
        r["value"] for r in rows
        if r["member_id"] == member_id and r["variable"] == variable and r["lead_hours"] == lead
    )


def test_trend_windows_differ_under_kinked_trend():
    conn_in = make_input_db()
    _seed_flat_climo(conn_in)
    all_obs = _build_all_obs()
    obs = make_obs(ts=_ISSUED_AT)
    obs["air_temp"] = _temp_at_offset(0)

    rows = tcb.run(obs, _ISSUED_AT, conn_in=conn_in, all_obs=all_obs)

    v_trend3 = _value(rows, 1)   # trend3_lin
    v_trend6 = _value(rows, 4)   # trend6_lin
    v_trend12 = _value(rows, 7)  # trend12_lin
    v_flat = _value(rows, 10)    # flat_lin

    assert None not in (v_trend3, v_trend6, v_trend12, v_flat)
    assert len({v_trend3, v_trend6, v_trend12, v_flat}) == 4


def test_climo_only_member_ignores_trend_entirely():
    conn_in = make_input_db()
    _seed_flat_climo(conn_in)
    all_obs = _build_all_obs()
    obs = make_obs(ts=_ISSUED_AT)
    obs["air_temp"] = _temp_at_offset(0)  # far from the seeded climo constant

    rows = tcb.run(obs, _ISSUED_AT, conn_in=conn_in, all_obs=all_obs)

    for lead in (1, 12, 24):
        v = _value(rows, tcb._CLIMO_ONLY_MEMBER, lead=lead)
        assert v is not None
        assert abs(v - _CLIMO_VALUE["temperature"]) < 1e-6


def test_blend_weight_decays_basis_to_climo_over_lead():
    conn_in = make_input_db()
    _seed_flat_climo(conn_in)
    all_obs = _build_all_obs()
    obs = make_obs(ts=_ISSUED_AT)
    obs["air_temp"] = 25.0  # obs far from climo constant (15.0), basis = flat = 25.0

    rows = tcb.run(obs, _ISSUED_AT, conn_in=conn_in, all_obs=all_obs)

    v1 = _value(rows, 11, lead=1)    # flat_exp, mod rate
    v24 = _value(rows, 11, lead=24)

    climo = _CLIMO_VALUE["temperature"]
    basis = 25.0
    assert abs(v1 - basis) < abs(v1 - climo)      # short lead leans on the trend basis
    assert abs(v24 - climo) < abs(v24 - basis)    # long lead has given way to climatology
    assert v24 == v24  # not NaN
    assert abs(v24 - climo) < 0.5                 # exp(-0.15*24) ~= 0.027, nearly fully decayed


def test_faster_rate_decays_more_than_slower_rate_at_same_lead():
    conn_in = make_input_db()
    _seed_flat_climo(conn_in)
    all_obs = _build_all_obs()
    obs = make_obs(ts=_ISSUED_AT)
    obs["air_temp"] = 25.0

    rows = tcb.run(obs, _ISSUED_AT, conn_in=conn_in, all_obs=all_obs)

    v_fast = _value(rows, 24, lead=12)  # flat_exp_fast, k=0.35
    v_slow = _value(rows, 25, lead=12)  # flat_exp_slow, k=0.05
    climo = _CLIMO_VALUE["temperature"]

    assert abs(v_fast - climo) < abs(v_slow - climo)


def test_analog_k_means_differ_and_order_correctly():
    conn_in = make_input_db()
    _seed_flat_climo(conn_in)
    _seed_analog_candidates(conn_in)
    all_obs = _build_all_obs()
    obs = make_obs(ts=_ISSUED_AT)
    obs["air_temp"] = _temp_at_offset(0)

    rows = tcb.run(obs, _ISSUED_AT, conn_in=conn_in, all_obs=all_obs)

    v_k5 = _value(rows, 13, lead=1)   # analogk5_lin
    v_k10 = _value(rows, 16, lead=1)  # analogk10_lin
    v_k20 = _value(rows, 19, lead=1)  # analogk20_lin

    assert None not in (v_k5, v_k10, v_k20)
    # candidate temps rise monotonically with distance (20.5, 21.0, ... 30.0),
    # so widening K can only pull the mean up
    assert v_k5 < v_k10 < v_k20


def test_missing_input_falls_back_gracefully_when_climo_absent():
    conn_in = make_input_db()  # no climo seeded at all
    all_obs = _build_all_obs()
    obs = make_obs(ts=_ISSUED_AT)
    obs["air_temp"] = _temp_at_offset(0)

    rows = tcb.run(obs, _ISSUED_AT, conn_in=conn_in, all_obs=all_obs)

    # every member needs climo_future; with none available, every cell is None
    assert all(r["value"] is None for r in rows if r["variable"] == "temperature")


def test_confidence_shifts_value_versus_weights_alone():
    conn_in = make_input_db()
    _seed_flat_climo(conn_in)
    all_obs = _build_all_obs()
    obs = make_obs(ts=_ISSUED_AT)
    obs["air_temp"] = _temp_at_offset(0)
    lead = 6
    valid_at = obs["timestamp"] + lead * 3600
    sector = _sector(valid_at)

    weights = {(1, "temperature", lead, sector): 1.0,
               (10, "temperature", lead, sector): 1.0}
    rows_no_confidence = tcb.run(obs, _ISSUED_AT, conn_in=conn_in, all_obs=all_obs, weights=weights)
    mean_no_confidence = _value(rows_no_confidence, 0, lead=lead)

    matched_days = [10_000_000 + d * 86400 for d in range(10)]
    baseline_days = [10_000_000 + d * 86400 for d in range(50, 80)]

    def _history(matched_mae, baseline_mae):
        h = [{"variable": "temperature", "lead_hours": lead, "issued_at": ts, "mae": matched_mae} for ts in matched_days]
        h += [{"variable": "temperature", "lead_hours": lead, "issued_at": ts, "mae": baseline_mae} for ts in baseline_days]
        return h

    member_history = {
        1: _history(matched_mae=0.1, baseline_mae=5.0),
        10: _history(matched_mae=10.0, baseline_mae=5.0),
    }
    default_matches = matched_days

    rows_with_confidence = tcb.run(
        obs, _ISSUED_AT, conn_in=conn_in, all_obs=all_obs, weights=weights,
        member_history=member_history, default_matches=default_matches,
    )
    mean_with_confidence = _value(rows_with_confidence, 0, lead=lead)

    assert mean_with_confidence != mean_no_confidence


def test_self_correction_member_applies_learned_bias():
    conn_in = make_input_db()
    _seed_flat_climo(conn_in)
    all_obs = _build_all_obs()
    obs = make_obs(ts=_ISSUED_AT)
    obs["air_temp"] = _temp_at_offset(0)
    conn_out = make_output_db()

    lead = 6
    valid_at = _ISSUED_AT + lead * 3600
    # seed this model's own member_id=0 scored history: consistently forecast
    # 3.0 too high, so the learned bias correction should subtract ~3.0
    for d in range(1, 6):
        past_issued = _ISSUED_AT - d * 86400
        past_valid = past_issued + lead * 3600
        conn_out.execute(
            """insert into forecasts
               (model_id, model, member_id, issued_at, valid_at, lead_hours, variable,
                value, observed, error, mae, scored_at)
               values (30, 'trend_climo_blend', 0, ?, ?, ?, 'temperature', 18.0, 15.0, 3.0, 3.0, ?)""",
            (past_issued, past_valid, lead, past_issued + 1),
        )

    rows = tcb.run(obs, _ISSUED_AT, conn_in=conn_in, conn_out=conn_out, all_obs=all_obs)
    mean_val = _value(rows, 0, lead=lead)
    corrected_val = _value(rows, tcb._SELF_CORRECTION_MEMBER, lead=lead)

    assert corrected_val is not None
    assert abs(corrected_val - (mean_val - 3.0)) < 1e-6
