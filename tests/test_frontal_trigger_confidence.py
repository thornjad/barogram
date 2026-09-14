"""Confidence wiring for frontal_trigger.py: real weights + non-uniform
confidence should shift member_id=0's value versus weights alone, and a
missing weight should drop only that member, not the whole group.

Two isolated event types build the joint/marginal conditional tables:
"P" (pressure-tendency rising, wind steady) and "PV" (rising AND veering
together). ptend_veer_strict only fires when BOTH signals are active at once,
so it abstains under a P-only live state while ptend_veer_loose and
ptend_veer_weighted still produce real, distinct values -- the real
behavioral difference this model exists for.
"""
import models.frontal_trigger as ft_mod
from models._utils import _sector
from tests.conftest import make_input_db, make_obs

_LEAD = 6
_BLOCK_GAP = 24 * 3600
_PTEND_GAP = 3 * 3600
_FUTURE_OFFSET = _LEAD * 3600
_BASE_TS = 6_000_000

_P_TEMP_DELTA = 2.0   # type "P": ptend rising, veer steady
_PV_TEMP_DELTA = 8.0  # type "PV": ptend rising, veer veering

_PRESS_PAST = 1010.0
_PRESS_NOW = 1011.0  # +1.0 over 3h -> slow_rise -> "rising"
_TEMP_NOW = 20.0
_DEW_NOW = 10.0


def _row(ts, wind_direction, air_temp, station_pressure=_PRESS_NOW, wind_avg=2.0, dew_point=_DEW_NOW):
    return {
        "timestamp": ts,
        "wind_direction": wind_direction,
        "wind_avg": wind_avg,
        "air_temp": air_temp,
        "dew_point": dew_point,
        "station_pressure": station_pressure,
    }


def _build_all_obs():
    rows = []
    live_ts_p = None
    live_ts_pv = None
    block_types = [False] * 4 + [True] * 4  # False = type P, True = type PV
    for k, is_pv in enumerate(block_types):
        t_past = _BASE_TS + k * _BLOCK_GAP
        t_now = t_past + _PTEND_GAP
        t_fut = t_now + _FUTURE_OFFSET
        dir_now = 160.0 if is_pv else 105.0  # net veer 60deg (active) vs 5deg (steady)
        temp_delta = _PV_TEMP_DELTA if is_pv else _P_TEMP_DELTA

        rows.append(_row(t_past, wind_direction=100.0, air_temp=_TEMP_NOW, station_pressure=_PRESS_PAST))
        rows.append(_row(t_now, wind_direction=dir_now, air_temp=_TEMP_NOW, station_pressure=_PRESS_NOW))
        rows.append(_row(t_fut, wind_direction=None, air_temp=_TEMP_NOW + temp_delta, station_pressure=_PRESS_NOW))

        if not is_pv:
            live_ts_p = t_now
        else:
            live_ts_pv = t_now
    return rows, live_ts_p, live_ts_pv


def _temperature_value(rows, member_id):
    return next(
        r["value"] for r in rows
        if r["member_id"] == member_id and r["lead_hours"] == _LEAD and r["variable"] == "temperature"
    )


def test_loose_and_weighted_members_differ_under_partial_activation():
    conn_in = make_input_db()
    all_obs, live_ts_p, _ = _build_all_obs()
    obs = make_obs(ts=live_ts_p)

    rows = ft_mod.run(obs, live_ts_p, conn_in=conn_in, all_obs=all_obs)
    strict_val = _temperature_value(rows, 1)
    loose_val = _temperature_value(rows, 2)
    weighted_val = _temperature_value(rows, 3)

    # strict abstains (veer inactive) while loose and weighted still fire --
    # a real, verifiable behavioral difference this model exists for.
    assert strict_val is None
    assert loose_val is not None and weighted_val is not None
    assert loose_val != weighted_val


def test_confidence_shifts_value_versus_weights_alone():
    conn_in = make_input_db()
    all_obs, live_ts_p, _ = _build_all_obs()
    obs = make_obs(ts=live_ts_p)
    sector = _sector(live_ts_p + _LEAD * 3600)

    weights = {(2, "temperature", _LEAD, sector): 1.0,
               (3, "temperature", _LEAD, sector): 1.0}
    rows_no_confidence = ft_mod.run(obs, live_ts_p, conn_in=conn_in, all_obs=all_obs, weights=weights)
    mean_no_confidence = _temperature_value(rows_no_confidence, 0)

    matched_days = [10_000_000 + d * 86400 for d in range(10)]
    baseline_days = [10_000_000 + d * 86400 for d in range(50, 80)]  # not in default_matches

    def _history(matched_mae, baseline_mae):
        rows = [{"variable": "temperature", "lead_hours": _LEAD, "issued_at": ts, "mae": matched_mae} for ts in matched_days]
        rows += [{"variable": "temperature", "lead_hours": _LEAD, "issued_at": ts, "mae": baseline_mae} for ts in baseline_days]
        return rows

    member_history = {
        2: _history(matched_mae=0.1, baseline_mae=5.0),
        3: _history(matched_mae=10.0, baseline_mae=5.0),
    }
    default_matches = matched_days

    rows_with_confidence = ft_mod.run(
        obs, live_ts_p, conn_in=conn_in, all_obs=all_obs, weights=weights,
        member_history=member_history, default_matches=default_matches,
    )
    mean_with_confidence = _temperature_value(rows_with_confidence, 0)

    assert mean_with_confidence != mean_no_confidence


def test_missing_weight_drops_only_that_member():
    conn_in = make_input_db()
    all_obs, _, live_ts_pv = _build_all_obs()
    obs = make_obs(ts=live_ts_pv)
    sector = _sector(live_ts_pv + _LEAD * 3600)

    # all three members produce real values under the joint PV live state;
    # every member except member 3 (ptend_veer_weighted) gets a weight
    weights = {(1, "temperature", _LEAD, sector): 1.0,
               (2, "temperature", _LEAD, sector): 1.0}
    rows = ft_mod.run(obs, live_ts_pv, conn_in=conn_in, all_obs=all_obs, weights=weights)
    mean_row = next(r for r in rows if r["member_id"] == 0 and r["lead_hours"] == _LEAD and r["variable"] == "temperature")
    member_vals = {
        r["member_id"]: r["value"] for r in rows
        if r["lead_hours"] == _LEAD and r["variable"] == "temperature" and r["member_id"] != 0
        and r["value"] is not None
    }
    assert len(member_vals) == 3  # sanity: strict, loose, and weighted all fired here

    whole_group_average = sum(member_vals.values()) / len(member_vals)
    weighted_without_weighted_member = sum(v for mid, v in member_vals.items() if mid != 3) / (len(member_vals) - 1)

    assert abs(mean_row["value"] - weighted_without_weighted_member) < 1e-6
    assert abs(mean_row["value"] - whole_group_average) > 1e-6
