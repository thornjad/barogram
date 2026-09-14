"""Confidence wiring for wind_veer_detector.py: real weights + non-uniform
confidence should shift member_id=0's value versus weights alone, and a
missing weight should drop only that member, not the whole group.

Members differ by their own wind-speed floor / gust-confirmation gate
(veer_nogate floor=0.0, veer_lowgate floor=0.3, veer_gust_confirmed needs a
gust/avg ratio > 1.8 on top of the nogate floor), so three isolated 1h
veering events at different wind speeds/gust ratios make each member
accumulate a genuinely different subset of the same synthetic history.
"""
import models.wind_veer_detector as wvd_mod
from models._utils import _sector
from tests.conftest import make_input_db, make_obs

_LEAD = 6
_BLOCK_GAP = 24 * 3600
_EVENT_GAP = 3600
_FUTURE_OFFSET = _LEAD * 3600
_BASE_TS = 5_000_000

# (type_label, wind_avg, wind_gust, temp_delta) -- 4 isolated repeats of each,
# well outside each other's 3h lookback window.
_BLOCK_SPECS = (
    [("A", 1.0, 2.5, 6.0)] * 4 +   # confirmed: passes every member's gate
    [("B", 0.5, 0.6, 1.0)] * 4 +   # unconfirmed: passes floors 0.0/0.3, fails gust confirm
    [("D", 0.15, 0.05, 9.0)] * 4   # low-wind: passes only the 0.0 floor
)

_TEMP_NOW = 20.0
_DEW_NOW = 10.0
_PRESS_NOW = 1013.0


def _obs_row(ts, wind_direction, wind_avg, wind_gust, air_temp, dew_point, station_pressure):
    return {
        "timestamp": ts,
        "wind_direction": wind_direction,
        "wind_avg": wind_avg,
        "wind_gust": wind_gust,
        "air_temp": air_temp,
        "dew_point": dew_point,
        "station_pressure": station_pressure,
    }


def _build_all_obs():
    rows = []
    last_type_a_t1 = None
    for k, (label, wind_avg, wind_gust, temp_delta) in enumerate(_BLOCK_SPECS):
        t0 = _BASE_TS + k * _BLOCK_GAP
        t1 = t0 + _EVENT_GAP
        t_fut = t1 + _FUTURE_OFFSET
        rows.append(_obs_row(t0, 100.0, wind_avg, wind_gust, _TEMP_NOW, _DEW_NOW, _PRESS_NOW))
        rows.append(_obs_row(t1, 170.0, wind_avg, wind_gust, _TEMP_NOW, _DEW_NOW, _PRESS_NOW))
        rows.append(_obs_row(t_fut, None, None, None, _TEMP_NOW + temp_delta, _DEW_NOW, _PRESS_NOW))
        if label == "A":
            last_type_a_t1 = t1
    return rows, last_type_a_t1


def _temperature_value(rows, member_id):
    return next(
        r["value"] for r in rows
        if r["member_id"] == member_id and r["lead_hours"] == _LEAD and r["variable"] == "temperature"
    )


def test_members_differ_by_gate():
    conn_in = make_input_db()
    all_obs, live_ts = _build_all_obs()
    obs = make_obs(ts=live_ts)

    rows = wvd_mod.run(obs, live_ts, conn_in=conn_in, all_obs=all_obs)
    nogate_val = _temperature_value(rows, 1)
    lowgate_val = _temperature_value(rows, 2)
    gust_confirmed_val = _temperature_value(rows, 3)

    assert nogate_val is not None and lowgate_val is not None and gust_confirmed_val is not None
    assert len({nogate_val, lowgate_val, gust_confirmed_val}) == 3


def test_confidence_shifts_value_versus_weights_alone():
    conn_in = make_input_db()
    all_obs, live_ts = _build_all_obs()
    obs = make_obs(ts=live_ts)
    sector = _sector(live_ts + _LEAD * 3600)

    weights = {(1, "temperature", _LEAD, sector): 1.0,
               (2, "temperature", _LEAD, sector): 1.0}
    rows_no_confidence = wvd_mod.run(obs, live_ts, conn_in=conn_in, all_obs=all_obs, weights=weights)
    mean_no_confidence = _temperature_value(rows_no_confidence, 0)

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

    rows_with_confidence = wvd_mod.run(
        obs, live_ts, conn_in=conn_in, all_obs=all_obs, weights=weights,
        member_history=member_history, default_matches=default_matches,
    )
    mean_with_confidence = _temperature_value(rows_with_confidence, 0)

    assert mean_with_confidence != mean_no_confidence


def test_missing_weight_drops_only_that_member():
    conn_in = make_input_db()
    all_obs, live_ts = _build_all_obs()
    obs = make_obs(ts=live_ts)
    sector = _sector(live_ts + _LEAD * 3600)

    # every member except member 3 (veer_gust_confirmed) gets a weight
    weights = {(1, "temperature", _LEAD, sector): 1.0,
               (2, "temperature", _LEAD, sector): 1.0}
    rows = wvd_mod.run(obs, live_ts, conn_in=conn_in, all_obs=all_obs, weights=weights)
    mean_row = next(r for r in rows if r["member_id"] == 0 and r["lead_hours"] == _LEAD and r["variable"] == "temperature")
    member_vals = {
        r["member_id"]: r["value"] for r in rows
        if r["lead_hours"] == _LEAD and r["variable"] == "temperature" and r["member_id"] != 0
        and r["value"] is not None
    }
    whole_group_average = sum(member_vals.values()) / len(member_vals)
    weighted_without_gust_confirmed = sum(v for mid, v in member_vals.items() if mid != 3) / (len(member_vals) - 1)

    assert abs(mean_row["value"] - weighted_without_gust_confirmed) < 1e-6
    assert abs(mean_row["value"] - whole_group_average) > 1e-6
