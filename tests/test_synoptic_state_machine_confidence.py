"""Confidence wiring for synoptic_state_machine.py: real weights + non-uniform
confidence should shift member_id=0's value versus weights alone, and a
missing weight should drop only that member, not the whole group.

Uses member 8 (moisture-only, state=(dp_trend,)) and member 9
(convective-only, state=(conv,), which always resolves since lightning and
precip stay zero throughout) -- the two simplest members to drive with
synthetic data, needing no wind or solar signals."""
import models.synoptic_state_machine as ssm_mod
from tests.conftest import make_input_db, make_obs

_STEP = 3 * 3600  # 3h spacing matches this model's signal-lookback windows
_N_A = 20         # "narrowing" dewpoint-spread era: indices 0..19
_N_B = 20         # "widening" dewpoint-spread era: indices 20..39
_ISSUED_AT = 1_700_000_000
_LEAD = 6
_MOISTURE_ONLY = 8
_CONVECTIVE_ONLY = 9


def _insert_obs(conn, ts, air_temp, dew_point=10.0):
    conn.execute(
        """
        insert into tempest_obs
            (station_id, timestamp, air_temp, dew_point, station_pressure,
             wind_avg, wind_gust, wind_direction, precip_accum_day,
             solar_radiation, uv_index, lightning_count)
        values ('KTEST', ?, ?, ?, 1013.0, 3.0, null, null, 0.0, 0.0, 0.0, 0)
        """,
        (ts, air_temp, dew_point),
    )


def _seed_history(conn_in):
    """40 points at 3h spacing ending 3h before _ISSUED_AT: air_temp drops
    2 degrees/step for the first 20 points (dewpoint spread "narrowing",
    since dew_point stays fixed at 10.0), then rises 2 degrees/step for the
    next 20 ("widening"). precip/lightning stay zero throughout, so member 9
    (convective-only) sees the same "dry" state across both eras and
    averages a near-cancelling delta, while member 8 (moisture-only) keys
    off the live "widening" state and averages only the second era.
    Returns the last historical air_temp value (era B's endpoint)."""
    temp = 100.0
    last_inserted = temp
    for i in range(_N_A + _N_B):
        ts = _ISSUED_AT - (_N_A + _N_B - i) * _STEP
        _insert_obs(conn_in, ts, air_temp=temp)
        last_inserted = temp
        temp += -2.0 if i < _N_A else 2.0
    return last_inserted


def _make_live_obs(last_hist_temp):
    obs = make_obs(ts=_ISSUED_AT)
    obs["air_temp"] = last_hist_temp + 2.0  # continues the widening trend
    obs["dew_point"] = 10.0
    obs["station_pressure"] = 1013.0
    obs["precip_accum_day"] = 0.0
    obs["lightning_count"] = 0
    obs["solar_radiation"] = 0.0
    return obs


def test_moisture_and_convective_members_differ():
    conn_in = make_input_db()
    last_hist_temp = _seed_history(conn_in)
    obs = _make_live_obs(last_hist_temp)

    rows = ssm_mod.run(obs, _ISSUED_AT, conn_in=conn_in)
    moisture_val = next(r["value"] for r in rows if r["member_id"] == _MOISTURE_ONLY and r["lead_hours"] == _LEAD and r["variable"] == "temperature")
    conv_val = next(r["value"] for r in rows if r["member_id"] == _CONVECTIVE_ONLY and r["lead_hours"] == _LEAD and r["variable"] == "temperature")

    assert moisture_val is not None and conv_val is not None
    assert abs(moisture_val - conv_val) > 1.0


def test_confidence_shifts_value_versus_weights_alone():
    conn_in = make_input_db()
    last_hist_temp = _seed_history(conn_in)
    obs = _make_live_obs(last_hist_temp)
    valid_at = _ISSUED_AT + _LEAD * 3600
    sector = ssm_mod._sector(valid_at)

    weights = {(_MOISTURE_ONLY, "temperature", _LEAD, sector): 1.0,
               (_CONVECTIVE_ONLY, "temperature", _LEAD, sector): 1.0}
    rows_no_confidence = ssm_mod.run(obs, _ISSUED_AT, conn_in=conn_in, weights=weights)
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
        _MOISTURE_ONLY: _history(matched_mae=0.1, baseline_mae=5.0),
        _CONVECTIVE_ONLY: _history(matched_mae=10.0, baseline_mae=5.0),
    }
    default_matches = matched_days

    rows_with_confidence = ssm_mod.run(
        obs, _ISSUED_AT, conn_in=conn_in, weights=weights,
        member_history=member_history, default_matches=default_matches,
    )
    mean_with_confidence = next(
        r["value"] for r in rows_with_confidence
        if r["member_id"] == 0 and r["lead_hours"] == _LEAD and r["variable"] == "temperature"
    )

    assert mean_with_confidence != mean_no_confidence


def test_missing_weight_drops_only_that_member():
    conn_in = make_input_db()
    last_hist_temp = _seed_history(conn_in)
    obs = _make_live_obs(last_hist_temp)
    valid_at = _ISSUED_AT + _LEAD * 3600
    sector = ssm_mod._sector(valid_at)

    # only the moisture-only member gets a weight; convective-only is dropped
    weights = {(_MOISTURE_ONLY, "temperature", _LEAD, sector): 1.0}
    rows = ssm_mod.run(obs, _ISSUED_AT, conn_in=conn_in, weights=weights)
    mean_row = next(r for r in rows if r["member_id"] == 0 and r["lead_hours"] == _LEAD and r["variable"] == "temperature")
    moisture_val = next(r["value"] for r in rows if r["member_id"] == _MOISTURE_ONLY and r["lead_hours"] == _LEAD and r["variable"] == "temperature")
    conv_val = next(r["value"] for r in rows if r["member_id"] == _CONVECTIVE_ONLY and r["lead_hours"] == _LEAD and r["variable"] == "temperature")

    assert abs(mean_row["value"] - moisture_val) < 1e-6
    assert abs(mean_row["value"] - (moisture_val + conv_val) / 2) > 1e-6
