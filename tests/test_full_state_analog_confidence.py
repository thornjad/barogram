"""Confidence wiring for full_state_analog.py, same shape as analog.py's own
confidence tests: members differing only by K should genuinely differ in
value, confidence should shift member_id=0 versus weights alone, and a
missing weight should drop only that member."""
import time

import models.full_state_analog as fsa_mod
from tests.conftest import make_input_db, make_obs

_DAY = 86400


def _insert_obs(conn, ts: int, air_temp=20.0):
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


def _seed_candidates(conn_in, now, n=15):
    """n distinct historical days, air_temp offset by an increasing amount,
    and future (+6h) value offset the same way, so 'full-k5' (member 1) and
    'full-k10' (member 2) -- same feature set, different K -- genuinely
    average different subsets."""
    for d in range(1, n + 1):
        ts = now - d * _DAY
        _insert_obs(conn_in, ts, air_temp=20.0 + d * 0.1)
        _insert_obs(conn_in, ts + 6 * 3600, air_temp=20.0 + d)
    _insert_obs(conn_in, now, air_temp=20.0)


def test_k5_and_k10_members_differ():
    conn_in = make_input_db()
    now = int(time.time())
    _seed_candidates(conn_in, now)
    obs = make_obs(ts=now)

    rows = fsa_mod.run(obs, now, conn_in=conn_in)
    mean_k5 = next(r["value"] for r in rows if r["member_id"] == 1 and r["lead_hours"] == 6 and r["variable"] == "temperature")
    mean_k10 = next(r["value"] for r in rows if r["member_id"] == 2 and r["lead_hours"] == 6 and r["variable"] == "temperature")

    # nearest 5 days (offsets 1-5) -> future mean 20 + (1+..+5)/5 = 23.0
    # nearest 10 days (offsets 1-10) -> future mean 20 + (1+..+10)/10 = 25.5
    assert abs(mean_k5 - 23.0) < 1e-6
    assert abs(mean_k10 - 25.5) < 1e-6


def test_confidence_shifts_value_versus_weights_alone():
    conn_in = make_input_db()
    now = int(time.time())
    _seed_candidates(conn_in, now)
    obs = make_obs(ts=now)

    weights = {(mid, "temperature", 6): 1.0 for mid in fsa_mod._ALL_MEMBER_IDS}
    rows_no_confidence = fsa_mod.run(obs, now, conn_in=conn_in, weights=weights)
    mean_no_confidence = next(
        r["value"] for r in rows_no_confidence
        if r["member_id"] == 0 and r["lead_hours"] == 6 and r["variable"] == "temperature"
    )

    matched_days_k5 = [now - d * _DAY for d in range(1, 6)]
    matched_days_k10 = [now - d * _DAY for d in range(1, 11)]
    baseline_days = [now - d * _DAY for d in range(50, 80)]

    def _history(matched_days, matched_mae, baseline_mae):
        rows = [
            {"variable": "temperature", "lead_hours": 6, "issued_at": ts, "mae": matched_mae}
            for ts in matched_days
        ]
        rows += [
            {"variable": "temperature", "lead_hours": 6, "issued_at": ts, "mae": baseline_mae}
            for ts in baseline_days
        ]
        return rows

    member_history = {
        1: _history(matched_days_k5, matched_mae=0.1, baseline_mae=5.0),   # trusted
        2: _history(matched_days_k10, matched_mae=10.0, baseline_mae=5.0),  # distrusted
    }
    rows_with_confidence = fsa_mod.run(
        obs, now, conn_in=conn_in, weights=weights, member_history=member_history,
    )
    mean_with_confidence = next(
        r["value"] for r in rows_with_confidence
        if r["member_id"] == 0 and r["lead_hours"] == 6 and r["variable"] == "temperature"
    )

    assert mean_with_confidence != mean_no_confidence
    assert mean_with_confidence < mean_no_confidence  # pulled toward member 1's lower value


def test_missing_weight_drops_only_that_member():
    conn_in = make_input_db()
    now = int(time.time())
    _seed_candidates(conn_in, now)
    obs = make_obs(ts=now)

    weights = {
        (mid, "temperature", 6): 1.0
        for mid in fsa_mod._ALL_MEMBER_IDS if mid != 2
    }
    rows = fsa_mod.run(obs, now, conn_in=conn_in, weights=weights)
    mean_row = next(
        r for r in rows if r["member_id"] == 0 and r["lead_hours"] == 6 and r["variable"] == "temperature"
    )
    member_vals = {
        r["member_id"]: r["value"] for r in rows
        if r["lead_hours"] == 6 and r["variable"] == "temperature" and r["member_id"] != 0
        and r["value"] is not None
    }
    whole_group_average = sum(member_vals.values()) / len(member_vals)
    weighted_without_member_2 = sum(v for mid, v in member_vals.items() if mid != 2) / (len(member_vals) - 1)

    assert abs(mean_row["value"] - weighted_without_member_2) < 1e-6
    assert abs(mean_row["value"] - whole_group_average) > 1e-6
