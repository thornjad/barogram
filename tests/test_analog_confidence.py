"""Confidence wiring for analog.py: real weights + non-uniform confidence
should shift member_id=0's value versus weights alone, and a member missing
a weight should be dropped from the group, not fall the whole group back to
a plain average."""
import time

import models.analog as analog_mod
from tests.conftest import make_input_db, make_obs

_DAY = 86400


def _insert_obs(conn, ts: int, air_temp=20.0, dew_point=12.0,
                station_pressure=1013.0, wind_avg=3.0):
    conn.execute(
        """
        insert into tempest_obs
            (station_id, timestamp, air_temp, dew_point, station_pressure,
             wind_avg, wind_gust, wind_direction, precip_accum_day,
             solar_radiation, uv_index, lightning_count)
        values ('KTEST', ?, ?, ?, ?, ?, null, null, 0.0, 0.0, 0.0, 0)
        """,
        (ts, air_temp, dew_point, station_pressure, wind_avg),
    )


def _seed_candidates(conn_in, now):
    """20 distinct historical days, each offset from today's obs by a
    deterministic, increasing amount, and each day's future (+6h) value
    offset the same way -- so member 'k3' (nearest 3 by distance) and
    member 'k20' (nearest 20, i.e. all of them) average genuinely
    different subsets, giving different member values to weight between."""
    for d in range(1, 21):
        ts = now - d * _DAY
        _insert_obs(conn_in, ts, air_temp=20.0 + d * 0.1)
        _insert_obs(conn_in, ts + 6 * 3600, air_temp=20.0 + d)
    _insert_obs(conn_in, now, air_temp=20.0)


def _sector(valid_at):
    from models._utils import _sector as s
    return s(valid_at)


def test_weighted_mean_shifts_between_k3_and_k20_members():
    conn_in = make_input_db()
    now = int(time.time())
    _seed_candidates(conn_in, now)
    obs = make_obs(ts=now)
    valid_at = now + 6 * 3600
    sector = _sector(valid_at)

    # weight member 1 (k3, nearest-3 average) heavily, member 4 (k20) not at all
    weights_k3 = {(1, "temperature", 6, sector): 1.0, (4, "temperature", 6, sector): 0.0}
    rows_k3 = analog_mod.run(obs, now, conn_in=conn_in, weights=weights_k3)
    mean_k3 = next(r["value"] for r in rows_k3 if r["member_id"] == 1 and r["lead_hours"] == 6 and r["variable"] == "temperature")
    mean_k20 = next(r["value"] for r in rows_k3 if r["member_id"] == 4 and r["lead_hours"] == 6 and r["variable"] == "temperature")

    # nearest-3 days are offsets 1,2,3 -> future values 21,22,23 -> mean 22.0
    # nearest-20 (all) days are offsets 1..20 -> future mean is 20 + (1+..+20)/20 = 30.5
    assert abs(mean_k3 - 22.0) < 1e-6
    assert abs(mean_k20 - 30.5) < 1e-6
    assert mean_k3 != mean_k20  # confirms these two members really do differ


def test_confidence_shifts_value_versus_weights_alone():
    conn_in = make_input_db()
    now = int(time.time())
    _seed_candidates(conn_in, now)
    obs = make_obs(ts=now)
    valid_at = now + 6 * 3600
    sector = _sector(valid_at)

    # equal weights between member 1 (k3) and member 4 (k20); every other
    # member gets a weight too so the "any missing" fallback never fires
    weights = {(mid, "temperature", 6, sector): 1.0 for mid in analog_mod._ALL_MEMBER_IDS}

    rows_no_confidence = analog_mod.run(obs, now, conn_in=conn_in, weights=weights)
    mean_no_confidence = next(
        r["value"] for r in rows_no_confidence
        if r["member_id"] == 0 and r["lead_hours"] == 6 and r["variable"] == "temperature"
    )

    # member 1 (k3) only ever gets 3 matched-day samples (it matches analog
    # days at offsets 1,2,3, by construction); member 4 (k20) always gets 20
    # (offsets 1..20). Confidence now shrinks TOWARD ZERO as evidence thins,
    # not toward a neutral 0.5 -- so a member with only 3 samples can't win
    # on quality alone, thin evidence is muted regardless of which way it
    # points. Give member 4 (more evidence) the GOOD track record and member
    # 1 (less evidence) the BAD one, so volume and quality reinforce the same
    # direction instead of fighting each other on a knife's edge.
    matched_days_k3 = [now - d * _DAY for d in [1, 2, 3]]
    matched_days_k20 = [now - d * _DAY for d in range(1, 21)]
    baseline_days = [now - d * _DAY for d in range(50, 80)]  # not selected by any member

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
        1: _history(matched_days_k3, matched_mae=10.0, baseline_mae=5.0),   # thin AND bad
        4: _history(matched_days_k20, matched_mae=0.1, baseline_mae=5.0),   # thick AND good
    }

    rows_with_confidence = analog_mod.run(
        obs, now, conn_in=conn_in, weights=weights, member_history=member_history,
    )
    mean_with_confidence = next(
        r["value"] for r in rows_with_confidence
        if r["member_id"] == 0 and r["lead_hours"] == 6 and r["variable"] == "temperature"
    )

    # member 4's value (30.5, from k20) should now count more than before,
    # since it has both far more evidence AND better quality than member 1
    # (22.0, from k3, thin and bad)
    assert mean_with_confidence != mean_no_confidence
    assert mean_with_confidence > mean_no_confidence  # pulled toward member 4's higher value


def test_missing_weight_drops_only_that_member():
    """The pre-existing behavior fix, independent of confidence: a member
    missing a weight is dropped, not the whole group falling back to a
    plain average."""
    conn_in = make_input_db()
    now = int(time.time())
    _seed_candidates(conn_in, now)
    obs = make_obs(ts=now)
    valid_at = now + 6 * 3600
    sector = _sector(valid_at)

    # every member except member 4 gets a weight
    weights = {
        (mid, "temperature", 6, sector): 1.0
        for mid in analog_mod._ALL_MEMBER_IDS if mid != 4
    }
    rows = analog_mod.run(obs, now, conn_in=conn_in, weights=weights)
    mean_row = next(
        r for r in rows if r["member_id"] == 0 and r["lead_hours"] == 6 and r["variable"] == "temperature"
    )
    member_vals = {
        r["member_id"]: r["value"] for r in rows
        if r["lead_hours"] == 6 and r["variable"] == "temperature" and r["member_id"] != 0
    }
    whole_group_average = sum(member_vals.values()) / len(member_vals)
    weighted_without_member_4 = sum(v for mid, v in member_vals.items() if mid != 4) / (len(member_vals) - 1)

    # member 4 (k20, an outlier at 30.5) is dropped, not averaged in with equal
    # weight, so the result matches the group minus member 4, not everyone
    assert abs(mean_row["value"] - weighted_without_member_4) < 1e-6
    assert abs(mean_row["value"] - whole_group_average) > 1e-6
