"""Confidence wiring for climo_deviation.py: real weights + non-uniform
confidence should shift member_id=0's value versus weights alone, and a
missing weight should drop only that member, not the whole group."""
import datetime

import models.climo_deviation as cd_mod
from tests.conftest import make_input_db, make_obs

_ISSUED_AT = 1_700_000_000  # fixed epoch for determinism
_LEAD = 6
_VALID_AT = _ISSUED_AT + _LEAD * 3600


def _insert_climo_obs(conn, month, hour, air_temp):
    """Insert a synthetic obs whose OWN timestamp falls in the given
    (month, hour) bucket, matching climo_bucket_obs's own strftime lookup."""
    # any real date with this month works; use day 15 for safety
    dt = datetime.datetime(2020, month, 15, hour, 0, 0)
    ts = int(dt.timestamp())
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


def _seed_buckets(conn_in):
    now = datetime.datetime.fromtimestamp(_ISSUED_AT)
    valid_at_dt = datetime.datetime.fromtimestamp(_VALID_AT)
    # a real deviation: "now" bucket differs from obs value, and the future
    # bucket has its own distinct value, so static vs decay members
    # (which shrink the deviation's contribution via exp(-k*lead)) differ
    _insert_climo_obs(conn_in, now.month, now.hour, air_temp=15.0)
    _insert_climo_obs(conn_in, valid_at_dt.month, valid_at_dt.hour, air_temp=25.0)


def _static_and_decay_member_ids():
    # member_id 1 = first static member (offset 0); member_id 10 = first
    # decay-k03 member (offset 9), per _GROUPS/_BASE_MEMBERS ordering
    static_mid = cd_mod._GROUPS[0][0] + cd_mod._BASE_MEMBERS[0][0]
    decay_mid = cd_mod._GROUPS[1][0] + cd_mod._BASE_MEMBERS[0][0]
    return static_mid, decay_mid


def test_static_and_decay_members_differ():
    conn_in = make_input_db()
    _seed_buckets(conn_in)
    obs = make_obs(ts=_ISSUED_AT)
    obs["air_temp"] = 20.0  # obs differs from the "now" bucket mean (15.0) -> real deviation

    rows = cd_mod.run(obs, _ISSUED_AT, conn_in=conn_in)
    static_mid, decay_mid = _static_and_decay_member_ids()
    static_val = next(r["value"] for r in rows if r["member_id"] == static_mid and r["lead_hours"] == _LEAD and r["variable"] == "temperature")
    decay_val = next(r["value"] for r in rows if r["member_id"] == decay_mid and r["lead_hours"] == _LEAD and r["variable"] == "temperature")

    assert static_val is not None and decay_val is not None
    assert static_val != decay_val


def test_confidence_shifts_value_versus_weights_alone():
    conn_in = make_input_db()
    _seed_buckets(conn_in)
    obs = make_obs(ts=_ISSUED_AT)
    obs["air_temp"] = 20.0
    static_mid, decay_mid = _static_and_decay_member_ids()
    sector = cd_mod._sector(_VALID_AT)

    weights = {(static_mid, "temperature", _LEAD, sector): 1.0,
               (decay_mid, "temperature", _LEAD, sector): 1.0}
    rows_no_confidence = cd_mod.run(obs, _ISSUED_AT, conn_in=conn_in, weights=weights)
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
        static_mid: _history(matched_mae=0.1, baseline_mae=5.0),
        decay_mid: _history(matched_mae=10.0, baseline_mae=5.0),
    }
    default_matches = matched_days

    rows_with_confidence = cd_mod.run(
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
    _seed_buckets(conn_in)
    obs = make_obs(ts=_ISSUED_AT)
    obs["air_temp"] = 20.0
    static_mid, decay_mid = _static_and_decay_member_ids()
    sector = cd_mod._sector(_VALID_AT)
    all_member_ids = [offset + mid for offset, k, amp, prefix in cd_mod._GROUPS for mid, _, _ in cd_mod._BASE_MEMBERS]

    # every member except decay_mid gets a weight
    weights = {
        (mid, "temperature", _LEAD, sector): 1.0
        for mid in all_member_ids if mid != decay_mid
    }
    rows = cd_mod.run(obs, _ISSUED_AT, conn_in=conn_in, weights=weights)
    mean_row = next(r for r in rows if r["member_id"] == 0 and r["lead_hours"] == _LEAD and r["variable"] == "temperature")
    member_vals = {
        r["member_id"]: r["value"] for r in rows
        if r["lead_hours"] == _LEAD and r["variable"] == "temperature" and r["member_id"] != 0
        and r["value"] is not None
    }
    whole_group_average = sum(member_vals.values()) / len(member_vals)
    weighted_without_decay = sum(v for mid, v in member_vals.items() if mid != decay_mid) / (len(member_vals) - 1)

    assert abs(mean_row["value"] - weighted_without_decay) < 1e-6
    assert abs(mean_row["value"] - whole_group_average) > 1e-6
