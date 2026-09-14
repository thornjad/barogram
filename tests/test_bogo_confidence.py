"""Confidence wiring for bogo.py: real weights + non-uniform confidence should
shift member_id=0's value versus weights alone, a member missing a weight
should be dropped from the group (not fall the whole group back to a plain
average), and the pre-existing spread/dewpoint-clamp behavior in
_ensemble_mean must survive its signature change untouched."""
import datetime
import statistics

import models.bogo as bogo_mod
from tests.conftest import make_input_db, make_obs

_ISSUED_AT = 1_700_000_000  # fixed epoch for determinism
_LEAD = 6
_VALID_AT = _ISSUED_AT + _LEAD * 3600
_DAY = 86400

# member 8 (_weatherperson) reports the climo mean unmodified; member 12
# (_climate_anxiety) reports climo + 3.0 -- both pure functions of the climo
# bucket, with no randomness, so their values are exactly predictable
_WEATHERPERSON_MID = 8
_CLIMATE_ANXIETY_MID = 12


def _insert_obs(conn, ts, air_temp, dew_point, pressure):
    conn.execute(
        """
        insert into tempest_obs
            (station_id, timestamp, air_temp, dew_point, station_pressure,
             wind_avg, wind_gust, wind_direction, precip_accum_day,
             solar_radiation, uv_index, lightning_count)
        values ('KTEST', ?, ?, ?, ?, null, null, null, 0.0, 0.0, 0.0, 0)
        """,
        (ts, air_temp, dew_point, pressure),
    )


def _seed_climo(conn_in, n=35, air_temp=15.0, dew_point=5.0, pressure=1010.0):
    """n identical obs in the (month, hour) bucket _VALID_AT falls in, so
    climo_bucket_means (MIN_OBS=30) resolves to an exact, known mean."""
    dt = datetime.datetime.fromtimestamp(_VALID_AT)
    ts = int(datetime.datetime(2020, dt.month, 15, dt.hour, 0, 0).timestamp())
    for _ in range(n):
        _insert_obs(conn_in, ts, air_temp, dew_point, pressure)


def test_higher_weighted_member_pulls_mean_toward_own_value():
    conn_in = make_input_db()
    _seed_climo(conn_in)
    obs = make_obs(ts=_ISSUED_AT)
    sector = bogo_mod._sector(_VALID_AT)

    # only members 8 and 12 get a weight -- every other member lacks one and
    # is dropped from the group by combine_pattern
    weights = {
        (_WEATHERPERSON_MID, "temperature", _LEAD, sector): 3.0,
        (_CLIMATE_ANXIETY_MID, "temperature", _LEAD, sector): 1.0,
    }
    rows = bogo_mod.run(obs, _ISSUED_AT, conn_in=conn_in, weights=weights)
    weatherperson_val = next(
        r["value"] for r in rows
        if r["member_id"] == _WEATHERPERSON_MID and r["lead_hours"] == _LEAD and r["variable"] == "temperature"
    )
    anxiety_val = next(
        r["value"] for r in rows
        if r["member_id"] == _CLIMATE_ANXIETY_MID and r["lead_hours"] == _LEAD and r["variable"] == "temperature"
    )
    mean_row = next(
        r for r in rows if r["member_id"] == 0 and r["lead_hours"] == _LEAD and r["variable"] == "temperature"
    )

    assert weatherperson_val == 15.0
    assert anxiety_val == 18.0

    equal_avg = (weatherperson_val + anxiety_val) / 2
    # 3:1 weighting with no confidence data (effective weight 0.5 each side):
    # (3*0.5*15 + 1*0.5*18) / (3*0.5 + 1*0.5) = 15.75
    assert abs(mean_row["value"] - 15.75) < 1e-9
    assert mean_row["value"] < equal_avg  # pulled toward the heavier-weighted member
    assert abs(mean_row["value"] - weatherperson_val) < abs(mean_row["value"] - anxiety_val)


def test_confidence_shifts_value_versus_weights_alone():
    conn_in = make_input_db()
    _seed_climo(conn_in)
    obs = make_obs(ts=_ISSUED_AT)
    sector = bogo_mod._sector(_VALID_AT)

    weights = {
        (_WEATHERPERSON_MID, "temperature", _LEAD, sector): 1.0,
        (_CLIMATE_ANXIETY_MID, "temperature", _LEAD, sector): 1.0,
    }
    rows_no_confidence = bogo_mod.run(obs, _ISSUED_AT, conn_in=conn_in, weights=weights)
    mean_no_confidence = next(
        r["value"] for r in rows_no_confidence
        if r["member_id"] == 0 and r["lead_hours"] == _LEAD and r["variable"] == "temperature"
    )

    matched_days = [10_000_000 + d * _DAY for d in range(10)]
    baseline_days = [10_000_000 + d * _DAY for d in range(50, 80)]  # not in default_matches

    def _history(matched_mae, baseline_mae):
        rows = [{"variable": "temperature", "lead_hours": _LEAD, "issued_at": ts, "mae": matched_mae} for ts in matched_days]
        rows += [{"variable": "temperature", "lead_hours": _LEAD, "issued_at": ts, "mae": baseline_mae} for ts in baseline_days]
        return rows

    member_history = {
        _WEATHERPERSON_MID: _history(matched_mae=0.1, baseline_mae=5.0),    # trusted
        _CLIMATE_ANXIETY_MID: _history(matched_mae=10.0, baseline_mae=5.0),  # distrusted
    }
    default_matches = matched_days

    rows_with_confidence = bogo_mod.run(
        obs, _ISSUED_AT, conn_in=conn_in, weights=weights,
        member_history=member_history, default_matches=default_matches,
    )
    mean_with_confidence = next(
        r["value"] for r in rows_with_confidence
        if r["member_id"] == 0 and r["lead_hours"] == _LEAD and r["variable"] == "temperature"
    )

    # weatherperson (15.0, now trusted) should count more than climate_anxiety
    # (18.0, now distrusted), pulling the mean down from the equal-confidence 16.5
    assert mean_with_confidence != mean_no_confidence
    assert mean_with_confidence < mean_no_confidence


def test_missing_weight_drops_only_that_member():
    conn_in = make_input_db()
    _seed_climo(conn_in)
    obs = make_obs(ts=_ISSUED_AT)
    sector = bogo_mod._sector(_VALID_AT)

    # every member except climate_anxiety gets a weight
    weights = {
        (mid, "temperature", _LEAD, sector): 1.0
        for mid in range(1, 70) if mid != _CLIMATE_ANXIETY_MID
    }
    rows = bogo_mod.run(obs, _ISSUED_AT, conn_in=conn_in, weights=weights)
    mean_row = next(r for r in rows if r["member_id"] == 0 and r["lead_hours"] == _LEAD and r["variable"] == "temperature")
    member_vals = {
        r["member_id"]: r["value"] for r in rows
        if r["lead_hours"] == _LEAD and r["variable"] == "temperature" and r["member_id"] != 0
        and r["value"] is not None
    }
    whole_group_average = sum(member_vals.values()) / len(member_vals)
    weighted_without_anxiety = sum(v for mid, v in member_vals.items() if mid != _CLIMATE_ANXIETY_MID) / (
        len(member_vals) - 1
    )

    assert abs(mean_row["value"] - weighted_without_anxiety) < 1e-6
    assert abs(mean_row["value"] - whole_group_average) > 1e-6


def test_spread_uses_stdev_not_pstdev():
    """The exact pre-existing spread formula must survive the signature
    change: statistics.stdev (sample stdev), never pstdev (population)."""
    members = {1: {(_LEAD, "temperature"): 10.0}, 2: {(_LEAD, "temperature"): 20.0}}
    member_confidence = {1: {}, 2: {}}

    result, _ = bogo_mod._ensemble_mean(members, member_confidence, None, 0)
    mean, spread = result[(_LEAD, "temperature")]

    assert mean == 15.0
    assert spread == statistics.stdev([10.0, 20.0])
    assert spread != statistics.pstdev([10.0, 20.0])


def test_spread_zero_not_none_with_fewer_than_two_members():
    """Fewer than 2 valid members must yield spread 0.0, never None."""
    members = {1: {(_LEAD, "temperature"): 10.0}}
    member_confidence = {1: {}}

    result, _ = bogo_mod._ensemble_mean(members, member_confidence, None, 0)
    mean, spread = result[(_LEAD, "temperature")]

    assert mean == 10.0
    assert spread == 0.0
    assert spread is not None


def test_dewpoint_clamp_still_applied_after_signature_change():
    """The second loop's _cdp clamp (untouched by this task) must still see
    and correct an out-of-order dewpoint/temperature mean."""
    members = {
        1: {(_LEAD, "temperature"): 10.0, (_LEAD, "dewpoint"): 15.0},
        2: {(_LEAD, "temperature"): 10.0, (_LEAD, "dewpoint"): 15.0},
    }
    member_confidence = {1: {}, 2: {}}

    result, _ = bogo_mod._ensemble_mean(members, member_confidence, None, 0)
    mean_t, _ = result[(_LEAD, "temperature")]
    mean_d, spread_d = result[(_LEAD, "dewpoint")]

    assert mean_t == 10.0
    assert mean_d == 10.0  # clamped down from the raw dewpoint mean of 15.0
    assert spread_d == 0.0  # spread itself is untouched by the clamp
