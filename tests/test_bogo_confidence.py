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

    # 3:1 weights, each scaled by that member's own random confidence (floored like
    # combine_pattern does)
    c8 = max(bogo_mod._random_confidence(_ISSUED_AT, _WEATHERPERSON_MID, "temperature", _LEAD), 0.001)
    c12 = max(bogo_mod._random_confidence(_ISSUED_AT, _CLIMATE_ANXIETY_MID, "temperature", _LEAD), 0.001)
    expected = (3.0 * c8 * 15.0 + 1.0 * c12 * 18.0) / (3.0 * c8 + 1.0 * c12)
    assert abs(mean_row["value"] - expected) < 1e-9
    assert 15.0 <= mean_row["value"] <= 18.0


def test_each_member_states_its_own_random_confidence_and_it_weights_bogos_blend():
    """Random is bogo's algorithm for confidence: every member claims a seeded random
    value, and, as in every model, those confidences weight bogo's own member 0 blend."""
    conn_in = make_input_db()
    _seed_climo(conn_in)
    obs = make_obs(ts=_ISSUED_AT)
    sector = bogo_mod._sector(_VALID_AT)
    weights = {
        (_WEATHERPERSON_MID, "temperature", _LEAD, sector): 1.0,
        (_CLIMATE_ANXIETY_MID, "temperature", _LEAD, sector): 1.0,
    }

    rows = bogo_mod.run(obs, _ISSUED_AT, conn_in=conn_in, weights=weights)

    def row(mid):
        return next(r for r in rows if r["member_id"] == mid and r["lead_hours"] == _LEAD
                    and r["variable"] == "temperature")

    c8 = bogo_mod._random_confidence(_ISSUED_AT, _WEATHERPERSON_MID, "temperature", _LEAD)
    c12 = bogo_mod._random_confidence(_ISSUED_AT, _CLIMATE_ANXIETY_MID, "temperature", _LEAD)
    assert row(_WEATHERPERSON_MID)["confidence"] == c8
    assert row(_CLIMATE_ANXIETY_MID)["confidence"] == c12

    w8, w12 = max(c8, 0.001), max(c12, 0.001)     # combine_pattern floors confidence
    expected = (w8 * 15.0 + w12 * 18.0) / (w8 + w12)
    assert abs(row(0)["value"] - expected) < 1e-9


def test_random_confidence_is_in_range_reproducible_and_varies_by_member():
    draws = [bogo_mod._random_confidence(_ISSUED_AT, mid, "temperature", _LEAD) for mid in range(1, 70)]
    assert all(0.0 <= c <= 1.0 for c in draws)
    assert draws == [bogo_mod._random_confidence(_ISSUED_AT, mid, "temperature", _LEAD) for mid in range(1, 70)]
    assert len(set(draws)) > 30


def test_random_confidence_can_reach_both_ends_of_the_range():
    draws = {bogo_mod._random_confidence(t, 1, "temperature", 1) for t in range(30_000)}
    assert min(draws) == 0.0
    assert max(draws) == 1.0


def test_bogo_members_never_enter_the_calibration_log():
    import models._confidence as confidence
    conn_in = make_input_db()
    _seed_climo(conn_in)
    obs = make_obs(ts=_ISSUED_AT)
    history = {mid: confidence.CellHistory([], model_id=12, member_id=mid, now=_ISSUED_AT) for mid in range(0, 71)}

    bogo_mod.run(obs, _ISSUED_AT, conn_in=conn_in, weights={}, member_history=history, default_matches=[])

    assert confidence.pop_issue_log() == {}


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
    conf = {mid: max(bogo_mod._random_confidence(_ISSUED_AT, mid, "temperature", _LEAD), 0.001)
            for mid in member_vals}
    weighted_without_anxiety = sum(
        conf[mid] * v for mid, v in member_vals.items() if mid != _CLIMATE_ANXIETY_MID
    ) / sum(conf[mid] for mid in member_vals if mid != _CLIMATE_ANXIETY_MID)
    weighted_with_anxiety = sum(conf[mid] * v for mid, v in member_vals.items()) / sum(conf.values())

    assert abs(mean_row["value"] - weighted_without_anxiety) < 1e-6
    assert abs(mean_row["value"] - weighted_with_anxiety) > 1e-6


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
