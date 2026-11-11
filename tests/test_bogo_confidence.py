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


def _fake_own_confidence(monkeypatch):
    """pin each member's confidence to a known number so the blend math can be checked exactly"""
    def conf(mid):
        return 0.1 + mid / 100.0
    monkeypatch.setattr(bogo_mod, "_own_confidence",
                        lambda member_history, default_matches, mid, var, lead: conf(mid))
    return conf


def test_higher_weighted_member_pulls_mean_toward_own_value(monkeypatch):
    conf = _fake_own_confidence(monkeypatch)
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
    mean_row = next(
        r for r in rows if r["member_id"] == 0 and r["lead_hours"] == _LEAD and r["variable"] == "temperature"
    )
    # 3:1 weights, each scaled by that member's own confidence
    c8, c12 = conf(_WEATHERPERSON_MID), conf(_CLIMATE_ANXIETY_MID)
    expected = (3.0 * c8 * 15.0 + 1.0 * c12 * 18.0) / (3.0 * c8 + 1.0 * c12)
    assert abs(mean_row["value"] - expected) < 1e-9
    assert 15.0 <= mean_row["value"] <= 18.0


def test_member_confidences_are_stated_on_the_rows_and_member_zero_has_its_own(monkeypatch):
    _fake_own_confidence(monkeypatch)
    conn_in = make_input_db()
    _seed_climo(conn_in)
    obs = make_obs(ts=_ISSUED_AT)

    rows = bogo_mod.run(obs, _ISSUED_AT, conn_in=conn_in, weights={})

    for r in rows:
        assert r["confidence"] == 0.1 + r["member_id"] / 100.0   # member 0 is its own, not an average


def test_random_matches_is_a_random_subset_of_the_shared_matches_with_similarity_one():
    import random
    matches = [(1_000 * i, 0.3) for i in range(1, 21)]
    random.seed(3)
    sizes = set()
    for _ in range(400):
        picked = bogo_mod._random_matches(matches)
        sizes.add(len(picked))
        assert {ts for ts, _ in picked} <= {ts for ts, _ in matches}
        assert len({ts for ts, _ in picked}) == len(picked)
        assert all(sim == 1.0 for _, sim in picked)
    assert 0 in sizes and 20 in sizes and len(sizes) > 10   # empty, full and everything between


def test_random_matches_differ_between_calls_unless_a_test_seeds_them():
    import random
    matches = [(1_000 * i, 1.0) for i in range(1, 100)]
    assert any(bogo_mod._random_matches(matches) != bogo_mod._random_matches(matches) for _ in range(20))
    random.seed(11)
    first = bogo_mod._random_matches(matches)
    random.seed(11)
    assert bogo_mod._random_matches(matches) == first


def test_bogo_confidence_comes_from_its_record_on_the_matches_it_picked(monkeypatch):
    import models._confidence as confidence
    base = 10_000_000
    base -= base % 86400
    matched = [(base + d * 86400 + 3600, 0.9) for d in range(60)]
    good = confidence.CellHistory([{"variable": "temperature", "lead_hours": _LEAD, "issued_at": ts + 600, "mae": 0.0}
                                   for ts, _ in matched])
    bad = confidence.CellHistory([{"variable": "temperature", "lead_hours": _LEAD, "issued_at": ts + 600, "mae": 50.0}
                                  for ts, _ in matched])
    monkeypatch.setattr(bogo_mod, "_random_matches", lambda m: [(ts, 1.0) for ts, _ in m])   # pick everything

    assert bogo_mod._own_confidence({5: good}, matched, 5, "temperature", _LEAD) > 0.0
    assert bogo_mod._own_confidence({5: bad}, matched, 5, "temperature", _LEAD) < 1e-6
    assert bogo_mod._own_confidence({}, matched, 5, "temperature", _LEAD) == 0.0   # no record, no confidence


def test_bogo_member_zero_uses_its_own_history(monkeypatch):
    import models._confidence as confidence
    conn_in = make_input_db()
    _seed_climo(conn_in)
    obs = make_obs(ts=_ISSUED_AT)
    base = 10_000_000
    base -= base % 86400
    matched = [(base + d * 86400 + 3600, 1.0) for d in range(60)]
    zero_hist = confidence.CellHistory([{"variable": "temperature", "lead_hours": _LEAD, "issued_at": ts + 600, "mae": 0.0}
                                        for ts, _ in matched])
    monkeypatch.setattr(bogo_mod, "_random_matches", lambda m: list(m))
    history = {0: zero_hist}

    rows = bogo_mod.run(obs, _ISSUED_AT, conn_in=conn_in, weights={}, member_history=history, default_matches=matched)

    zero = next(r for r in rows if r["member_id"] == 0 and r["lead_hours"] == _LEAD and r["variable"] == "temperature")
    other = next(r for r in rows if r["member_id"] == 5 and r["lead_hours"] == _LEAD and r["variable"] == "temperature")
    assert zero["confidence"] > 0.0 and other["confidence"] == 0.0


def test_bogo_runs_without_history_or_matches():
    conn_in = make_input_db()
    _seed_climo(conn_in)
    rows = bogo_mod.run(make_obs(ts=_ISSUED_AT), _ISSUED_AT, conn_in=conn_in, weights={})
    assert rows and all(0.0 <= r["confidence"] <= 1.0 for r in rows)


def test_missing_weight_drops_only_that_member(monkeypatch):
    conf = _fake_own_confidence(monkeypatch)
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
    weighted_without_anxiety = sum(
        conf(mid) * v for mid, v in member_vals.items() if mid != _CLIMATE_ANXIETY_MID
    ) / sum(conf(mid) for mid in member_vals if mid != _CLIMATE_ANXIETY_MID)
    weighted_with_anxiety = sum(conf(mid) * v for mid, v in member_vals.items()) / sum(conf(mid) for mid in member_vals)

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
