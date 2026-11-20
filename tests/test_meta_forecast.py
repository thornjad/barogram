import math

import pytest

import db
import models._confidence as _confidence
import models.meta_forecast as meta
from models._utils import _sector
from tests.conftest import make_obs, make_output_db

_ISSUED_AT = 1_700_000_000
_HOUR = 3600


@pytest.fixture
def conn():
    """Output DB with model 300 and its static members registered, as migration
    does later. The migration inserts with `insert or ignore`, so this does not
    conflict with it. Foreign keys are on, so the models row must come first."""
    c = make_output_db()
    c.execute("insert or ignore into models (id, name, type) values (300, 'meta_forecast', 'meta')")
    c.execute("insert or ignore into members (model_id, member_id, name) values (300, 0, null)")
    c.execute("insert or ignore into members (model_id, member_id, name) values (300, -1, 'raw_mix')")
    for member_id, name in meta.STATIC_MEMBERS.items():
        c.execute(
            "insert or ignore into members (model_id, member_id, name) values (300, ?, ?)",
            (member_id, name),
        )
    return c


def test_static_registry_ids_and_names_are_unique_and_positive():
    ids = list(meta.STATIC_MEMBERS)
    names = list(meta.STATIC_MEMBERS.values())
    assert len(ids) == len(set(ids))
    assert len(names) == len(set(names))
    assert all(i > 0 for i in ids)


def test_static_registry_sources_exist_in_models_table(conn):
    model_ids = {r["id"] for r in conn.execute("select id from models")}
    for member_id, (source_model_id, _name) in meta.PASS_THROUGH_SOURCES.items():
        assert member_id in meta.STATIC_MEMBERS
        assert source_model_id in model_ids


def test_static_registry_self_correction_ids():
    assert meta.STATIC_MEMBERS[meta.SELF_CORRECTION_MEMBER] == "self_correction"
    assert meta.STATIC_MEMBERS[meta.SELF_CORRECTION_FAST_MEMBER] == "self_correction_fast"
    assert (meta.SELF_CORRECTION_MEMBER, meta.SELF_CORRECTION_FAST_MEMBER) == (91, 92)


def test_derived_members_are_registered():
    for member_id in meta.DERIVED_MEMBERS:
        assert member_id in meta.STATIC_MEMBERS


def test_base_pass_through_ids_follow_100_plus_model_id():
    assert meta.base_member_id(1) == 101
    assert meta.base_member_id(27) == 127


# ---------------------------------------------------------------------------
# core: pass-throughs, registration, cell valid_at, member 0, SC rows
# ---------------------------------------------------------------------------

def _seed(conn, model_id, variable, value, lead=6, *, valid_at=None, confidence=None, spread=None,
          issued_at=_ISSUED_AT):
    name = conn.execute("select name from models where id = ?", (model_id,)).fetchone()["name"]
    valid_at = issued_at + lead * _HOUR if valid_at is None else valid_at
    conn.execute(
        """
        insert into forecasts
            (model_id, model, member_id, issued_at, valid_at, lead_hours, variable, value, spread, confidence)
        values (?, ?, 0, ?, ?, ?, ?, ?, ?, ?)
        """,
        (model_id, name, issued_at, valid_at, lead, variable, value, spread, confidence),
    )


def _run(conn, weights=None):
    return meta.run(make_obs(_ISSUED_AT), _ISSUED_AT, conn_out=conn, weights=weights)


def _member(rows, member_id, variable="temperature", lead=6):
    found = [r for r in rows if r["member_id"] == member_id and r["variable"] == variable
             and r["lead_hours"] == lead]
    assert len(found) == 1, (member_id, variable, lead, len(found))
    return found[0]


def _members(rows, variable="temperature", lead=6):
    return {r["member_id"] for r in rows if r["variable"] == variable and r["lead_hours"] == lead}


def test_no_inputs_returns_no_rows(conn):
    assert _run(conn) == []


def test_pass_through_rows_carry_source_value_valid_at_and_confidence(conn):
    _seed(conn, 202, "temperature", 21.0, confidence=0.4, valid_at=_ISSUED_AT + 6 * _HOUR)
    _seed(conn, 100, "temperature", 19.0, confidence=0.2, valid_at=_ISSUED_AT + 6 * _HOUR + 600)

    rows = _run(conn)

    c202 = _member(rows, 3)
    ens = _member(rows, 4)
    assert (c202["value"], c202["confidence"], c202["valid_at"]) == (21.0, 0.4, _ISSUED_AT + 6 * _HOUR)
    assert (ens["value"], ens["confidence"], ens["valid_at"]) == (19.0, 0.2, _ISSUED_AT + 6 * _HOUR + 600)
    for r in rows:
        assert r["model_id"] == 300 and r["model"] == "meta_forecast"
        assert r["issued_at"] == _ISSUED_AT and r["lead_hours"] == 6


def test_persistence_emits_every_variable_and_other_base_models_only_pressure(conn):
    for variable, value in (("temperature", 20.0), ("dewpoint", 12.0), ("pressure", 1013.0)):
        _seed(conn, 1, variable, value)       # persistence
        _seed(conn, 5, variable, value + 1)   # pressure_tendency

    rows = _run(conn)

    assert 101 in _members(rows, "temperature") and 101 in _members(rows, "dewpoint")
    assert 101 in _members(rows, "pressure")
    assert 105 not in _members(rows, "temperature") and 105 not in _members(rows, "dewpoint")
    assert 105 in _members(rows, "pressure")


def test_run_registers_exactly_the_emitted_base_pass_throughs(conn):
    _seed(conn, 1, "temperature", 20.0)        # persistence: emitted
    _seed(conn, 2, "pressure", 1012.0)         # emitted
    _seed(conn, 6, "temperature", 21.0)        # base model, temperature only: not emitted
    _seed(conn, 27, "pressure", None)          # null value: not emitted

    _run(conn)

    registered = {r["member_id"] for r in conn.execute(
        "select member_id from members where model_id = 300 and member_id > 100")}
    assert registered == {101, 102}
    names = {r["member_id"]: r["name"] for r in conn.execute(
        "select member_id, name from members where model_id = 300 and member_id > 100")}
    assert names == {101: "persistence", 102: "climatological_mean"}


def test_null_source_value_emits_no_pass_through_row(conn):
    _seed(conn, 202, "temperature", None)
    _seed(conn, 100, "temperature", 19.0)

    rows = _run(conn)

    assert _members(rows) >= {4, 0}
    assert 3 not in _members(rows)


@pytest.mark.parametrize("present, expected_source", [
    ((202, 200, 201, 100), 202),
    ((200, 201, 100), 200),
    ((201, 100), 201),
    ((100,), 100),
])
def test_cell_valid_at_follows_202_then_200_201_100(conn, present, expected_source):
    offsets = {202: 0, 200: 60, 201: 120, 100: 600}
    for model_id in present:
        _seed(conn, model_id, "temperature", 20.0, valid_at=_ISSUED_AT + 6 * _HOUR + offsets[model_id])

    rows = _run(conn)

    expected = _ISSUED_AT + 6 * _HOUR + offsets[expected_source]
    assert _member(rows, 0)["valid_at"] == expected
    assert _member(rows, meta.SELF_CORRECTION_MEMBER)["valid_at"] == expected
    assert _member(rows, meta.SELF_CORRECTION_FAST_MEMBER)["valid_at"] == expected


def test_cell_valid_at_falls_back_to_lowest_among_remaining_sources(conn):
    _seed(conn, 1, "pressure", 1013.0, valid_at=_ISSUED_AT + 6 * _HOUR + 300)
    _seed(conn, 2, "pressure", 1012.0, valid_at=_ISSUED_AT + 6 * _HOUR + 100)

    rows = _run(conn)

    assert _member(rows, 0, "pressure")["valid_at"] == _ISSUED_AT + 6 * _HOUR + 100


def test_pass_through_keeps_its_own_valid_at_when_cell_differs(conn):
    _seed(conn, 202, "temperature", 21.0, valid_at=_ISSUED_AT + 6 * _HOUR)
    _seed(conn, 100, "temperature", 19.0, valid_at=_ISSUED_AT + 6 * _HOUR + 900)

    rows = _run(conn)

    assert _member(rows, 4)["valid_at"] == _ISSUED_AT + 6 * _HOUR + 900
    assert _member(rows, 0)["valid_at"] == _ISSUED_AT + 6 * _HOUR


def test_member_zero_is_the_weighted_mean_of_the_non_sc_members(conn):
    _seed(conn, 202, "temperature", 21.0, confidence=0.5)
    _seed(conn, 100, "temperature", 19.0, confidence=0.5)
    sector = _sector(_ISSUED_AT + 6 * _HOUR)
    weights = {(3, "temperature", 6, sector): 0.75, (4, "temperature", 6, sector): 0.25}

    rows = _run(conn, weights)

    expected, _ = _confidence.combine_pattern(
        [(3, 21.0), (4, 19.0)], {3: 0.75, 4: 0.25}, {3: 0.5, 4: 0.5})
    assert _member(rows, 0)["value"] == pytest.approx(expected)
    assert expected == pytest.approx(20.5)


def test_member_zero_is_the_plain_mean_without_weights_and_spread_is_population_std(conn):
    _seed(conn, 202, "temperature", 22.0)
    _seed(conn, 100, "temperature", 20.0)
    _seed(conn, 200, "temperature", 18.0)

    rows = _run(conn)

    zero = _member(rows, 0)
    member_values = [r["value"] for r in rows
                     if r["member_id"] not in (0, 91, 92) and r["lead_hours"] == 6]
    assert zero["value"] == pytest.approx(20.0)
    assert sorted(member_values) == [18.0, 20.0, 20.0, 20.0, 22.0]   # three sources, median, hot hand
    assert zero["spread"] == pytest.approx(math.sqrt(sum((v - 20.0) ** 2 for v in member_values) / 5))


def test_with_only_202_present_member_zero_equals_it(conn):
    _seed(conn, 202, "temperature", 21.5)

    rows = _run(conn)

    assert _member(rows, 0)["value"] == pytest.approx(21.5)
    assert _members(rows) >= {0, 3, meta.SELF_CORRECTION_MEMBER, meta.SELF_CORRECTION_FAST_MEMBER}


def test_self_correction_rows_are_emitted_with_no_value_before_history(conn):
    _seed(conn, 202, "temperature", 21.0)

    rows = _run(conn)

    assert _member(rows, meta.SELF_CORRECTION_MEMBER)["value"] is None
    assert _member(rows, meta.SELF_CORRECTION_FAST_MEMBER)["value"] is None


def _seed_raw_mix_history(conn, days_ago, error, count=3):
    for i in range(count):
        issued = _ISSUED_AT - days_ago * 86400 - i * 3 * _HOUR
        conn.execute(
            """
            insert into forecasts
                (model_id, model, member_id, issued_at, valid_at, lead_hours, variable, value,
                 observed, error, mae, scored_at)
            values (300, 'meta_forecast', -1, ?, ?, 6, 'temperature', 20.0, 19.0, ?, ?, ?)
            """,
            (issued, issued + 6 * _HOUR, error, abs(error), issued + 7 * _HOUR),
        )


def test_member_92_uses_a_14_day_window_and_member_91_a_60_day_one(conn):
    _seed(conn, 202, "temperature", 21.0)
    _seed_raw_mix_history(conn, days_ago=20, error=1.0)   # inside 60 days, outside 14

    rows = _run(conn)

    assert _member(rows, meta.SELF_CORRECTION_MEMBER)["value"] == pytest.approx(21.0 - 1.0)
    assert _member(rows, meta.SELF_CORRECTION_FAST_MEMBER)["value"] is None

    _seed_raw_mix_history(conn, days_ago=5, error=3.0)   # inside both windows
    rows = _run(conn)
    assert _member(rows, meta.SELF_CORRECTION_FAST_MEMBER)["value"] == pytest.approx(21.0 - 3.0)
    assert _member(rows, meta.SELF_CORRECTION_MEMBER)["value"] == pytest.approx(21.0 - 2.0)


# ---------------------------------------------------------------------------
# derived members 11, 12, 13, 17, 18
# ---------------------------------------------------------------------------

def _seed_scored(conn, model_id, variable, lead, mae, count, *, spread=None, days_ago=1):
    """Scored member-0 history rows for one model/cell, newest first, 3 hours apart."""
    name = conn.execute("select name from models where id = ?", (model_id,)).fetchone()["name"]
    for i in range(count):
        issued = _ISSUED_AT - days_ago * 86400 - i * 3 * _HOUR
        valid = issued + lead * _HOUR
        conn.execute(
            """
            insert into forecasts
                (model_id, model, member_id, issued_at, valid_at, lead_hours, variable, value, spread,
                 observed, error, mae, scored_at)
            values (?, ?, 0, ?, ?, ?, ?, 20.0, ?, 19.0, ?, ?, ?)
            """,
            (model_id, name, issued, valid, lead, variable, spread, mae, mae, valid + 600),
        )


def test_median_sources_needs_three_values(conn):
    _seed(conn, 202, "temperature", 20.0)
    _seed(conn, 100, "temperature", 30.0)

    assert 11 not in _members(_run(conn))

    _seed(conn, 200, "temperature", 22.0)
    assert _member(_run(conn), 11)["value"] == 22.0


def test_median_sources_uses_the_pressure_pool_for_pressure(conn):
    for model_id, value in ((1, 1010.0), (2, 1014.0), (3, 1012.0)):
        _seed(conn, model_id, "pressure", value)

    rows = _run(conn)

    assert _member(rows, 11, "pressure")["value"] == 1012.0
    assert {101, 102, 103} <= _members(rows, "pressure")
    assert not (_members(rows, "pressure") & {1, 2, 3, 4, 13, 14, 15, 16, 17})


def test_hot_hand_weights_sources_by_inverse_recent_mae(conn):
    _seed(conn, 202, "temperature", 20.0)
    _seed(conn, 200, "temperature", 26.0)
    _seed_scored(conn, 202, "temperature", 6, 1.0, 6)
    _seed_scored(conn, 200, "temperature", 6, 3.0, 6)

    value = _member(_run(conn), 12)["value"]

    assert value == pytest.approx((20.0 / 1.0 + 26.0 / 3.0) / (1.0 / 1.0 + 1.0 / 3.0))


def test_hot_hand_falls_back_to_the_plain_mean_under_six_rows_in_the_cell(conn):
    _seed(conn, 202, "temperature", 20.0)
    _seed(conn, 200, "temperature", 26.0)
    _seed_scored(conn, 202, "temperature", 6, 1.0, 6)
    _seed_scored(conn, 200, "temperature", 6, 3.0, 5)

    assert _member(_run(conn), 12)["value"] == pytest.approx(23.0)


def test_hot_hand_ignores_history_older_than_five_days(conn):
    _seed(conn, 202, "temperature", 20.0)
    _seed(conn, 200, "temperature", 26.0)
    _seed_scored(conn, 202, "temperature", 6, 1.0, 6, days_ago=2)
    _seed_scored(conn, 200, "temperature", 6, 3.0, 6, days_ago=9)

    assert _member(_run(conn), 12)["value"] == pytest.approx(23.0)


def test_hot_hand_still_weights_the_remaining_members_when_a_source_value_is_null(conn):
    _seed(conn, 202, "temperature", 20.0)
    _seed(conn, 200, "temperature", 26.0)
    _seed(conn, 201, "temperature", None)
    _seed_scored(conn, 202, "temperature", 6, 1.0, 6)
    _seed_scored(conn, 200, "temperature", 6, 3.0, 6)

    rows = _run(conn)

    assert 2 not in _members(rows)
    assert _member(rows, 12)["value"] == pytest.approx((20.0 / 1.0 + 26.0 / 3.0) / (1.0 + 1.0 / 3.0))


def test_hot_hand_needs_two_pool_values(conn):
    _seed(conn, 202, "temperature", 20.0)

    assert 12 not in _members(_run(conn))


def _seed_spread_history(conn, spread, count=20):
    _seed_scored(conn, 100, "temperature", 6, 1.0, count, spread=spread)


def test_spread_gated_ensemble_follows_the_ensemble_spread_against_its_own_median(conn):
    _seed_spread_history(conn, 1.0)
    _seed(conn, 202, "temperature", 20.0)
    _seed(conn, 100, "temperature", 24.0, spread=0.0)
    assert _member(_run(conn), 13)["value"] == pytest.approx(20.0 + 0.5 * 1.0 * 4.0)

    conn.execute("update forecasts set spread = 1.0 where model_id = 100 and issued_at = ?", (_ISSUED_AT,))
    assert _member(_run(conn), 13)["value"] == pytest.approx(20.0 + 0.5 * 0.5 * 4.0)

    conn.execute("update forecasts set spread = 2.0 where model_id = 100 and issued_at = ?", (_ISSUED_AT,))
    assert _member(_run(conn), 13)["value"] == pytest.approx(20.0)


def test_spread_gated_ensemble_defaults_the_gate_with_under_twenty_history_rows(conn):
    _seed_spread_history(conn, 1.0, count=19)
    _seed(conn, 202, "temperature", 20.0)
    _seed(conn, 100, "temperature", 24.0, spread=0.3)

    assert _member(_run(conn), 13)["value"] == pytest.approx(20.0 + 0.5 * 0.5 * 4.0)


def test_spread_gated_ensemble_needs_202_the_ensemble_and_a_spread(conn):
    _seed(conn, 202, "temperature", 20.0)
    assert 13 not in _members(_run(conn))

    _seed(conn, 100, "temperature", 24.0, spread=None)
    assert 13 not in _members(_run(conn))


def test_ensemble_gated_weights_by_the_ensembles_share_of_the_two_confidences(conn):
    _seed(conn, 202, "temperature", 20.0, confidence=0.6)
    _seed(conn, 100, "temperature", 24.0, confidence=0.2)

    assert _member(_run(conn), 17)["value"] == pytest.approx(20.0 + 0.25 * 4.0)


def test_ensemble_gated_abstains_when_both_confidences_are_zero_or_missing(conn):
    _seed(conn, 202, "temperature", 20.0, confidence=0.0)
    _seed(conn, 100, "temperature", 24.0, confidence=0.0)
    assert 17 not in _members(_run(conn))

    conn.execute("update forecasts set confidence = null where issued_at = ?", (_ISSUED_AT,))
    assert 17 not in _members(_run(conn))


def test_confidence_argmax_picks_the_most_confident_pool_member(conn):
    _seed(conn, 202, "temperature", 21.0, confidence=0.3)
    _seed(conn, 100, "temperature", 24.0, confidence=0.5)
    _seed(conn, 200, "temperature", 18.0, confidence=0.1)

    assert _member(_run(conn), 18)["value"] == 24.0


def test_confidence_argmax_ties_go_to_the_lowest_member_id(conn):
    _seed(conn, 202, "temperature", 21.0, confidence=0.4)   # member 3
    _seed(conn, 100, "temperature", 24.0, confidence=0.4)   # member 4

    assert _member(_run(conn), 18)["value"] == 21.0


def test_confidence_argmax_needs_two_members_with_a_confidence(conn):
    _seed(conn, 202, "temperature", 21.0, confidence=0.4)
    _seed(conn, 100, "temperature", 24.0, confidence=None)
    assert 18 not in _members(_run(conn))

    conn.execute("update forecasts set confidence = null")
    assert 18 not in _members(_run(conn))


def test_derived_members_use_the_cell_valid_at_and_their_own_confidence(conn):
    _seed(conn, 202, "temperature", 20.0, valid_at=_ISSUED_AT + 6 * _HOUR)
    _seed(conn, 100, "temperature", 24.0, valid_at=_ISSUED_AT + 6 * _HOUR + 600)
    _seed(conn, 200, "temperature", 22.0)

    rows = _run(conn)

    for member_id in (11, 12):
        assert _member(rows, member_id)["valid_at"] == _ISSUED_AT + 6 * _HOUR
        assert _member(rows, member_id)["confidence"] == 0.0   # no history yet


def test_member_zero_includes_the_derived_members(conn):
    _seed(conn, 202, "temperature", 20.0)
    _seed(conn, 100, "temperature", 24.0)
    _seed(conn, 200, "temperature", 22.0)

    rows = _run(conn)

    member_rows = [r for r in rows if r["member_id"] not in (0, 91, 92) and r["variable"] == "temperature"]
    assert {11, 12} <= {r["member_id"] for r in member_rows}
    expected = sum(r["value"] for r in member_rows) / len(member_rows)
    assert _member(rows, 0)["value"] == pytest.approx(expected)


def test_emitted_member_ids_are_registered(conn):
    for model_id, value in ((202, 21.0), (100, 24.0), (200, 22.0), (201, 23.0), (1, 20.0)):
        _seed(conn, model_id, "temperature", value, confidence=0.3, spread=1.0)
    for model_id in (1, 2, 5):
        _seed(conn, model_id, "pressure", 1010.0 + model_id, confidence=0.2)

    rows = _run(conn)

    registered = {r["member_id"] for r in conn.execute("select member_id from members where model_id = 300")}
    assert {r["member_id"] for r in rows} <= registered


# ---------------------------------------------------------------------------
# fitted and lagged members 14, 15, 16
# ---------------------------------------------------------------------------

def _seed_error(conn, model_id, issued, lead, variable, error, *, valid_at=None):
    name = conn.execute("select name from models where id = ?", (model_id,)).fetchone()["name"]
    valid = issued + lead * _HOUR if valid_at is None else valid_at
    conn.execute(
        """
        insert into forecasts
            (model_id, model, member_id, issued_at, valid_at, lead_hours, variable, value,
             observed, error, mae, scored_at)
        values (?, ?, 0, ?, ?, ?, ?, 20.0, 19.0, ?, ?, ?)
        """,
        (model_id, name, issued, valid, lead, variable, error, abs(error), valid + 600),
    )


def test_fit_gamma_returns_the_shrunk_least_squares_coefficient():
    d = [(i % 5) - 2.0 or 1.0 for i in range(40)]
    pairs = [(-0.5 * x, x) for x in d]          # e_c = -0.5 d  ->  gamma = 0.5 / (1 + 20/40)

    assert meta._fit_gamma(pairs) == pytest.approx(0.5 / 1.5)


def test_fit_gamma_is_zero_without_a_relationship_and_clipped_at_the_bounds():
    d = [(i % 5) - 2.0 or 1.0 for i in range(40)]
    assert meta._fit_gamma([(0.0, x) for x in d]) == 0.0
    assert meta._fit_gamma([(0.0, 0.0)] * 40) == 0.0
    assert meta._fit_gamma([(-10.0 * x, x) for x in d]) == 1.0
    assert meta._fit_gamma([(10.0 * x, x) for x in d]) == -0.5


def test_fit_gamma_and_rho_abstain_below_thirty_pairs():
    assert meta._fit_gamma([(1.0, 1.0)] * 29) is None
    assert meta._fit_rho([(1.0, 1.0)] * 29) is None
    assert meta._fit_gamma([(1.0, 1.0)] * 30) is not None


def test_fit_rho_returns_the_shrunk_coefficient_clipped_to_zero_and_one_and_a_half():
    r = [(i % 5) - 2.0 or 1.0 for i in range(40)]
    assert meta._fit_rho([(0.5 * x, x) for x in r]) == pytest.approx(0.5 / 1.5)
    assert meta._fit_rho([(-1.0 * x, x) for x in r]) == 0.0
    assert meta._fit_rho([(10.0 * x, x) for x in r]) == 1.5
    assert meta._fit_rho([(0.0, 0.0)] * 40) == 0.0


def test_anchor_feature_is_the_mean_error_of_rows_valid_in_the_last_two_hours():
    history = [(100, 5.0), (7000, 1.0), (9000, 3.0), (9100, 100.0)]   # sorted by valid_at
    now = 9050

    assert meta._anchor_feature(history, now) == pytest.approx(2.0)   # valid 7000 (inside) and 9000
    assert meta._anchor_feature(history, 20000) is None
    assert meta._anchor_feature([], now) is None


def _seed_gamma_history(conn, count=40):
    """202 and ensemble errors with e_c = -2 * e_c' pattern so that d = -2 e_c, gamma = 1/3."""
    for i in range(count):
        issued = _ISSUED_AT - (i + 1) * 5 * _HOUR
        e_c = (i % 5) - 2.0 or 1.0
        _seed_error(conn, 202, issued, 6, "temperature", e_c)
        _seed_error(conn, 100, issued, 6, "temperature", e_c + (-2.0 * e_c))


def test_ensemble_residual_applies_the_fitted_gamma(conn):
    _seed_gamma_history(conn)
    _seed(conn, 202, "temperature", 20.0)
    _seed(conn, 100, "temperature", 26.0)

    assert _member(_run(conn), 14)["value"] == pytest.approx(20.0 + (1 / 3) * 6.0)


def test_ensemble_residual_abstains_without_enough_pairs_or_without_the_ensemble(conn):
    _seed_gamma_history(conn, count=29)
    _seed(conn, 202, "temperature", 20.0)
    _seed(conn, 100, "temperature", 26.0)
    assert 14 not in _members(_run(conn))

    conn.execute("delete from forecasts where model_id = 100 and issued_at = ?", (_ISSUED_AT,))
    assert 14 not in _members(_run(conn))


def test_ensemble_residual_ignores_history_older_than_ninety_days(conn):
    for i in range(40):
        issued = _ISSUED_AT - 100 * 86400 - i * 5 * _HOUR
        e_c = (i % 5) - 2.0 or 1.0
        _seed_error(conn, 202, issued, 6, "temperature", e_c)
        _seed_error(conn, 100, issued, 6, "temperature", -e_c)
    _seed(conn, 202, "temperature", 20.0)
    _seed(conn, 100, "temperature", 26.0)

    assert 14 not in _members(_run(conn))


def _seed_anchor_history(conn, count=40):
    """Lead-6 202 rows issued every 5h, each preceded by a lead-1 row valid at the
    same issue time, so r = that lead-1 error and e_c = 0.5 * r  ->  rho = 0.5 / 1.5."""
    for i in range(count):
        t = _ISSUED_AT - (i + 1) * 5 * _HOUR - 12 * _HOUR
        r = (i % 5) - 2.0 or 1.0
        _seed_error(conn, 202, t - _HOUR, 1, "temperature", r)       # valid at t
        _seed_error(conn, 202, t, 6, "temperature", 0.5 * r)


def test_obs_anchor_subtracts_the_fitted_share_of_the_recent_202_error(conn):
    _seed_anchor_history(conn)
    _seed_error(conn, 202, _ISSUED_AT - _HOUR, 1, "temperature", 2.0)   # valid now, error +2
    _seed(conn, 202, "temperature", 20.0)

    assert _member(_run(conn), 15)["value"] == pytest.approx(20.0 - (0.5 / 1.5) * 2.0)


def test_obs_anchor_abstains_without_a_recent_202_error(conn):
    _seed_anchor_history(conn)
    _seed(conn, 202, "temperature", 20.0)

    assert 15 not in _members(_run(conn))


def test_obs_anchor_abstains_without_a_fit(conn):
    _seed_error(conn, 202, _ISSUED_AT - _HOUR, 1, "temperature", 2.0)
    _seed(conn, 202, "temperature", 20.0)

    assert 15 not in _members(_run(conn))


def _seed_earlier_run(conn, hours_ago, value, *, variable="temperature", valid_at=None, lead=None):
    issued = _ISSUED_AT - hours_ago * _HOUR
    valid_at = _ISSUED_AT + 6 * _HOUR if valid_at is None else valid_at
    lead = (valid_at - issued) // _HOUR if lead is None else lead
    _seed(conn, 202, variable, value, lead=lead, valid_at=valid_at, issued_at=issued)


def test_lagged_consensus_averages_this_run_with_up_to_two_earlier_runs(conn):
    _seed(conn, 202, "temperature", 20.0)
    _seed_earlier_run(conn, 3, 24.0)
    _seed_earlier_run(conn, 6, 26.0)
    _seed_earlier_run(conn, 5, 100.0)      # newest two earlier runs are kept (-3h, -5h), so -6h is dropped

    value = _member(_run(conn), 16)["value"]

    assert value == pytest.approx((20.0 + 24.0 + 100.0) / 3)


def test_lagged_consensus_skips_runs_closer_than_an_hour_to_this_run_or_each_other(conn):
    _seed(conn, 202, "temperature", 20.0)
    _seed_earlier_run(conn, 0.8, 90.0, valid_at=_ISSUED_AT + 6 * _HOUR + 300)   # 48 min ago: too close
    _seed_earlier_run(conn, 3, 24.0)
    _seed_earlier_run(conn, 3.5, 70.0, valid_at=_ISSUED_AT + 6 * _HOUR + 300)   # 30 min after the -3h run
    _seed_earlier_run(conn, 5, 26.0)

    assert _member(_run(conn), 16)["value"] == pytest.approx((20.0 + 24.0 + 26.0) / 3)


def test_lagged_consensus_ignores_runs_outside_seven_hours_and_far_valid_times(conn):
    _seed(conn, 202, "temperature", 20.0)
    _seed_earlier_run(conn, 8, 90.0)                                           # outside the 7h lookback
    _seed_earlier_run(conn, 3, 24.0, valid_at=_ISSUED_AT + 6 * _HOUR + 700)    # 700 s off: too far

    assert 16 not in _members(_run(conn))


def test_lagged_consensus_is_per_variable_and_needs_this_runs_202_value(conn):
    _seed(conn, 202, "temperature", 20.0)
    _seed_earlier_run(conn, 3, 12.0, variable="dewpoint")

    assert 16 not in _members(_run(conn))

    conn.execute("delete from forecasts where model_id = 202 and issued_at = ?", (_ISSUED_AT,))
    _seed_earlier_run(conn, 3, 24.0)
    _seed(conn, 100, "temperature", 21.0)
    assert 16 not in _members(_run(conn))


def test_fitted_and_lagged_rows_are_registered_and_in_member_zero(conn):
    _seed_gamma_history(conn)
    _seed_anchor_history(conn)
    _seed_error(conn, 202, _ISSUED_AT - _HOUR, 1, "temperature", 2.0)
    _seed(conn, 202, "temperature", 20.0, confidence=0.3, spread=1.0)
    _seed(conn, 100, "temperature", 26.0, confidence=0.1, spread=1.0)
    _seed_earlier_run(conn, 3, 24.0)

    rows = _run(conn)

    assert {14, 15, 16} <= _members(rows)
    registered = {r["member_id"] for r in conn.execute("select member_id from members where model_id = 300")}
    assert {r["member_id"] for r in rows} <= registered


def test_confidence_argmax_skips_bogo_whose_confidence_is_a_random_draw(conn):
    _seed(conn, 12, "pressure", 1000.0, confidence=0.9)    # bogo, member 112
    _seed(conn, 1, "pressure", 1010.0, confidence=0.4)     # persistence, member 101
    _seed(conn, 2, "pressure", 1012.0, confidence=0.2)     # member 102

    rows = _run(conn)

    assert 112 in _members(rows, "pressure")
    assert _member(rows, 18, "pressure")["value"] == 1010.0


def test_confidence_argmax_abstains_when_the_highest_confidence_is_zero(conn):
    _seed(conn, 202, "temperature", 21.0, confidence=0.0)
    _seed(conn, 100, "temperature", 24.0, confidence=0.0)

    assert 18 not in _members(_run(conn))


def test_fixed_sources_never_become_members_of_a_pressure_cell(conn):
    for model_id in (100, 200, 201, 202):
        _seed(conn, model_id, "pressure", 1013.0)
    _seed(conn, 1, "pressure", 1010.0)

    rows = _run(conn)

    assert _members(rows, "pressure") & {1, 2, 3, 4} == set()
    assert 101 in _members(rows, "pressure")
