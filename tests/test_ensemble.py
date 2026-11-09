import math

import models.ensemble as ens
from tests.conftest import make_obs, make_output_db

_ISSUED_AT = 1_700_000_000
_VALID_AT = _ISSUED_AT + 6 * 3600


def _seed(conn, model_id, model_name, variable, value, lead_hours=6, confidence=None):
    conn.execute(
        """
        insert into forecasts
            (model_id, model, member_id, issued_at, valid_at, lead_hours, variable, value, confidence)
        values (?, ?, 0, ?, ?, ?, ?, ?, ?)
        """,
        (model_id, model_name, _ISSUED_AT, _VALID_AT, lead_hours, variable, value, confidence),
    )


def test_empty_output_returns_empty():
    conn = make_output_db()
    obs = make_obs()
    result = ens.run(obs, _ISSUED_AT, conn_out=conn)
    assert result == []


def test_equal_weight_mean_two_models():
    conn = make_output_db()
    _seed(conn, 1, "persistence", "temperature", 10.0)
    _seed(conn, 2, "climatological_mean", "temperature", 20.0)
    obs = make_obs()

    rows = ens.run(obs, _ISSUED_AT, conn_out=conn, weights=None)

    mean_row = next(r for r in rows if r["member_id"] == 0)
    assert abs(mean_row["value"] - 15.0) < 1e-9


def test_weighted_mean():
    conn = make_output_db()
    _seed(conn, 1, "persistence", "temperature", 10.0)
    _seed(conn, 2, "climatological_mean", "temperature", 20.0)
    obs = make_obs()
    s = ens._sector(_VALID_AT)
    # weight model 1 at 0.25, model 2 at 0.75 -> expected mean = 17.5
    weights = {(1, "temperature", 6, s): 0.25, (2, "temperature", 6, s): 0.75}

    rows = ens.run(obs, _ISSUED_AT, conn_out=conn, weights=weights)

    mean_row = next(r for r in rows if r["member_id"] == 0)
    assert abs(mean_row["value"] - 17.5) < 1e-9


def test_member_rows_emitted():
    conn = make_output_db()
    _seed(conn, 1, "persistence", "temperature", 10.0)
    _seed(conn, 2, "climatological_mean", "temperature", 20.0)
    obs = make_obs()

    rows = ens.run(obs, _ISSUED_AT, conn_out=conn)

    member_ids = {r["member_id"] for r in rows}
    assert 0 in member_ids
    assert 1 in member_ids
    assert 2 in member_ids


def test_single_model_produces_mean_equal_to_value():
    conn = make_output_db()
    _seed(conn, 1, "persistence", "temperature", 15.0)
    obs = make_obs()

    rows = ens.run(obs, _ISSUED_AT, conn_out=conn)

    mean_row = next(r for r in rows if r["member_id"] == 0)
    assert abs(mean_row["value"] - 15.0) < 1e-9


def test_spread_is_zero_for_single_model():
    conn = make_output_db()
    _seed(conn, 1, "persistence", "temperature", 15.0)
    obs = make_obs()

    rows = ens.run(obs, _ISSUED_AT, conn_out=conn)

    mean_row = next(r for r in rows if r["member_id"] == 0)
    assert mean_row["spread"] == 0.0


def test_spread_correct_for_two_models():
    conn = make_output_db()
    _seed(conn, 1, "persistence", "temperature", 10.0)
    _seed(conn, 2, "climatological_mean", "temperature", 20.0)
    obs = make_obs()

    rows = ens.run(obs, _ISSUED_AT, conn_out=conn)

    mean_row = next(r for r in rows if r["member_id"] == 0)
    # population std dev of [10, 20] around mean 15 = 5.0
    assert abs(mean_row["spread"] - 5.0) < 1e-9


def test_model_id_is_100():
    conn = make_output_db()
    _seed(conn, 1, "persistence", "temperature", 15.0)
    obs = make_obs()

    rows = ens.run(obs, _ISSUED_AT, conn_out=conn)

    assert all(r["model_id"] == 100 for r in rows)


def test_model_name_is_barogram_ensemble():
    conn = make_output_db()
    _seed(conn, 1, "persistence", "temperature", 15.0)
    obs = make_obs()

    rows = ens.run(obs, _ISSUED_AT, conn_out=conn)

    assert all(r["model"] == "barogram_ensemble" for r in rows)


def test_missing_variable_in_one_model_excluded():
    # a variable present in only some base models still forms a partial
    # ensemble from the ones that cover it
    conn = make_output_db()
    _seed(conn, 1, "persistence", "dewpoint", 8.0)
    # model 6 (diurnal_curve) has no dewpoint row — not seeded
    obs = make_obs()

    rows = ens.run(obs, _ISSUED_AT, conn_out=conn)

    dewpoint_mean = next(
        (r for r in rows if r["member_id"] == 0 and r["variable"] == "dewpoint"), None
    )
    assert dewpoint_mean is not None
    assert abs(dewpoint_mean["value"] - 8.0) < 1e-9


def test_pressure_excluded_from_ensemble():
    # base models keep predicting and scoring pressure individually, but the
    # ensemble itself never combines or outputs it
    conn = make_output_db()
    _seed(conn, 1, "persistence", "pressure", 1013.0)
    _seed(conn, 2, "climatological_mean", "pressure", 1015.0)
    _seed(conn, 1, "persistence", "temperature", 15.0)
    obs = make_obs()

    rows = ens.run(obs, _ISSUED_AT, conn_out=conn)

    assert all(r["variable"] != "pressure" for r in rows)


def test_required_keys_present():
    conn = make_output_db()
    _seed(conn, 1, "persistence", "temperature", 15.0)
    obs = make_obs()

    rows = ens.run(obs, _ISSUED_AT, conn_out=conn)

    required = {"model_id", "model", "issued_at", "valid_at", "lead_hours", "variable", "value"}
    for row in rows:
        missing = required - row.keys()
        assert not missing, f"row missing keys: {missing}"


def test_partial_weights_uses_available():
    """When weights exist for some members but not all, weighted mean uses only the
    members that have weights; untuned members are excluded rather than triggering
    a fallback to equal weighting."""
    conn = make_output_db()
    _seed(conn, 1, "persistence", "temperature", 10.0)
    _seed(conn, 2, "climatological_mean", "temperature", 20.0)
    _seed(conn, 3, "weighted_climatological_mean", "temperature", 30.0)
    obs = make_obs()
    s = ens._sector(_VALID_AT)
    # only models 1 and 2 have weights; model 3 is absent
    weights = {(1, "temperature", 6, s): 0.9, (2, "temperature", 6, s): 0.1}

    rows = ens.run(obs, _ISSUED_AT, conn_out=conn, weights=weights)

    mean_row = next(r for r in rows if r["member_id"] == 0)
    # weighted mean of models 1 and 2 only: 0.9*10 + 0.1*20 = 11.0
    assert abs(mean_row["value"] - 11.0) < 1e-9


def test_confidence_shifts_value_versus_weights_alone():
    conn = make_output_db()
    _seed(conn, 1, "persistence", "temperature", 10.0, confidence=0.9)  # trusted
    _seed(conn, 2, "climatological_mean", "temperature", 20.0, confidence=0.1)  # distrusted
    obs = make_obs()
    s = ens._sector(_VALID_AT)
    weights = {(1, "temperature", 6, s): 1.0, (2, "temperature", 6, s): 1.0}

    rows = ens.run(obs, _ISSUED_AT, conn_out=conn, weights=weights)
    mean_row = next(r for r in rows if r["member_id"] == 0)

    # equal weights alone would give 15.0; confidence should pull it toward
    # model 1's trusted 10.0 instead
    assert mean_row["value"] < 15.0
    assert mean_row["confidence"] is not None


def test_member_rows_carry_their_own_confidence():
    conn = make_output_db()
    _seed(conn, 1, "persistence", "temperature", 10.0, confidence=0.42)
    obs = make_obs()

    rows = ens.run(obs, _ISSUED_AT, conn_out=conn)
    member_row = next(r for r in rows if r["member_id"] == 1)
    assert abs(member_row["confidence"] - 0.42) < 1e-9


# --- self-correction (member 101, formerly model 101) ---

def _register_sc(conn):
    conn.execute("insert or ignore into members (model_id, member_id, name) "
                 "values (100, 101, 'self_correction')")
    conn.execute("insert or ignore into members (model_id, member_id, name) "
                 "values (100, -1, 'raw_mix')")


def _seed_raw_mix_history(conn, bias, runs=5, variable="temperature", lead_hours=6):
    for d in range(1, runs + 1):
        issued = _ISSUED_AT - d * 86400
        conn.execute(
            """
            insert into forecasts
                (model_id, model, member_id, issued_at, valid_at, lead_hours, variable,
                 value, observed, error, mae, scored_at)
            values (100, 'barogram_ensemble', -1, ?, ?, ?, ?, ?, 15.0, ?, ?, ?)
            """,
            (issued, issued + lead_hours * 3600, lead_hours, variable, 15.0 + bias, bias,
             abs(bias), issued + 1),
        )


def test_ensemble_emits_its_own_self_correction_member():
    conn = make_output_db()
    _register_sc(conn)
    _seed(conn, 1, "persistence", "temperature", 10.0)
    _seed(conn, 2, "climatological_mean", "temperature", 20.0)
    _seed_raw_mix_history(conn, bias=2.0)

    rows = ens.run(make_obs(), _ISSUED_AT, conn_out=conn)

    mean_row = next(r for r in rows if r["member_id"] == 0)
    sc_row = next(r for r in rows if r["member_id"] == 101)
    # learned from the raw mix history (+2.0 hot), not from the final member 0
    assert abs(sc_row["value"] - (mean_row["value"] - 2.0)) < 1e-9
    assert sc_row["valid_at"] == mean_row["valid_at"]


def test_self_correction_is_none_without_enough_history():
    conn = make_output_db()
    _register_sc(conn)
    _seed(conn, 1, "persistence", "temperature", 10.0)

    rows = ens.run(make_obs(), _ISSUED_AT, conn_out=conn)

    assert next(r for r in rows if r["member_id"] == 101)["value"] is None


def test_self_correction_member_does_not_change_the_raw_blend():
    conn = make_output_db()
    _register_sc(conn)
    _seed(conn, 1, "persistence", "temperature", 10.0)
    _seed(conn, 2, "climatological_mean", "temperature", 20.0)
    _seed_raw_mix_history(conn, bias=2.0)

    rows = ens.run(make_obs(), _ISSUED_AT, conn_out=conn)

    # member 0 is the blend of the base models only; sc_blend adds the correction later
    assert abs(next(r for r in rows if r["member_id"] == 0)["value"] - 15.0) < 1e-9


def test_self_correction_confidence_comes_from_its_own_history():
    conn = make_output_db()
    _register_sc(conn)
    _seed(conn, 1, "persistence", "temperature", 10.0)

    rows = ens.run(make_obs(), _ISSUED_AT, conn_out=conn,
                   member_history={101: []}, default_matches=[])

    # no history yet: the confidence is the zero the other members report, not missing
    assert next(r for r in rows if r["member_id"] == 101)["confidence"] == 0.0


def test_sc_blend_gives_the_ensemble_a_final_member_zero_and_a_raw_mix():
    import db
    import sc_blend

    conn = make_output_db()
    _register_sc(conn)
    _seed(conn, 1, "persistence", "temperature", 10.0)
    _seed(conn, 2, "climatological_mean", "temperature", 20.0)
    _seed_raw_mix_history(conn, bias=2.0)
    s = ens._sector(_VALID_AT)
    weights = {(1, "temperature", 6, s): 0.4, (2, "temperature", 6, s): 0.4,
               (101, "temperature", 6, s): 0.2}

    rows = ens.run(make_obs(), _ISSUED_AT, conn_out=conn, weights=weights)
    out = sc_blend.apply(rows, sc_members=[(101, "self_correction")], weights=weights)

    final = next(r for r in out if r["member_id"] == 0)
    raw = next(r for r in out if r["member_id"] == db.RAW_MIX_ID)
    assert abs(raw["value"] - 15.0) < 1e-9
    assert abs(final["value"] - (0.8 * 15.0 + 0.2 * 13.0)) < 1e-9
