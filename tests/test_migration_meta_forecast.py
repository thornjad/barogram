import db
import models.meta_forecast as meta
from tests.conftest import make_output_db

_ISSUED = 1_700_000_000


def test_migration_registers_model_300_as_meta():
    conn = make_output_db()

    row = conn.execute("select name, type from models where id = 300").fetchone()

    assert (row["name"], row["type"]) == ("meta_forecast", "meta")


def test_members_equal_the_static_registry_plus_zero_and_raw_mix():
    conn = make_output_db()

    registered = {r["member_id"]: r["name"] for r in conn.execute(
        "select member_id, name from members where model_id = 300")}

    assert registered == {0: None, -1: "raw_mix", **meta.STATIC_MEMBERS}


def test_sc_blend_models_lists_300_with_both_self_correction_members():
    conn = make_output_db()

    sc = db.sc_blend_models(conn)

    assert sorted(sc[300]) == [(91, "self_correction"), (92, "self_correction_fast")]


def test_the_ensemble_never_includes_model_300():
    conn = make_output_db()
    conn.execute(
        """
        insert into forecasts
            (model_id, model, member_id, issued_at, valid_at, lead_hours, variable, value)
        values (300, 'meta_forecast', 0, ?, ?, 6, 'temperature', 20.0)
        """,
        (_ISSUED, _ISSUED + 6 * 3600),
    )

    db.sync_ensemble_members(conn)

    assert db.ensemble_inputs(conn, _ISSUED) == []
    assert 300 not in {r["member_id"] for r in conn.execute("select member_id from members where model_id = 100")}
