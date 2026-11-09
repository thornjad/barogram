import db
from tests.conftest import make_output_db


def _register(conn, model_id, member_id, name):
    conn.execute(
        "insert or ignore into members (model_id, member_id, name) values (?, ?, ?)",
        (model_id, member_id, name),
    )


def test_has_raw_mix_false_until_registered():
    conn = make_output_db()
    assert db.has_raw_mix(conn, 1) is False
    _register(conn, 1, -1, "raw_mix")
    assert db.has_raw_mix(conn, 1) is True


def test_has_raw_mix_requires_the_raw_mix_name():
    conn = make_output_db()
    _register(conn, 2, -1, "something_else")
    assert db.has_raw_mix(conn, 2) is False


def test_sc_blend_models_lists_sc_members_of_registered_models_only():
    conn = make_output_db()
    _register(conn, 1, -1, "raw_mix")
    _register(conn, 1, 4, "self_correction")
    _register(conn, 1, 5, "self_correction_fast")
    _register(conn, 1, 1, "ordinary_member")
    _register(conn, 2, 4, "self_correction")  # no raw_mix: not part of the blend

    result = db.sc_blend_models(conn)

    assert sorted(result[1]) == [(4, "self_correction"), (5, "self_correction_fast")]
    assert 2 not in result
