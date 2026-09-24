import db
from tests.conftest import make_output_db


def _insert_forecast(conn, model_id, member_id, model="climo_deviation", lead_hours=6):
    conn.execute(
        """
        insert into forecasts
            (model_id, model, member_id, issued_at, valid_at, lead_hours, variable, value)
        values (?, ?, ?, 1000, 1600, ?, 'temperature', 20.0)
        """,
        (model_id, model, member_id, lead_hours),
    )


def test_find_orphan_members_clean_when_all_scored_members_registered():
    conn = make_output_db()
    _insert_forecast(conn, model_id=1, member_id=0, model="persistence")

    assert db.find_orphan_members(conn) == []


def test_find_orphan_members_reports_missing_member_row():
    conn = make_output_db()
    # model 4 (climo_deviation) exists, but member_id 999 has no members row
    _insert_forecast(conn, model_id=4, member_id=999, lead_hours=6)
    _insert_forecast(conn, model_id=4, member_id=999, lead_hours=12)

    orphans = db.find_orphan_members(conn)

    assert len(orphans) == 1
    assert orphans[0]["model_id"] == 4
    assert orphans[0]["member_id"] == 999
    assert orphans[0]["model"] == "climo_deviation"
    assert orphans[0]["n"] == 2


def test_find_orphan_members_ignores_registered_members():
    conn = make_output_db()
    conn.execute(
        "insert or ignore into members (model_id, member_id, name) values (4, 55, 'self_correction')"
    )
    _insert_forecast(conn, model_id=4, member_id=55)

    assert db.find_orphan_members(conn) == []


def test_next_member_ids_is_max_plus_one_per_model():
    conn = make_output_db()

    next_ids = {row["model_id"]: row["next_member_id"] for row in db.next_member_ids(conn)}
    max_member_id = max(
        r["member_id"] for r in conn.execute("select member_id from members where model_id = 1")
    )

    assert next_ids[1] == max_member_id + 1


def test_next_migration_number_is_highest_file_plus_one(tmp_path):
    (tmp_path / "001_baseline.sql").write_text("select 1;")
    (tmp_path / "002_members.sql").write_text("select 1;")
    (tmp_path / "010_something.sql").write_text("select 1;")

    assert db.next_migration_number(tmp_path) == 11


def test_next_migration_number_empty_dir(tmp_path):
    assert db.next_migration_number(tmp_path) == 1
