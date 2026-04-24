import sqlite3
from pathlib import Path

import pytest

import db
from tests.conftest import make_input_db, make_output_db

_MIGRATIONS_DIR = Path(__file__).parent.parent / "migrations"

_T = 1_700_000_000  # arbitrary fixed timestamp for nearest_tempest_obs tests


def _make_min_input_db() -> sqlite3.Connection:
    """Minimal in-memory DB with the full wxlog schema for validate_schema tests."""
    conn = sqlite3.connect(":memory:")
    conn.executescript("""
        create table stations (
            station_id text, source text, name text,
            latitude real, longitude real,
            elevation real, agl real
        );
        create table tempest_obs (
            station_id text, timestamp integer,
            air_temp real, dew_point real, station_pressure real,
            wind_avg real, wind_gust real, wind_direction real,
            precip_accum_day real, solar_radiation real,
            uv_index real, lightning_count integer
        );
        create table nws_obs (
            station_id text, timestamp integer,
            air_temp real, dew_point real,
            wind_speed real, wind_direction real,
            sea_level_pressure real, sky_cover text, raw_metar text
        );
    """)
    return conn


# --- validate_schema ---

def test_validate_schema_passes_correct_schema():
    conn = _make_min_input_db()
    db.validate_schema(conn)  # should not raise


def test_validate_schema_missing_table():
    conn = sqlite3.connect(":memory:")
    conn.execute("create table stations (station_id text, source text, name text, latitude real, longitude real)")
    conn.execute("create table nws_obs (station_id text, timestamp integer, air_temp real, dew_point real, wind_speed real, wind_direction real, sea_level_pressure real, sky_cover text, raw_metar text)")
    # tempest_obs table absent
    with pytest.raises(ValueError, match="tempest_obs"):
        db.validate_schema(conn)


def test_validate_schema_missing_column():
    conn = sqlite3.connect(":memory:")
    conn.executescript("""
        create table stations (station_id text, source text, name text, latitude real, longitude real);
        create table tempest_obs (
            station_id text, timestamp integer,
            dew_point real, station_pressure real,
            wind_avg real, wind_gust real, wind_direction real,
            precip_accum_day real, solar_radiation real,
            uv_index real, lightning_count integer
            -- air_temp intentionally omitted
        );
        create table nws_obs (station_id text, timestamp integer, air_temp real, dew_point real, wind_speed real, wind_direction real, sea_level_pressure real, sky_cover text, raw_metar text);
    """)
    with pytest.raises(ValueError, match="air_temp"):
        db.validate_schema(conn)


# --- run_migrations ---

def _highest_migration_version() -> int:
    files = sorted(_MIGRATIONS_DIR.glob("*.sql"))
    assert files, "no migration files found"
    return int(files[-1].stem.split("_")[0])


def test_run_migrations_applies_all():
    conn = sqlite3.connect(":memory:")
    db.run_migrations(conn, _MIGRATIONS_DIR)
    row = conn.execute("select value from metadata where key='schema_version'").fetchone()
    assert row is not None
    assert int(row[0]) == _highest_migration_version()


def test_run_migrations_idempotent():
    conn = sqlite3.connect(":memory:")
    db.run_migrations(conn, _MIGRATIONS_DIR)
    db.run_migrations(conn, _MIGRATIONS_DIR)  # second run should not raise
    row = conn.execute("select value from metadata where key='schema_version'").fetchone()
    assert int(row[0]) == _highest_migration_version()


# --- nearest_tempest_obs ---

def _seed_obs(conn, ts):
    conn.execute(
        """
        insert into tempest_obs
            (station_id, timestamp, air_temp, dew_point,
             station_pressure, wind_avg, wind_gust, wind_direction,
             precip_accum_day, solar_radiation, uv_index, lightning_count)
        values ('KTEST', ?, 20.0, 10.0, 1013.0, 3.0, null, null, null, null, null, null)
        """,
        (ts,),
    )


def test_nearest_tempest_obs_exact():
    conn = make_input_db()
    _seed_obs(conn, _T)
    row = db.nearest_tempest_obs(conn, _T)
    assert row is not None


def test_nearest_tempest_obs_upper_boundary():
    conn = make_input_db()
    _seed_obs(conn, _T)
    row = db.nearest_tempest_obs(conn, _T + 1800)
    assert row is not None


def test_nearest_tempest_obs_just_outside_upper():
    conn = make_input_db()
    _seed_obs(conn, _T)
    row = db.nearest_tempest_obs(conn, _T + 1801)
    assert row is None


def test_nearest_tempest_obs_lower_boundary():
    conn = make_input_db()
    _seed_obs(conn, _T)
    row = db.nearest_tempest_obs(conn, _T - 1800)
    assert row is not None


def test_nearest_tempest_obs_just_outside_lower():
    conn = make_input_db()
    _seed_obs(conn, _T)
    row = db.nearest_tempest_obs(conn, _T - 1801)
    assert row is None


def test_nearest_tempest_obs_empty_table():
    conn = make_input_db()
    assert db.nearest_tempest_obs(conn, _T) is None


# --- open_input_db ---

def test_open_input_db_missing_file():
    with pytest.raises(FileNotFoundError):
        db.open_input_db("/nonexistent/path/to/db.sqlite")


# --- insert_forecasts roundtrip ---

def test_insert_forecasts_default_member_id():
    conn = make_output_db()
    row = {
        "model_id": 1, "model": "persistence",
        "issued_at": 1700000000, "valid_at": 1700021600,
        "lead_hours": 6, "variable": "temperature", "value": 20.0,
        # member_id intentionally omitted
    }
    db.insert_forecasts(conn, [row])
    stored = conn.execute("select member_id from forecasts").fetchone()
    assert stored["member_id"] == 0


def test_insert_forecasts_default_spread_is_null():
    conn = make_output_db()
    row = {
        "model_id": 1, "model": "persistence",
        "issued_at": 1700000000, "valid_at": 1700021600,
        "lead_hours": 6, "variable": "temperature", "value": 20.0,
        # spread intentionally omitted
    }
    db.insert_forecasts(conn, [row])
    stored = conn.execute("select spread from forecasts").fetchone()
    assert stored["spread"] is None


# --- ensemble_inputs ---

def test_ensemble_inputs_empty_when_no_base_rows():
    conn = make_output_db()
    result = db.ensemble_inputs(conn, 1700000000)
    assert result == []


def test_ensemble_inputs_returns_base_model_rows():
    conn = make_output_db()
    conn.execute(
        """
        insert into forecasts
            (model_id, model, member_id, issued_at, valid_at, lead_hours, variable, value)
        values (1, 'persistence', 0, 1700000000, 1700021600, 6, 'temperature', 20.0)
        """
    )
    rows = db.ensemble_inputs(conn, 1700000000)
    assert len(rows) == 1
    assert rows[0]["model_id"] == 1
    assert rows[0]["variable"] == "temperature"


def test_ensemble_inputs_excludes_wrong_issued_at():
    conn = make_output_db()
    conn.execute(
        """
        insert into forecasts
            (model_id, model, member_id, issued_at, valid_at, lead_hours, variable, value)
        values (1, 'persistence', 0, 1700000000, 1700021600, 6, 'temperature', 20.0)
        """
    )
    rows = db.ensemble_inputs(conn, 9999999999)
    assert rows == []


def test_ensemble_inputs_excludes_nonzero_member_id():
    conn = make_output_db()
    conn.execute(
        """
        insert into forecasts
            (model_id, model, member_id, issued_at, valid_at, lead_hours, variable, value)
        values (3, 'weighted_climatological_mean', 1, 1700000000, 1700021600, 6, 'temperature', 20.0)
        """
    )
    rows = db.ensemble_inputs(conn, 1700000000)
    assert rows == []
