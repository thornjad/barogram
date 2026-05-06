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


def test_climo_precip_probability_no_precip(make_input_db_with_precip):
    import datetime
    conn, ts = make_input_db_with_precip
    t = datetime.datetime.fromtimestamp(ts)
    result = db.climo_precip_probability(conn, t.month, t.hour, min_obs=1)
    assert result == 0.0


def test_climo_precip_probability_with_precip(make_input_db_with_precip_rain):
    import datetime
    conn, ts = make_input_db_with_precip_rain
    t = datetime.datetime.fromtimestamp(ts)
    result = db.climo_precip_probability(conn, t.month, t.hour, min_obs=1)
    assert result == 1.0


def test_climo_precip_probability_insufficient(make_input_db_with_precip):
    import datetime
    conn, ts = make_input_db_with_precip
    t = datetime.datetime.fromtimestamp(ts)
    result = db.climo_precip_probability(conn, t.month, t.hour, min_obs=9999)
    assert result is None


# --- precip_event_count ---

def _insert_scored_precip(conn, issued_at, observed):
    conn.execute(
        """
        insert into forecasts
            (model_id, model, member_id, issued_at, valid_at, lead_hours,
             variable, value, observed, mae, scored_at)
        values (200, 'nws', 0, ?, ?, 6, 'precip_prob', 0.1, ?, 0.0, ?)
        """,
        (issued_at, issued_at + 21600, observed, issued_at + 100),
    )


def test_precip_event_count_zero_when_no_rain():
    conn = make_output_db()
    _insert_scored_precip(conn, 1_700_000_000, 0.0)
    assert db.precip_event_count(conn) == 0


def test_precip_event_count_counts_rain_events():
    conn = make_output_db()
    _insert_scored_precip(conn, 1_700_000_000, 1.0)
    _insert_scored_precip(conn, 1_700_010_000, 1.0)
    _insert_scored_precip(conn, 1_700_020_000, 0.0)
    assert db.precip_event_count(conn) == 2


def test_precip_event_count_since_filter():
    conn = make_output_db()
    _insert_scored_precip(conn, 1_700_000_000, 1.0)  # before cutoff
    _insert_scored_precip(conn, 1_700_100_000, 1.0)  # after cutoff
    assert db.precip_event_count(conn, since=1_700_050_000) == 1


# --- huber_delta_per_variable ---

_BASE_ISSUED = 1_700_000_000
_BASE_VALID  = 1_700_021_600


def _insert_scored_forecast_with_error(conn, model_id, model_type, variable, error,
                                       seq: int = 0):
    """seq offsets issued_at so multiple calls with the same key don't collide."""
    conn.execute(
        "insert or ignore into models (id, name, type) values (?, ?, ?)",
        (model_id, f"model_{model_id}", model_type),
    )
    conn.execute(
        """
        insert into forecasts
            (model_id, model, member_id, issued_at, valid_at, lead_hours,
             variable, value, observed, error, mae, scored_at)
        values (?, ?, 0, ?, ?, 6, ?, 0.0, 0.0, ?, abs(?), 1700021700)
        """,
        (model_id, f"model_{model_id}",
         _BASE_ISSUED + seq * 3600, _BASE_VALID + seq * 3600,
         variable, error, error),
    )


def test_huber_delta_per_variable_percentile():
    """80th percentile of 10 sorted errors [0.1..1.0] should be 0.8 (index 7)."""
    conn = make_output_db()
    for i, e in enumerate([0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]):
        _insert_scored_forecast_with_error(conn, 1, "base", "temperature", e, seq=i)
    deltas = db.huber_delta_per_variable(conn, percentile=80.0)
    assert abs(deltas["temperature"] - 0.8) < 1e-9


def test_huber_delta_per_variable_excludes_external():
    """External model errors must not influence delta computation."""
    conn = make_output_db()
    # base model: errors 1.0..10.0
    for i, e in enumerate(range(1, 11)):
        _insert_scored_forecast_with_error(conn, 1, "base", "temperature", float(e), seq=i)
    # external model: errors 1000.0 — should be excluded
    _insert_scored_forecast_with_error(conn, 200, "external", "temperature", 1000.0, seq=10)
    deltas = db.huber_delta_per_variable(conn, percentile=80.0)
    # 80th percentile of [1..10] = index 7 = 8.0; would be skewed if 1000 were included
    assert deltas["temperature"] < 100.0


def test_huber_delta_per_variable_no_data_returns_empty():
    conn = make_output_db()
    deltas = db.huber_delta_per_variable(conn, percentile=80.0)
    assert deltas == {}


# --- save_weights / load_weights roundtrip ---

def test_save_load_weights_roundtrip():
    conn = make_output_db()
    weights = {
        (1, "temperature", 6, 0): 0.5,
        (2, "temperature", 6, 0): 0.3,
        (1, "dewpoint", 12, 2): 0.7,
    }
    db.save_weights(conn, 100, weights, updated_at=1700000000)
    loaded = db.load_weights(conn, 100)
    assert loaded == weights


def test_save_weights_replaces_existing():
    conn = make_output_db()
    db.save_weights(conn, 100, {(1, "temperature", 6, 0): 0.5}, updated_at=1700000000)
    db.save_weights(conn, 100, {(1, "temperature", 6, 0): 0.9}, updated_at=1700000001)
    loaded = db.load_weights(conn, 100)
    assert abs(loaded[(1, "temperature", 6, 0)] - 0.9) < 1e-9


def test_load_weights_empty_when_no_rows():
    conn = make_output_db()
    assert db.load_weights(conn, 100) == {}


# --- raw_errors_by_sector ---

def _insert_scored_member_forecast(conn, model_id, model_type, member_id, variable,
                                   valid_at, error):
    conn.execute(
        "insert or ignore into models (id, name, type) values (?, ?, ?)",
        (model_id, f"model_{model_id}", model_type),
    )
    conn.execute(
        "insert or ignore into members (model_id, member_id, name) values (?, ?, ?)",
        (model_id, member_id, f"m{member_id}"),
    )
    conn.execute(
        """
        insert into forecasts
            (model_id, model, member_id, issued_at, valid_at, lead_hours,
             variable, value, observed, error, mae, scored_at)
        values (?, ?, ?, 1700000000, ?, 6, ?, 0.0, 0.0, ?, abs(?), 1700100000)
        """,
        (model_id, f"model_{model_id}", member_id, valid_at, variable, error, error),
    )


# 2023-11-14 00:00 UTC; adjust for localtime sector tests
# Hour 0 local → sector 0, hour 6 → sector 1, hour 12 → sector 2, hour 18 → sector 3
import datetime as _dt
import time as _time

_BASE_DATE = _dt.date(2023, 11, 14)


def _local_epoch(hour: int) -> int:
    """Unix timestamp for 2023-11-14 at the given local hour."""
    d = _dt.datetime(_BASE_DATE.year, _BASE_DATE.month, _BASE_DATE.day, hour, 0, 0)
    return int(d.timestamp())


def test_raw_errors_by_sector_excludes_external():
    conn = make_output_db()
    ts = _local_epoch(12)
    _insert_scored_member_forecast(conn, 100, "ensemble", 1, "temperature", ts, 2.0)
    _insert_scored_member_forecast(conn, 200, "external", 1, "temperature", ts, 99.0)
    rows = db.raw_errors_by_sector(conn)
    model_ids = {r["model_id"] for r in rows}
    assert 200 not in model_ids
    assert 100 in model_ids


def test_raw_errors_by_sector_sector_boundaries():
    conn = make_output_db()
    cases = [
        (0, 0), (5, 0),   # hour 0, 5 → sector 0
        (6, 1), (11, 1),  # hour 6, 11 → sector 1
        (12, 2), (17, 2), # hour 12, 17 → sector 2
        (18, 3), (23, 3), # hour 18, 23 → sector 3
    ]
    for hour, expected_sector in cases:
        ts = _local_epoch(hour)
        _insert_scored_member_forecast(conn, 1, "base", 1, "temperature", ts, float(hour))
    rows = db.raw_errors_by_sector(conn)
    sector_by_error = {r["error"]: r["sector"] for r in rows}
    for hour, expected_sector in cases:
        assert sector_by_error[float(hour)] == expected_sector, \
            f"hour={hour} expected sector={expected_sector}, got {sector_by_error.get(float(hour))}"


def test_raw_errors_by_sector_includes_member_id_zero():
    """member_id=0 rows are returned; callers filter them as needed."""
    conn = make_output_db()
    ts = _local_epoch(12)
    _insert_scored_member_forecast(conn, 100, "ensemble", 0, "temperature", ts, 1.0)
    _insert_scored_member_forecast(conn, 100, "ensemble", 1, "temperature", ts, 2.0)
    rows = db.raw_errors_by_sector(conn)
    member_ids = {r["member_id"] for r in rows}
    assert 0 in member_ids
    assert 1 in member_ids


# --- run_migrations duplicate version number ---

def test_run_migrations_duplicate_version_both_run_on_fresh_install(tmp_path):
    """On a fresh install, duplicate version files both run (current never updates mid-loop)."""
    (tmp_path / "001_create.sql").write_text(
        "create table if not exists t (x text);"
    )
    (tmp_path / "002_a_insert.sql").write_text(
        "insert into t (x) values ('from_a');"
    )
    (tmp_path / "002_b_insert.sql").write_text(
        "insert into t (x) values ('from_b');"
    )
    conn = sqlite3.connect(":memory:", isolation_level=None)
    conn.row_factory = sqlite3.Row
    db.run_migrations(conn, tmp_path)
    rows = [r[0] for r in conn.execute("select x from t").fetchall()]
    # both run since current=0 and never updates mid-loop
    assert "from_a" in rows
    assert "from_b" in rows


def test_run_migrations_duplicate_version_second_skipped_on_upgrade(tmp_path):
    """On upgrade from exactly version 2, the second 002_ file is skipped — the collision bug."""
    (tmp_path / "001_create.sql").write_text(
        "create table if not exists t (x text);"
    )
    (tmp_path / "002_a_model.sql").write_text(
        "insert into t (x) values ('model_row');"
    )
    (tmp_path / "003_members.sql").write_text(
        "insert into t (x) values ('member_row');"
    )
    # simulate a DB already at version 2 but missing the model row
    conn = sqlite3.connect(":memory:", isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("create table metadata (key text primary key, value text)")
    conn.execute("create table t (x text)")
    conn.execute("insert into metadata values ('schema_version', '2')")
    # version 2 is already "done", so 002_a_model.sql will be skipped
    # version 3 will run — but any dependency on 002_a's content would break
    db.run_migrations(conn, tmp_path)
    rows = [r[0] for r in conn.execute("select x from t").fetchall()]
    assert "model_row" not in rows  # 002_a skipped because version 2 <= current 2
    assert "member_row" in rows     # 003 ran
