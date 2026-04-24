import sqlite3
import time
from pathlib import Path

import db as dbmod

_MIGRATIONS_DIR = Path(__file__).parent.parent / "migrations"


def make_input_db() -> sqlite3.Connection:
    """Writable in-memory DB with wxlog schema and one pre-seeded Tempest station.

    NOT opened via db.open_input_db() — that uses URI read-only mode, which
    prevents tests from inserting rows. Tests insert their own obs as needed.
    """
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript("""
        create table stations (
            station_id text primary key,
            source     text not null,
            name       text,
            latitude   real,
            longitude  real
        );
        create table tempest_obs (
            station_id        text not null,
            timestamp         integer not null,
            air_temp          real,
            dew_point         real,
            station_pressure  real,
            wind_avg          real,
            wind_gust         real,
            wind_direction    real,
            precip_accum_day  real,
            solar_radiation   real,
            uv_index          real,
            lightning_count   integer
        );
        create table nws_obs (
            station_id        text not null,
            timestamp         integer not null,
            air_temp          real,
            dew_point         real,
            wind_speed        real,
            wind_direction    real,
            sea_level_pressure real,
            sky_cover         text,
            raw_metar         text
        );
        insert into stations (station_id, source, name, latitude, longitude)
            values ('KTEST', 'tempest', 'Test Station', 44.98, -93.27);
    """)
    return conn


def make_output_db() -> sqlite3.Connection:
    """In-memory output DB with all migrations applied.

    isolation_level=None (autocommit) matches db.open_output_db() so that
    explicit BEGIN/COMMIT calls in db.insert_forecasts and
    db.update_scored_forecasts work correctly.
    """
    conn = sqlite3.connect(":memory:", isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("pragma foreign_keys=on")
    dbmod.run_migrations(conn, _MIGRATIONS_DIR)
    return conn


def make_obs(ts: int | None = None) -> dict:
    """Synthetic obs dict usable as the obs argument to any model run()."""
    if ts is None:
        ts = int(time.time()) - 3600
    return {
        "timestamp": ts,
        "air_temp": 20.0,
        "dew_point": 12.0,
        "station_pressure": 1013.2,
        "wind_avg": 3.5,
        "wind_direction": None,
        "solar_radiation": None,
        "wind_gust": None,
        "lightning_count": None,
        "precip_accum_day": None,
    }
