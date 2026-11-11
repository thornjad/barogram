import sqlite3
import time
from pathlib import Path

import pytest

import db as dbmod
import models._confidence as _confidence

collect_ignore_glob = ["*sync-conflict*"]

_MIGRATIONS_DIR = Path(__file__).parent.parent / "migrations"

_DEFAULT_TEST_SPREAD = 2.0  # arbitrary positive spread, applied to every
                            # (variable, lead_hours) cell a test might use -- most
                            # confidence tests only care that confidence is nonzero
                            # and differentiates members, not this exact number. A
                            # test that asserts a precise confidence value sets its
                            # own spread via set_spread before calling in.


@pytest.fixture(autouse=True)
def _spread_default():
    """models/_confidence.py's confidence math now reads its natural-variability
    yardstick from a module-level registry (set_spread) instead of computing one
    from each call's own history -- see the 2026-09-23 confidence rework. Without
    this, every test would need its own boilerplate setup just to get a
    nonzero confidence at all. Reset after each test so one test's spread
    choice can't leak into the next."""
    _confidence.set_spread({
        (variable, lead_hours): _DEFAULT_TEST_SPREAD
        for variable in ("temperature", "dewpoint", "pressure")
        for lead_hours in range(1, 25)
    })
    yield
    _confidence.set_spread({})


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
            wind_lull         real,
            wind_direction    real,
            precip            real,
            precip_accum_day  real,
            relative_humidity real,
            solar_radiation   real,
            uv_index          real,
            lightning_count   integer,
            precip_type       integer,
            lightning_avg_distance         real,
            lightning_strike_last_distance real,
            lightning_strike_count_last_3hr integer,
            nc_rain           real,
            battery           real
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
        "uv_index": None,
        "wind_gust": None,
        "lightning_count": None,
        "precip_accum_day": None,
    }
