import sqlite3
from pathlib import Path

# minimum columns required from the wxlog schema for barogram to function
REQUIRED_COLUMNS: dict[str, set[str]] = {
    "tempest_obs": {
        "station_id", "timestamp", "air_temp", "station_pressure",
        "relative_humidity", "wind_avg", "wind_gust", "wind_direction",
        "precip_accum_day", "solar_radiation", "uv_index", "lightning_count",
    },
    "nws_obs": {
        "station_id", "timestamp", "air_temp", "dew_point", "relative_humidity",
        "wind_speed", "wind_direction", "sea_level_pressure", "sky_cover", "raw_metar",
    },
    "stations": {"station_id", "source", "name"},
}


def open_input_db(path: str) -> sqlite3.Connection:
    # read-only URI mode: barogram never writes to wxlog's database
    p = Path(path).resolve()
    if not p.exists():
        raise FileNotFoundError(f"input database not found: {p}")
    conn = sqlite3.connect(f"file:{p}?mode=ro", uri=True, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def validate_schema(conn: sqlite3.Connection) -> None:
    tables = {row[0] for row in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    )}
    for table, required_cols in REQUIRED_COLUMNS.items():
        if table not in tables:
            raise ValueError(
                f"input database is missing required table '{table}' — "
                f"is this a wxlog database?"
            )
        actual_cols = {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
        missing = required_cols - actual_cols
        if missing:
            raise ValueError(
                f"table '{table}' is missing required columns: "
                f"{', '.join(sorted(missing))}"
            )


def latest_tempest_obs(conn: sqlite3.Connection) -> sqlite3.Row | None:
    return conn.execute(
        """
        SELECT t.*, s.name
        FROM tempest_obs t
        JOIN stations s ON s.station_id = t.station_id
        WHERE s.source = 'tempest'
        ORDER BY t.timestamp DESC
        LIMIT 1
        """
    ).fetchone()


def latest_nws_obs(conn: sqlite3.Connection) -> sqlite3.Row | None:
    return conn.execute(
        """
        SELECT n.*, s.name
        FROM nws_obs n
        JOIN stations s ON s.station_id = n.station_id
        WHERE s.source = 'nws'
        ORDER BY n.timestamp DESC
        LIMIT 1
        """
    ).fetchone()
