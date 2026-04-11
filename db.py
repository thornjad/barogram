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


def recent_tempest_obs(conn: sqlite3.Connection, limit: int = 50) -> list:
    return conn.execute(
        """
        SELECT t.*, s.name
        FROM tempest_obs t
        JOIN stations s ON s.station_id = t.station_id
        WHERE s.source = 'tempest'
        ORDER BY t.timestamp DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()


def recent_nws_obs(conn: sqlite3.Connection, limit: int = 50) -> list:
    return conn.execute(
        """
        SELECT n.*, s.name
        FROM nws_obs n
        JOIN stations s ON s.station_id = n.station_id
        WHERE s.source = 'nws'
        ORDER BY n.timestamp DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()


def open_output_db(path: str) -> sqlite3.Connection:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(p), isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def run_migrations(conn: sqlite3.Connection, migrations_dir: Path) -> None:
    # bootstrap metadata table before checking schema_version
    conn.execute(
        "CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT)"
    )
    row = conn.execute(
        "SELECT value FROM metadata WHERE key = 'schema_version'"
    ).fetchone()
    current = int(row[0]) if row else 0

    for f in sorted(migrations_dir.glob("[0-9][0-9][0-9]_*.sql")):
        version = int(f.name[:3])
        if version <= current:
            continue
        # executescript issues an implicit commit before running; DDL migrations
        # are idempotent via IF NOT EXISTS so re-running on partial failure is safe
        conn.executescript(f.read_text())
        conn.execute(
            "INSERT OR REPLACE INTO metadata (key, value) VALUES ('schema_version', ?)",
            (str(version),),
        )


def insert_forecasts(conn: sqlite3.Connection, rows: list[dict]) -> None:
    conn.execute("BEGIN")
    try:
        conn.executemany(
            """
            INSERT INTO forecasts
                (model_id, model, issued_at, valid_at, lead_hours, variable, value)
            VALUES
                (:model_id, :model, :issued_at, :valid_at, :lead_hours, :variable, :value)
            """,
            rows,
        )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
