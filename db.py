import sqlite3
from pathlib import Path

# minimum columns required from the wxlog schema for barogram to function
REQUIRED_COLUMNS: dict[str, set[str]] = {
    "tempest_obs": {
        "station_id", "timestamp", "air_temp", "station_pressure",
        "dew_point", "wind_avg", "wind_gust", "wind_direction",
        "precip_accum_day", "solar_radiation", "uv_index", "lightning_count",
    },
    "nws_obs": {
        "station_id", "timestamp", "air_temp", "dew_point",
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


def nearest_tempest_obs(
    conn: sqlite3.Connection, timestamp: int, window_sec: int = 1800
) -> sqlite3.Row | None:
    return conn.execute(
        """
        SELECT t.air_temp, t.dew_point, t.station_pressure, t.wind_avg
        FROM tempest_obs t
        JOIN stations s ON s.station_id = t.station_id
        WHERE s.source = 'tempest'
          AND t.timestamp BETWEEN ? AND ?
        ORDER BY ABS(t.timestamp - ?) ASC
        LIMIT 1
        """,
        (timestamp - window_sec, timestamp + window_sec, timestamp),
    ).fetchone()


def climo_bucket_means(
    conn: sqlite3.Connection,
    month: int,
    hour: int,
    min_obs: int = 30,
) -> dict[str, float | None]:
    row = conn.execute(
        """
        SELECT
            AVG(t.air_temp)           AS temperature,
            AVG(t.dew_point)          AS dewpoint,
            AVG(t.station_pressure)   AS pressure,
            AVG(t.wind_avg)           AS wind_speed,
            COUNT(*)                  AS n
        FROM tempest_obs t
        JOIN stations s ON s.station_id = t.station_id
        WHERE s.source = 'tempest'
          AND CAST(strftime('%m', datetime(t.timestamp, 'unixepoch', 'localtime')) AS INTEGER) = ?
          AND CAST(strftime('%H', datetime(t.timestamp, 'unixepoch', 'localtime')) AS INTEGER) = ?
        """,
        (month, hour),
    ).fetchone()
    if row is None or row["n"] < min_obs:
        return {}
    return {
        "temperature": row["temperature"],
        "dewpoint": row["dewpoint"],
        "pressure": row["pressure"],
        "wind_speed": row["wind_speed"],
    }


def climo_bucket_obs(
    conn: sqlite3.Connection,
    month: int,
    hour: int,
) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT t.timestamp, t.air_temp, t.dew_point,
               t.station_pressure, t.wind_avg
        FROM tempest_obs t
        JOIN stations s ON s.station_id = t.station_id
        WHERE s.source = 'tempest'
          AND CAST(strftime('%m', datetime(t.timestamp, 'unixepoch', 'localtime')) AS INTEGER) = ?
          AND CAST(strftime('%H', datetime(t.timestamp, 'unixepoch', 'localtime')) AS INTEGER) = ?
        ORDER BY t.timestamp DESC
        """,
        (month, hour),
    ).fetchall()


def update_scored_forecasts(conn: sqlite3.Connection, rows: list[dict]) -> None:
    conn.execute("BEGIN")
    try:
        conn.executemany(
            """
            UPDATE forecasts
            SET observed = :observed, error = :error, mae = :mae, scored_at = :scored_at
            WHERE id = :id
            """,
            rows,
        )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise


def score_summary(conn: sqlite3.Connection) -> list:
    return conn.execute(
        """
        SELECT f.model_id, f.model, m.type, f.member_id, mem.name AS member_name,
               f.variable, f.lead_hours,
               COUNT(*) AS n, AVG(f.mae) AS avg_mae, AVG(f.error) AS avg_bias
        FROM forecasts f
        JOIN models m ON m.id = f.model_id
        LEFT JOIN members mem ON mem.model_id = f.model_id AND mem.member_id = f.member_id
        WHERE f.scored_at IS NOT NULL
        GROUP BY f.model_id, f.model, m.type, f.member_id, mem.name, f.variable, f.lead_hours
        ORDER BY f.model_id, f.member_id, f.variable, f.lead_hours
        """
    ).fetchall()


def score_summary_since(conn: sqlite3.Connection, since: int) -> list:
    return conn.execute(
        """
        SELECT f.model_id, f.model, m.type, f.member_id, mem.name AS member_name,
               f.variable, f.lead_hours,
               COUNT(*) AS n, AVG(f.mae) AS avg_mae, AVG(f.error) AS avg_bias
        FROM forecasts f
        JOIN models m ON m.id = f.model_id
        LEFT JOIN members mem ON mem.model_id = f.model_id AND mem.member_id = f.member_id
        WHERE f.scored_at IS NOT NULL AND f.issued_at >= ?
        GROUP BY f.model_id, f.model, m.type, f.member_id, mem.name, f.variable, f.lead_hours
        ORDER BY f.model_id, f.member_id, f.variable, f.lead_hours
        """,
        (since,),
    ).fetchall()


def score_summary_last_n_runs(conn: sqlite3.Connection, n: int) -> list:
    return conn.execute(
        """
        WITH recent AS (
            SELECT DISTINCT issued_at
            FROM forecasts
            WHERE scored_at IS NOT NULL
            ORDER BY issued_at DESC
            LIMIT ?
        )
        SELECT f.model_id, f.model, m.type, f.member_id, mem.name AS member_name,
               f.variable, f.lead_hours,
               COUNT(*) AS n, AVG(f.mae) AS avg_mae, AVG(f.error) AS avg_bias
        FROM forecasts f
        JOIN models m ON m.id = f.model_id
        LEFT JOIN members mem ON mem.model_id = f.model_id AND mem.member_id = f.member_id
        JOIN recent r ON r.issued_at = f.issued_at
        WHERE f.scored_at IS NOT NULL
        GROUP BY f.model_id, f.model, m.type, f.member_id, mem.name, f.variable, f.lead_hours
        ORDER BY f.model_id, f.member_id, f.variable, f.lead_hours
        """,
        (n,),
    ).fetchall()


def score_timeseries(conn: sqlite3.Connection) -> list:
    """Per-run average MAE by model/member/variable/lead, ordered by run time."""
    return conn.execute(
        """
        SELECT f.model, m.type, f.member_id, f.variable, f.lead_hours, f.issued_at,
               AVG(f.mae) AS avg_mae
        FROM forecasts f
        JOIN models m ON m.id = f.model_id
        WHERE f.scored_at IS NOT NULL
        GROUP BY f.model, m.type, f.member_id, f.variable, f.lead_hours, f.issued_at
        ORDER BY f.issued_at
        """
    ).fetchall()


def latest_forecast_per_model(conn: sqlite3.Connection) -> list:
    """All rows from each model/member's most recent run, ordered by type then name."""
    return conn.execute(
        """
        SELECT f.model_id, f.model, f.member_id, mem.name AS member_name,
               m.type, f.issued_at, f.variable, f.lead_hours, f.value, f.valid_at
        FROM forecasts f
        JOIN models m ON m.id = f.model_id
        LEFT JOIN members mem ON mem.model_id = f.model_id AND mem.member_id = f.member_id
        WHERE f.issued_at = (
            SELECT MAX(f2.issued_at)
            FROM forecasts f2
            WHERE f2.model_id = f.model_id AND f2.member_id = f.member_id
        )
        ORDER BY m.type, f.model, f.member_id, f.variable, f.lead_hours
        """
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
    normalized = [
        {**row, "member_id": row.get("member_id", 0), "spread": row.get("spread")}
        for row in rows
    ]
    conn.execute("BEGIN")
    try:
        conn.executemany(
            """
            INSERT INTO forecasts
                (model_id, model, member_id, issued_at, valid_at, lead_hours, variable, value, spread)
            VALUES
                (:model_id, :model, :member_id, :issued_at, :valid_at, :lead_hours, :variable, :value, :spread)
            """,
            normalized,
        )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
