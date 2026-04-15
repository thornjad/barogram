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
    "stations": {"station_id", "source", "name", "latitude", "longitude", "elevation", "agl"},
}


def get_metadata(conn: sqlite3.Connection, key: str) -> str | None:
    row = conn.execute(
        "select value from metadata where key = ?", (key,)
    ).fetchone()
    return row[0] if row else None


def set_metadata(conn: sqlite3.Connection, key: str, value: str) -> None:
    conn.execute(
        "insert or replace into metadata (key, value) values (?, ?)",
        (key, value),
    )


def open_input_db(path: str) -> sqlite3.Connection:
    # read-only URI mode: barogram never writes to wxlog's database
    p = Path(path).resolve()
    if not p.exists():
        raise FileNotFoundError(f"input database not found: {p}")
    conn = sqlite3.connect(f"file:{p}?mode=ro", uri=True, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("pragma foreign_keys=on")
    return conn


def validate_schema(conn: sqlite3.Connection) -> None:
    tables = {row[0] for row in conn.execute(
        "select name from sqlite_master where type='table'"
    )}
    for table, required_cols in REQUIRED_COLUMNS.items():
        if table not in tables:
            raise ValueError(
                f"input database is missing required table '{table}' — "
                f"is this a wxlog database?"
            )
        actual_cols = {row[1] for row in conn.execute(f"pragma table_info({table})")}
        missing = required_cols - actual_cols
        if missing:
            raise ValueError(
                f"table '{table}' is missing required columns: "
                f"{', '.join(sorted(missing))}"
            )


def latest_tempest_obs(conn: sqlite3.Connection) -> sqlite3.Row | None:
    return conn.execute(
        """
        select t.*, s.name
        from tempest_obs t
        join stations s on s.station_id = t.station_id
        where s.source = 'tempest'
        order by t.timestamp desc
        limit 1
        """
    ).fetchone()


def latest_nws_obs(conn: sqlite3.Connection) -> sqlite3.Row | None:
    return conn.execute(
        """
        select n.*, s.name
        from nws_obs n
        join stations s on s.station_id = n.station_id
        where s.source = 'nws'
        order by n.timestamp desc
        limit 1
        """
    ).fetchone()


def recent_tempest_obs(conn: sqlite3.Connection, limit: int = 50) -> list:
    return conn.execute(
        """
        select t.*, s.name
        from tempest_obs t
        join stations s on s.station_id = t.station_id
        where s.source = 'tempest'
        order by t.timestamp desc
        limit ?
        """,
        (limit,),
    ).fetchall()


def recent_nws_obs(conn: sqlite3.Connection, limit: int = 50) -> list:
    return conn.execute(
        """
        select n.*, s.name
        from nws_obs n
        join stations s on s.station_id = n.station_id
        where s.source = 'nws'
        order by n.timestamp desc
        limit ?
        """,
        (limit,),
    ).fetchall()


def nearest_tempest_obs(
    conn: sqlite3.Connection, timestamp: int, window_sec: int = 1800
) -> sqlite3.Row | None:
    return conn.execute(
        """
        select t.air_temp, t.dew_point, t.station_pressure, t.wind_avg
        from tempest_obs t
        join stations s on s.station_id = t.station_id
        where s.source = 'tempest'
          and t.timestamp between ? and ?
        order by abs(t.timestamp - ?) asc
        limit 1
        """,
        (timestamp - window_sec, timestamp + window_sec, timestamp),
    ).fetchone()


def tempest_station_location(conn: sqlite3.Connection) -> tuple[float, float] | None:
    row = conn.execute(
        "select latitude, longitude from stations where source = 'tempest' limit 1"
    ).fetchone()
    if row is None or row["latitude"] is None or row["longitude"] is None:
        return None
    return (row["latitude"], row["longitude"])


def tempest_station_elevation(conn: sqlite3.Connection) -> float:
    """Return the effective elevation (m ASL) of the Tempest station.

    Combines the station's ground elevation with the sensor's height above
    ground (agl) so that SLP reduction uses the actual sensor altitude.
    Returns 0.0 if elevation data is unavailable.
    """
    row = conn.execute(
        "select elevation, agl from stations where source = 'tempest' limit 1"
    ).fetchone()
    if row is None:
        return 0.0
    elev = row["elevation"] or 0.0
    agl = row["agl"] or 0.0
    return elev + agl


def climo_bucket_means(
    conn: sqlite3.Connection,
    month: int,
    hour: int,
    min_obs: int = 30,
) -> dict[str, float | None]:
    row = conn.execute(
        """
        select
            avg(t.air_temp)           as temperature,
            avg(t.dew_point)          as dewpoint,
            avg(t.station_pressure)   as pressure,
            avg(t.wind_avg)           as wind_speed,
            count(*)                  as n
        from tempest_obs t
        join stations s on s.station_id = t.station_id
        where s.source = 'tempest'
          and cast(strftime('%m', datetime(t.timestamp, 'unixepoch', 'localtime')) as integer) = ?
          and cast(strftime('%H', datetime(t.timestamp, 'unixepoch', 'localtime')) as integer) = ?
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
        select t.timestamp, t.air_temp, t.dew_point,
               t.station_pressure, t.wind_avg
        from tempest_obs t
        join stations s on s.station_id = t.station_id
        where s.source = 'tempest'
          and cast(strftime('%m', datetime(t.timestamp, 'unixepoch', 'localtime')) as integer) = ?
          and cast(strftime('%H', datetime(t.timestamp, 'unixepoch', 'localtime')) as integer) = ?
        order by t.timestamp desc
        """,
        (month, hour),
    ).fetchall()


def update_scored_forecasts(conn: sqlite3.Connection, rows: list[dict]) -> None:
    conn.execute("begin")
    try:
        conn.executemany(
            """
            update forecasts
            set observed = :observed, error = :error, mae = :mae, scored_at = :scored_at
            where id = :id
            """,
            rows,
        )
        conn.execute("commit")
    except Exception:
        conn.execute("rollback")
        raise


def score_summary(conn: sqlite3.Connection) -> list:
    return conn.execute(
        """
        select f.model_id, f.model, m.type, f.member_id, mem.name as member_name,
               f.variable, f.lead_hours,
               count(*) as n, avg(f.mae) as avg_mae, avg(f.error) as avg_bias
        from forecasts f
        join models m on m.id = f.model_id
        left join members mem on mem.model_id = f.model_id and mem.member_id = f.member_id
        where f.scored_at is not null
        group by f.model_id, f.model, m.type, f.member_id, mem.name, f.variable, f.lead_hours
        order by f.model_id, f.member_id, f.variable, f.lead_hours
        """
    ).fetchall()


def score_summary_since(conn: sqlite3.Connection, since: int) -> list:
    return conn.execute(
        """
        select f.model_id, f.model, m.type, f.member_id, mem.name as member_name,
               f.variable, f.lead_hours,
               count(*) as n, avg(f.mae) as avg_mae, avg(f.error) as avg_bias
        from forecasts f
        join models m on m.id = f.model_id
        left join members mem on mem.model_id = f.model_id and mem.member_id = f.member_id
        where f.scored_at is not null and f.issued_at >= ?
        group by f.model_id, f.model, m.type, f.member_id, mem.name, f.variable, f.lead_hours
        order by f.model_id, f.member_id, f.variable, f.lead_hours
        """,
        (since,),
    ).fetchall()


def score_summary_last_n_runs(conn: sqlite3.Connection, n: int) -> list:
    return conn.execute(
        """
        with recent as (
            select distinct issued_at
            from forecasts
            where scored_at is not null and lead_hours = 24
            order by issued_at desc
            limit ?
        )
        select f.model_id, f.model, m.type, f.member_id, mem.name as member_name,
               f.variable, f.lead_hours,
               count(*) as n, avg(f.mae) as avg_mae, avg(f.error) as avg_bias
        from forecasts f
        join models m on m.id = f.model_id
        left join members mem on mem.model_id = f.model_id and mem.member_id = f.member_id
        join recent r on r.issued_at = f.issued_at
        where f.scored_at is not null
        group by f.model_id, f.model, m.type, f.member_id, mem.name, f.variable, f.lead_hours
        order by f.model_id, f.member_id, f.variable, f.lead_hours
        """,
        (n,),
    ).fetchall()


def score_timeseries(conn: sqlite3.Connection) -> list:
    """Per-run average MAE by model/member/variable/lead, ordered by run time."""
    return conn.execute(
        """
        select f.model_id, f.model, m.type, f.member_id, f.variable, f.lead_hours, f.issued_at,
               avg(f.mae) as avg_mae
        from forecasts f
        join models m on m.id = f.model_id
        where f.scored_at is not null
        group by f.model_id, f.model, m.type, f.member_id, f.variable, f.lead_hours, f.issued_at
        order by f.issued_at
        """
    ).fetchall()


def bias_timeseries(conn: sqlite3.Connection) -> list:
    """Per-run average signed error by model/member/variable/lead, ordered by run time."""
    return conn.execute(
        """
        select f.model_id, f.model, m.type, f.member_id, f.variable, f.lead_hours, f.issued_at,
               avg(f.error) as avg_bias
        from forecasts f
        join models m on m.id = f.model_id
        where f.scored_at is not null
        group by f.model_id, f.model, m.type, f.member_id, f.variable, f.lead_hours, f.issued_at
        order by f.issued_at
        """
    ).fetchall()


def diurnal_errors(conn: sqlite3.Connection) -> list:
    """Average bias and MAE grouped by hour of valid_at (local time), model, and variable."""
    return conn.execute(
        """
        select
            cast(strftime('%H', datetime(f.valid_at, 'unixepoch', 'localtime')) as integer) as hour,
            f.model_id, f.model, m.type, f.member_id, f.variable,
            count(*) as n, avg(f.error) as avg_bias, avg(f.mae) as avg_mae
        from forecasts f
        join models m on m.id = f.model_id
        where f.scored_at is not null
        group by hour, f.model_id, f.model, m.type, f.member_id, f.variable
        order by f.model_id, f.member_id, f.variable, hour
        """
    ).fetchall()


def error_distribution(conn: sqlite3.Connection) -> list:
    """Raw signed error values for member_id=0 rows, for histogram analysis."""
    return conn.execute(
        """
        select f.model_id, f.model, m.type, f.variable, f.lead_hours, f.error
        from forecasts f
        join models m on m.id = f.model_id
        where f.scored_at is not null
          and f.member_id = 0
          and f.error is not null
        order by f.model_id, f.variable, f.lead_hours
        """
    ).fetchall()


def latest_forecast_per_model(conn: sqlite3.Connection) -> list:
    """All rows from each model/member's most recent run, ordered by type then name."""
    return conn.execute(
        """
        select f.model_id, f.model, f.member_id, mem.name as member_name,
               m.type, f.issued_at, f.variable, f.lead_hours, f.value, f.spread, f.valid_at
        from forecasts f
        join models m on m.id = f.model_id
        left join members mem on mem.model_id = f.model_id and mem.member_id = f.member_id
        where f.issued_at = (
            select max(f2.issued_at)
            from forecasts f2
            where f2.model_id = f.model_id and f2.member_id = f.member_id
        )
        order by m.type, f.model, f.member_id, f.variable, f.lead_hours
        """
    ).fetchall()


def ensemble_inputs(conn: sqlite3.Connection, issued_at: int) -> list:
    """Fetch member_id=0 rows from base models for a given forecast run."""
    return conn.execute(
        """
        select f.model_id, f.variable, f.lead_hours, f.value, f.valid_at
        from forecasts f
        join models m on m.id = f.model_id
        where f.issued_at = ? and f.member_id = 0 and m.type = 'base'
        """,
        (issued_at,),
    ).fetchall()


def all_weights_with_members(conn: sqlite3.Connection) -> list:
    return conn.execute(
        """
        select w.model_id, m.name as model_name, w.member_id,
               mem.name as member_name, w.variable, w.lead_hours, w.weight
        from weights w
        join models m on m.id = w.model_id
        left join members mem on mem.model_id = w.model_id and mem.member_id = w.member_id
        order by w.model_id, w.member_id, w.variable, w.lead_hours
        """
    ).fetchall()


def load_weights(conn: sqlite3.Connection, model_id: int) -> dict:
    rows = conn.execute(
        """
        select member_id, variable, lead_hours, weight
        from weights
        where model_id = ?
        """,
        (model_id,),
    ).fetchall()
    return {(row["member_id"], row["variable"], row["lead_hours"]): row["weight"]
            for row in rows}


def save_weights(
    conn: sqlite3.Connection,
    model_id: int,
    weights_by_key: dict,
    updated_at: int,
) -> None:
    rows = [
        {
            "model_id": model_id,
            "member_id": member_id,
            "variable": variable,
            "lead_hours": lead_hours,
            "weight": weight,
            "updated_at": updated_at,
        }
        for (member_id, variable, lead_hours), weight in weights_by_key.items()
    ]
    conn.execute("begin")
    try:
        conn.executemany(
            """
            insert or replace into weights
                (model_id, member_id, variable, lead_hours, weight, updated_at)
            values
                (:model_id, :member_id, :variable, :lead_hours, :weight, :updated_at)
            """,
            rows,
        )
        conn.execute("commit")
    except Exception:
        conn.execute("rollback")
        raise


def tempest_obs_in_range(conn: sqlite3.Connection, start_ts: int, end_ts: int) -> list:
    return conn.execute(
        """
        select t.timestamp, t.air_temp, t.dew_point, t.station_pressure, t.wind_avg
        from tempest_obs t
        join stations s on s.station_id = t.station_id
        where s.source = 'tempest'
          and t.timestamp >= ? and t.timestamp <= ?
        order by t.timestamp asc
        """,
        (start_ts, end_ts),
    ).fetchall()


def open_output_db(path: str) -> sqlite3.Connection:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(p), isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("pragma journal_mode=WAL")
    conn.execute("pragma foreign_keys=on")
    return conn


def run_migrations(conn: sqlite3.Connection, migrations_dir: Path) -> None:
    # bootstrap metadata table before checking schema_version
    conn.execute(
        "create table if not exists metadata (key text primary key, value text)"
    )
    row = conn.execute(
        "select value from metadata where key = 'schema_version'"
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
            "insert or replace into metadata (key, value) values ('schema_version', ?)",
            (str(version),),
        )


def insert_forecasts(conn: sqlite3.Connection, rows: list[dict]) -> None:
    normalized = [
        {**row, "member_id": row.get("member_id", 0), "spread": row.get("spread")}
        for row in rows
    ]
    conn.execute("begin")
    try:
        conn.executemany(
            """
            insert into forecasts
                (model_id, model, member_id, issued_at, valid_at, lead_hours, variable, value, spread)
            values
                (:model_id, :model, :member_id, :issued_at, :valid_at, :lead_hours, :variable, :value, :spread)
            """,
            normalized,
        )
        conn.execute("commit")
    except Exception:
        conn.execute("rollback")
        raise
