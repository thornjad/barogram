import sqlite3
import time

import db

# barogram variable name -> tempest_obs column name
# mirrors models/persistence.py VARIABLES; kept separate so scoring
# does not depend on any model module
_OBS_COLUMN = {
    "temperature": "air_temp",
    "dewpoint":    "dew_point",
    "pressure":    "station_pressure",
    "wind_speed":  "wind_avg",
}


def run(conn_in: sqlite3.Connection, conn_out: sqlite3.Connection) -> dict:
    """Score all forecasts whose valid_at has passed and have not yet been scored.

    Returns {"scored": N, "skipped": M} where skipped means no Tempest obs was
    found within the matching window, or the relevant obs column was NULL.
    """
    now = int(time.time())
    unscored = conn_out.execute(
        """
        SELECT id, variable, value, valid_at
        FROM forecasts
        WHERE valid_at < ? AND scored_at IS NULL
        """,
        (now,),
    ).fetchall()

    scored_rows = []
    skipped = 0
    for row in unscored:
        if row["value"] is None:
            skipped += 1
            continue
        obs = db.nearest_tempest_obs(conn_in, row["valid_at"])
        if obs is None:
            skipped += 1
            continue
        observed = obs[_OBS_COLUMN[row["variable"]]]
        if observed is None:
            skipped += 1
            continue
        error = row["value"] - observed
        scored_rows.append({
            "id": row["id"],
            "observed": observed,
            "error": error,
            "mae": abs(error),
            "scored_at": now,
        })

    if scored_rows:
        db.update_scored_forecasts(conn_out, scored_rows)

    return {"scored": len(scored_rows), "skipped": skipped}
