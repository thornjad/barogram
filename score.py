import bisect
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
}


def _build_obs_index(obs_rows: list) -> tuple[list, dict]:
    """Build a sorted timestamp list and dict for O(log n) nearest-neighbor lookup."""
    obs_by_ts: dict = {}
    for row in obs_rows:
        obs_by_ts[row["timestamp"]] = row  # last row wins on duplicate timestamp
    sorted_ts = sorted(obs_by_ts)
    return sorted_ts, obs_by_ts


def _find_nearest_obs(
    sorted_ts: list, obs_by_ts: dict, target: int, window_sec: int = 1800
) -> sqlite3.Row | None:
    """Return the obs row nearest to target within window_sec, or None.

    Replicates nearest_tempest_obs semantics: inclusive window endpoints,
    tie broken arbitrarily (matching SQL ORDER BY abs() LIMIT 1).
    """
    if not sorted_ts:
        return None
    i = bisect.bisect_left(sorted_ts, target)
    candidates = []
    if i < len(sorted_ts):
        candidates.append(sorted_ts[i])
    if i > 0:
        candidates.append(sorted_ts[i - 1])
    best = min(candidates, key=lambda ts: abs(ts - target), default=None)
    if best is None or abs(best - target) > window_sec:
        return None
    return obs_by_ts[best]


def run(conn_in: sqlite3.Connection, conn_out: sqlite3.Connection) -> dict:
    """Score all forecasts whose valid_at has passed and have not yet been scored.

    Returns {"scored": N, "skipped": M} where skipped means no Tempest obs was
    found within the matching window, or the relevant obs column was NULL.
    """
    now = int(time.time())
    unscored = conn_out.execute(
        """
        select id, variable, value, valid_at
        from forecasts
        where valid_at < ? and scored_at is null
        """,
        (now,),
    ).fetchall()

    scorable = [r for r in unscored if r["value"] is not None]
    skipped = len(unscored) - len(scorable)

    sorted_ts: list = []
    obs_by_ts: dict = {}
    if scorable:
        earliest = min(r["valid_at"] for r in scorable)
        latest = max(r["valid_at"] for r in scorable)
        raw = db.tempest_obs_range_for_scoring(conn_in, earliest, latest)
        sorted_ts, obs_by_ts = _build_obs_index(raw)

    scored_rows = []
    for row in scorable:
        obs = _find_nearest_obs(sorted_ts, obs_by_ts, row["valid_at"])
        if obs is None:
            skipped += 1
            continue
        col = _OBS_COLUMN.get(row["variable"])
        if col is None:
            skipped += 1
            continue
        observed = obs[col]
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
