import bisect
import sqlite3
import time
from datetime import datetime

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


def _find_surrounding_obs(
    sorted_ts: list, obs_by_ts: dict, target: int, window_sec: int = 5400
) -> tuple:
    """Return (pre_obs, post_obs) — obs at or before AND at or after target within window.

    pre_obs is the nearest obs with timestamp <= target.
    post_obs is the nearest obs with timestamp >= target.
    Either may be None if no obs exists in that half-window.
    """
    i = bisect.bisect_right(sorted_ts, target)
    pre = None
    post = None
    if i > 0:
        pre_ts = sorted_ts[i - 1]
        if abs(pre_ts - target) <= window_sec:
            pre = obs_by_ts[pre_ts]
    if i < len(sorted_ts):
        post_ts = sorted_ts[i]
        if abs(post_ts - target) <= window_sec:
            post = obs_by_ts[post_ts]
    return pre, post


def _precip_occurred(pre_obs, post_obs) -> float | None:
    """Return 1.0 if measurable precip accumulated between two obs, 0.0 if not, None if unknown."""
    if pre_obs is None or post_obs is None:
        return None
    pre_p = pre_obs["precip_accum_day"]
    post_p = post_obs["precip_accum_day"]
    if pre_p is None or post_p is None:
        return None
    pre_date = datetime.fromtimestamp(pre_obs["timestamp"]).date()
    post_date = datetime.fromtimestamp(post_obs["timestamp"]).date()
    if pre_date != post_date:
        return None  # midnight crossing — can't reliably compute delta
    return 1.0 if max(0.0, post_p - pre_p) > 0.1 else 0.0


def _precip_in_window(
    sorted_ts: list, obs_by_ts: dict, target: int, half_window_sec: int = 10800
) -> float | None:
    """Return 1.0 if >= 0.1mm fell in [target-half_window, target+half_window], 0.0 if not.

    Sums incremental deltas across all consecutive obs pairs in the window.
    When the calendar date changes between a pair, the post-reset value is
    treated as fresh accumulation since midnight.
    Returns None when fewer than 2 obs exist in the window.
    """
    lo = target - half_window_sec
    hi = target + half_window_sec
    i_lo = bisect.bisect_left(sorted_ts, lo)
    i_hi = bisect.bisect_right(sorted_ts, hi)
    window_ts = sorted_ts[i_lo:i_hi]
    if len(window_ts) < 2:
        return None
    total = 0.0
    for i in range(1, len(window_ts)):
        prev = obs_by_ts[window_ts[i - 1]]
        curr = obs_by_ts[window_ts[i]]
        p_prev = prev["precip_accum_day"]
        p_curr = curr["precip_accum_day"]
        if p_prev is None or p_curr is None:
            continue
        prev_date = datetime.fromtimestamp(prev["timestamp"]).date()
        curr_date = datetime.fromtimestamp(curr["timestamp"]).date()
        if curr_date != prev_date:
            total += max(0.0, p_curr)
        else:
            total += max(0.0, p_curr - p_prev)
    return 1.0 if total > 0.1 else 0.0


def _find_nearest_obs(
    sorted_ts: list, obs_by_ts: dict, target: int, window_sec: int = 1800
) -> dict | None:
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
        raw = db.tempest_obs_range_for_scoring(conn_in, earliest, latest, window_sec=10800)
        sorted_ts, obs_by_ts = _build_obs_index(raw)

    scored_rows = []
    for row in scorable:
        if row["variable"] == "precip_prob":
            observed = _precip_in_window(sorted_ts, obs_by_ts, row["valid_at"])
            if observed is None:
                skipped += 1
                continue
        else:
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
        mae_val = error ** 2 if row["variable"] == "precip_prob" else abs(error)
        scored_rows.append({
            "id": row["id"],
            "observed": observed,
            "error": error,
            "mae": mae_val,
            "scored_at": now,
        })

    if scored_rows:
        db.update_scored_forecasts(conn_out, scored_rows)

    return {"scored": len(scored_rows), "skipped": skipped}
