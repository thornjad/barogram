# history_fill: gives a member scored history at lead hours it never forecast.
#
# The scheduled runs have always forecast leads 6, 12, 18 and 24; the hours between were
# added later, so a cell like +10 h has months less history than +12 h. Confidence reads a
# member's past errors near matched moments, so a short history leaves those cells with
# almost no evidence. This rebuilds the missing history. For every run that has a scored
# forecast at both neighbouring anchors (0, 6, 12, 18, 24 hours), the in-between forecast
# is the straight line between them, scored against the observation at its own target
# time like any real forecast. Lead 0 is the observation at issue time, so leads 1 to 5
# lie on the line from "now" to the +6 h forecast.
#
# Only gaps are filled: a run that has a real scored row at an hourly lead keeps it and
# gets nothing synthetic there. Nothing is written back to the database.
#
# Checked on 25 days of real hourly rows: the line reproduces the real error within about
# 0.1 degrees from +5 h up, and runs too optimistic at +1 to +3 h for a model whose real
# short-lead shape is not linear (diurnal_curve dewpoint at +1 h: 0.48 line, 0.89 real).

import numpy as np

import db
from models._confidence import CellHistory

_OBS_WINDOW_SEC = 1800  # same match window score.py uses
_STEP = 6               # anchor spacing, hours
_MAX_LEAD = 24
_COLUMNS = {"temperature": 1, "dewpoint": 2, "pressure": 3}  # index in db.tempest_obs_since rows


class ObsIndex:
    """Tempest observations as arrays, for nearest-in-time lookup. Build once per run and
    share it between every member."""

    def __init__(self, rows):
        arr = np.array([[np.nan if v is None else v for v in r] for r in rows], dtype=float)
        if arr.size == 0:
            arr = np.empty((0, 4))
        self._ts = arr[:, 0]
        self._cols = arr

    @classmethod
    def load(cls, conn_in, since: int) -> "ObsIndex":
        return cls(db.tempest_obs_since(conn_in, since))

    def at(self, times, variable: str) -> np.ndarray:
        """The variable's observation nearest each time, NaN where none lies within the
        window or the nearest observation has no value, the way score.py matches."""
        times = np.asarray(times, dtype=float)
        out = np.full(times.shape, np.nan)
        n = self._ts.size
        if n == 0 or times.size == 0:
            return out
        i = np.searchsorted(self._ts, times)
        lo = np.clip(i - 1, 0, n - 1)
        hi = np.clip(i, 0, n - 1)
        pick = np.where(np.abs(self._ts[lo] - times) <= np.abs(self._ts[hi] - times), lo, hi)
        near = np.abs(self._ts[pick] - times) <= _OBS_WINDOW_SEC
        out[near] = self._cols[pick[near], _COLUMNS[variable]]
        return out


def filled_history(conn_out, model_id: int, member_id: int, since: int, obs: ObsIndex) -> CellHistory:
    """The member's scored history (db.model_history_rows) with synthetic rows added at the
    lead hours between its anchors."""
    rows = db.model_history_rows(conn_out, model_id, member_id, since)
    hist = CellHistory(rows)
    by_variable: dict[str, list] = {}
    for r in rows:
        if r["variable"] in _COLUMNS:
            by_variable.setdefault(r["variable"], []).append(r)
    for variable, var_rows in by_variable.items():
        _fill_variable(hist, variable, var_rows, obs)
    return hist


def _fill_variable(hist: CellHistory, variable: str, rows: list, obs: ObsIndex) -> None:
    nan = float("nan")
    issued = np.array([r["issued_at"] for r in rows], dtype=np.int64)
    lead = np.array([r["lead_hours"] for r in rows], dtype=int)
    value = np.array([nan if r["value"] is None else r["value"] for r in rows], dtype=float)
    error = np.array([nan if r["error"] is None else r["error"] for r in rows], dtype=float)

    # prune nulls the stored value but keeps the error, and error = value - observed
    pruned = np.isnan(value)
    if pruned.any():
        value[pruned] = error[pruned] + obs.at(issued[pruned] + lead[pruned] * 3600, variable)

    runs, run_idx = np.unique(issued, return_inverse=True)
    in_range = (lead >= 0) & (lead <= _MAX_LEAD)
    has_row = np.zeros((runs.size, _MAX_LEAD + 1), dtype=bool)
    has_row[run_idx[in_range], lead[in_range]] = True

    anchors = np.full((runs.size, _MAX_LEAD + 1), nan)
    is_anchor = in_range & (lead % _STEP == 0)
    anchors[run_idx[is_anchor], lead[is_anchor]] = value[is_anchor]
    anchors[:, 0] = obs.at(runs, variable)

    for target in range(1, _MAX_LEAD):
        if target % _STEP == 0:
            continue
        below = target // _STEP * _STEP
        above = below + _STEP
        ok = ~has_row[:, target] & ~np.isnan(anchors[:, below]) & ~np.isnan(anchors[:, above])
        if not ok.any():
            continue
        forecast = anchors[ok, below] + (anchors[ok, above] - anchors[ok, below]) * (target - below) / _STEP
        observed = obs.at(runs[ok] + target * 3600, variable)
        scored = ~np.isnan(observed)
        if scored.any():
            hist.add_filled(variable, target, runs[ok][scored], np.abs(forecast - observed)[scored])
