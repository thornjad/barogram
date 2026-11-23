# helpers for signature members: specialists that abstain unless a rare pattern shows in the
# Tempest data, and then state confidence from their own matches and their own spread
# (see docs/confidence.md "Own-evidence models"). Tempest columns only, no outside data.

import datetime as dt
import math
from collections import defaultdict

import numpy as np


class SolarEnvelope:
    """Per-local-hour solar_radiation ceiling from the trailing `days` days, so a grey day reads
    as a low ratio whatever the season. Built from obs rows (timestamp, solar_radiation) and
    never looks at or after the timestamp asked about, so a backtest cannot leak future days."""

    def __init__(self, obs_rows, days: int = 14):
        self.days = days
        self._by_day: dict[int, dict[int, float]] = defaultdict(dict)
        for r in obs_rows:
            sr = r["solar_radiation"]
            if sr is None:
                continue
            d = dt.datetime.fromtimestamp(r["timestamp"])
            day = d.toordinal()
            cell = self._by_day[day]
            if sr > cell.get(d.hour, -1.0):
                cell[d.hour] = sr
        self._cache: dict[tuple[int, int], float | None] = {}

    def ceiling(self, ts: int) -> float | None:
        """The highest reading at ts's local hour over the `days` days before ts's own day."""
        d = dt.datetime.fromtimestamp(ts)
        key = (d.toordinal(), d.hour)
        if key in self._cache:
            return self._cache[key]
        vals = [self._by_day[day][d.hour]
                for day in range(key[0] - self.days, key[0])
                if d.hour in self._by_day.get(day, {})]
        result = max(vals) if len(vals) >= 5 else None
        self._cache[key] = result
        return result

    def ratio(self, ts: int, solar: float | None) -> float | None:
        """solar / ceiling, or None at night, with no reading, or without enough days behind it."""
        ceil = self.ceiling(ts)
        if solar is None or ceil is None or ceil < 50:
            return None
        return solar / ceil


def kernel_matches(current: dict, candidates: list[dict], scales: dict[str, float],
                   width: float = 1.0, floor: float = 0.05) -> list[tuple[int, float]]:
    """(timestamp, similarity) pairs on a member's own small feature set. `scales` maps each
    feature to the difference that counts as one unit of distance. Similarity is
    exp(-(d / width)^2) with d the root mean square of the scaled differences, the same shape
    the shared matcher uses, and anything under `floor` is dropped. A candidate missing a
    feature is skipped. Candidates must already be strictly earlier than the current moment."""
    out = []
    for c in candidates:
        total = 0.0
        ok = True
        for f, s in scales.items():
            a, b = current.get(f), c.get(f)
            if a is None or b is None:
                ok = False
                break
            total += ((a - b) / s) ** 2
        if not ok:
            continue
        sim = math.exp(-(math.sqrt(total / len(scales)) / width) ** 2)
        if sim >= floor:
            out.append((c["timestamp"], sim))
    return out


def spearman(x, y) -> float | None:
    """Rank correlation, None when either side is constant or there are under 3 pairs."""
    x, y = np.asarray(x, dtype=float), np.asarray(y, dtype=float)
    if x.size < 3:
        return None
    rx, ry = _ranks(x), _ranks(y)
    if rx.std() == 0 or ry.std() == 0:
        return None
    return float(np.corrcoef(rx, ry)[0, 1])


def _ranks(v):
    """Ranks with ties averaged."""
    order = np.argsort(v, kind="stable")
    ranks = np.empty(len(v))
    ranks[order] = np.arange(len(v), dtype=float)
    for val in np.unique(v):
        mask = v == val
        if mask.sum() > 1:
            ranks[mask] = ranks[mask].mean()
    return ranks
