# models/_confidence.py

import datetime
import math
from collections import defaultdict

import numpy as np

import db
import models._similarity as _similarity

_CONFIDENCE_WINDOW_DAYS = 400    # independent of tune's own lookback window
_MIN_HISTORY_DAYS = 5            # a cell's history must span MORE than this many distinct days

# --- matching and evidence: every value in this block is experimental and expected to change ---
_MATCH_POOL = 200                # the closest this many past moments are kept as candidate matches,
                                 # with no similarity gate at this stage. A compute bound, not a
                                 # quality filter: quality is each match's similarity, below.
_SIM_SCALE = 0.6                 # default similarity is exp(-(distance / _SIM_SCALE)^2) ...
_SIM_MAX_DISTANCE = 1.1          # ... and exactly 0 beyond this distance. Distance is the root mean
                                 # weighted squared z of the fingerprint (divided by the root of the
                                 # total weight), so editing a feature weight never silently moves
                                 # these two. 0.6 and 1.1 equal the old 2.5 and 4.5 at the old weights.
_CRITICAL_LIMITS = {             # ... and exactly 0 when any of these differs from now by more
    "air_temp": 5.0,             # (absolute, in the column's own unit), however close the rest
    "dew_point": 6.0,            # of the fingerprint is: a match this far off on a variable that
    "station_pressure": 8.0,     # defines the airmass says nothing about today
}
_EVIDENCE_FULL_AT = 150          # evidence curve e(n) = n^p / (n^p + K), reaching 99% at this n
_EVIDENCE_POWER = 3
_EVIDENCE_BASE = 0.02            # a small separate term so one match is worth about 1%, not 0.003%:
                                 # base(n) = _EVIDENCE_BASE * n / (n + 1), zero at n = 0
_EVIDENCE_TOP = 0.99
_base_at_full = _EVIDENCE_BASE * _EVIDENCE_FULL_AT / (_EVIDENCE_FULL_AT + 1)
_cubic_at_full = 1 - (1 - _EVIDENCE_TOP) / (1 - _base_at_full)
_EVIDENCE_K = _EVIDENCE_FULL_AT ** _EVIDENCE_POWER * (1 - _cubic_at_full) / _cubic_at_full
# K is solved so the combined curve still hits exactly _EVIDENCE_TOP at _EVIDENCE_FULL_AT
_CONFIDENCE_FLOOR = 0.001        # 0.1%, not 10% -- a member reporting genuine 0%
                                 # confidence still keeps a sliver of ensemble influence
                                 # so it's never fully excluded, but a member that says
                                 # "I don't know what I'm doing" should count for almost
                                 # nothing, not a full tenth of a confident member's weight
_TREND_WINDOW_SEC = 3 * 3600     # how far back each trend delta looks
_MATCH_HOUR_TOLERANCE_SEC = 90 * 60  # a matched day's own nearest-clock-time snapshot
                                 # only pulls in this model's scored runs within this
                                 # window of it, not every run from that whole calendar
                                 # day -- runs are spaced roughly 3h apart in practice,
                                 # so this grabs the one relevant run without bleeding
                                 # into neighboring run slots

_DEFAULT_FEATURES = [
    "air_temp", "dew_point", "station_pressure", "wind_avg",
    "wind_direction", "wind_gust", "solar_radiation", "uv_index",
    "precip_accum_day", "lightning_count", "precip", "wind_lull",
    "battery", "lightning_avg_distance",
    "lightning_strike_last_distance", "nc_rain",
]  # every raw column find_default_matches fetches for a snapshot match.
   # battery/lightning_avg_distance/lightning_strike_last_distance/nc_rain
   # were added 2026-09-18 (wxlog migration 004) -- excluded from this list:
   # relative_humidity (a function of air_temp and dew_point, so it double
   # counted them; dropped from the fingerprint 2026-11-11), heat_index, wind_chill, delta_t (formulas over columns already here,
   # same double-counting reasoning that already excluded feels_like/
   # wet_bulb/air_density) and precip_type (categorical, not a continuous
   # quantity norm_sigmas/distance can z-score). All four are brand new
   # columns with no history before today -- norm_sigmas already handles
   # that gracefully (sigma is None, feature dropped) until enough
   # candidates accumulate real values.

_TREND_FEATURES = [f"{col}_trend" for col in _DEFAULT_FEATURES]
# one trend delta per snapshot column, computed by compute_trends and
# added alongside _DEFAULT_FEATURES so analog matching sees the last
# _TREND_WINDOW_SEC of change, not just an instantaneous reading -- two
# moments with the same pressure but opposite trajectories (falling vs
# steady) otherwise match as identical.

FEATURE_WEIGHTS = {
    "air_temp": 1.5,
    "dew_point": 1.5,
    "station_pressure": 1.0,
    "wind_avg": 0.5,
    "wind_direction": 0.6,
    "wind_gust": 0.2,
    "solar_radiation": 0.5,
    "uv_index": 0.1,
    "precip_accum_day": 0.5,
    "lightning_count": 0.1,
    "precip": 0.4,
    "wind_lull": 0.1,
    "battery": 0.02,
    "lightning_avg_distance": 0.1,
    "lightning_strike_last_distance": 0.1,
    "nc_rain": 0.2,
}
# first-guess per-feature weight for find_default_matches' distance calc, one
# entry per _DEFAULT_FEATURES column. Temp/dewpoint/pressure dominate because
# they define airmass and synoptic pattern -- the thing "similar weather"
# actually means. battery is sensor health, not weather, so it's barely above
# zero rather than dropped (every feature still counts, per the decision to
# keep the full feature set). This is a real starting point, not a
# placeholder -- models/similarity_tune.py's backtest is what earns a second,
# better-informed guess later, the same way `tune` improves ensemble weights
# from an initial equal-weight start.

_TREND_WEIGHT_SCALE = 0.7  # trend deltas matter (see the 2026-09-23 confidence
                           # message-board discussion on lightning trends), but
                           # a snapshot match is the primary signal -- trends
                           # get a discount off their snapshot counterpart's
                           # weight rather than their own hand-picked number
_TREND_SCALE_OVERRIDES = {  # meteorological tiers (2026-11-11): the 3h pressure change is the
    "station_pressure": 1.0,  # synoptic signal, so it counts as much as the level; temperature
    "air_temp": 0.8,          # trend is advection and cloud once time of day is matched; dewpoint
    "dew_point": 0.6,         # trend has a tight sigma, so it already pulls hard per unit weight
}
TREND_FEATURE_WEIGHTS = {f"{col}_trend": w * _TREND_SCALE_OVERRIDES.get(col, _TREND_WEIGHT_SCALE)
                         for col, w in FEATURE_WEIGHTS.items()}
ALL_FEATURE_WEIGHTS = {**FEATURE_WEIGHTS, **TREND_FEATURE_WEIGHTS}

_VARIABLE_COLUMN = {
    "temperature": "air_temp",
    "dewpoint": "dew_point",
    "pressure": "station_pressure",
}

_spread: dict[tuple[str, int], float] = {}
# populated once per forecast run by set_spread (cmd_forecast, right after
# computing shared_default_matches) from matched_day_spreads -- the natural
# day-to-day variability of what reality actually did, lead_hours later, on
# days that looked like today. confidence_for_cell compares matched-day error
# against this instead of a fixed reference-model baseline (the earlier
# 2026-09-23 design): anchoring to physical reality's own variability, not a
# specific reference model's error, means a lucky or unlucky stretch for
# that reference model can't distort the reading (see the confidence
# message-board thread for the full reasoning).


def set_spread(spread: dict[tuple[str, int], float]) -> None:
    """Call once per forecast run before computing any confidence. A
    (variable, lead_hours) cell missing from `spread` (fewer than 2 matched
    days had an observation near the target time) means no yardstick is
    known yet for that cell -- confidence_for_cell treats that the same as
    too-thin own history: zero confidence, not a guess."""
    global _spread
    _spread = spread


def matched_day_spreads(conn_in, matched_ts: list) -> dict[tuple[str, int], float]:
    """Population stdev of what the actual value looked like, lead_hours
    out, on each matched day -- one number per (variable, lead_hours),
    shared by every model that uses the same matched_ts (the shared
    analog-day set), since "how much does the value vary at that point,
    on days like today" doesn't depend on which model is asking.

    Deliberately the spread of the raw value at the target time, not the
    spread of the change from now to then (an earlier design used the
    latter). A change-based spread shrinks mechanically at short lead
    hours -- barely any time for two readings to drift apart -- and grows
    at long lead purely because more time has passed, independent of
    whether models actually get less accurate that far out. That mismatch
    made every model look artificially unconfident at short leads and
    artificially confident at long ones. Real data confirmed it: change
    spread roughly tripled from 1h to 6h+ while real historical error
    barely moved. Raw-value spread doesn't have this problem -- it reflects
    how variable the atmosphere itself is at that time/season, which
    doesn't inflate just because a longer lead was picked (see the
    2026-09-24 confidence message-board thread).

    A matched day missing an observation near the target time (too close
    to "now" to have a real outcome yet, or a data gap) is skipped for that
    cell rather than raising. A cell needs at least 2 matched days with a
    usable reading to report a stdev at all -- one sample has no spread to
    speak of.
    """
    values: dict[tuple[str, int], list[float]] = defaultdict(list)
    for ts, _ in as_pairs(matched_ts):
        for lead_hours in range(1, 25):
            after = db.nearest_tempest_obs(conn_in, ts + lead_hours * 3600, window_sec=1800)
            if after is None:
                continue
            for variable, col in _VARIABLE_COLUMN.items():
                v = after[col]
                if v is not None:
                    values[(variable, lead_hours)].append(v)
    spread = {}
    for key, vals in values.items():
        if len(vals) < 2:
            continue
        mean = sum(vals) / len(vals)
        variance = sum((v - mean) ** 2 for v in vals) / len(vals)
        spread[key] = variance ** 0.5
    return spread


class CellHistory:
    """A member's scored history grouped by (variable, lead_hours) cell, so
    confidence_for_cell reads a cell's rows directly instead of filtering the
    member's whole history once per cell (72 cells x every row, per member).

    Build one per member per run and pass it wherever a history list goes; a
    plain list is wrapped on the fly with the same result, only slower."""

    def __init__(self, rows):
        groups: dict[tuple[str, int], tuple[list, list]] = {}
        for r in rows:
            g = groups.get((r["variable"], r["lead_hours"]))
            if g is None:
                g = groups[(r["variable"], r["lead_hours"])] = ([], [])
            g[0].append(r["issued_at"])
            g[1].append(r["mae"])
        self._groups = groups
        self._arrays: dict[tuple[str, int], tuple] = {}

    def cell(self, variable: str, lead_hours: int):
        """(issued_at array, mae array, distinct calendar days) for the cell, rows
        in original history order, or None when the cell has no rows."""
        key = (variable, lead_hours)
        arrays = self._arrays.get(key)
        if arrays is None:
            g = self._groups.get(key)
            if g is None:
                return None
            issued = np.array(g[0])
            arrays = self._arrays[key] = (issued, np.array(g[1], dtype=float),
                                          np.unique(issued // 86400).size)
        return arrays


def as_pairs(matches) -> list[tuple[int, float]]:
    """Matches as (timestamp, similarity) pairs. A bare timestamp, which is what analog and
    full_state_analog hand in for the days their own forecasts selected, counts as similarity 1."""
    return [(m, 1.0) if isinstance(m, (int, np.integer)) else (m[0], m[1]) for m in (matches or [])]


def evidence(n_eff: float) -> float:
    """How much a similarity-weighted match count is worth: 0 at nothing, tiny at one match,
    99% at _EVIDENCE_FULL_AT matches, never 1.0."""
    if n_eff <= 0:
        return 0.0
    p = n_eff ** _EVIDENCE_POWER
    cubic = p / (p + _EVIDENCE_K)
    base = _EVIDENCE_BASE * n_eff / (n_eff + 1.0)
    return 1.0 - (1.0 - cubic) * (1.0 - base)


def confidence_for_cell(history: "list[dict] | CellHistory", variable: str, lead_hours: int,
                         matches: list) -> float:
    """
    history is every scored row for one (model_id, member_id) pair (dicts
    with variable/lead_hours/issued_at/mae; see db.model_error_history).
    matches is a list of past moments the member treats as similar to now: a bare
    timestamp (similarity 1) or a (timestamp, similarity) pair with similarity in
    [0, 1]. The shared default list comes from find_default_matches; a model with its
    own way of choosing and weighing matches passes its own list (bogo picks at random).

    Confidence is the member's claim that it has plenty of close matches to today
    and did well on them:

        confidence = q * e(n_eff)

    For each match that has any scored run of this member near it (within
    _MATCH_HOUR_TOLERANCE_SEC, averaged into one piece of evidence so back-to-back runs
    never count twice) the performance is exp(-2 z^2), z being its error over the
    spread, the natural variability of the value itself on the matched days (see
    set_spread), never the member's own average. n_eff is the sum of the similarities of
    those matches and q their similarity-weighted average performance, so q says how
    good the member was and e(n_eff) how much evidence stands behind it. q does not grow
    with more data, e does, so volume alone never makes a mediocre member confident.

    Returns 0.0 for a cell with no scored history, a history spanning
    _MIN_HISTORY_DAYS distinct days or fewer, a cell with no known spread, or no match
    with a scored run near it. Every forecast row gets a real confidence number; this
    function never returns None, and never 1.0.

    IMPORTANT for every caller: this function's result is specific to one
    (variable, lead_hours) cell. Any caller storing results across
    multiple cells must key that storage by (variable, lead_hours)
    together, never by variable alone.
    """
    hist = history if isinstance(history, CellHistory) else CellHistory(history)
    cell = hist.cell(variable, lead_hours)
    if cell is None or cell[2] <= _MIN_HISTORY_DAYS:
        return 0.0
    spread = _spread.get((variable, lead_hours))
    if spread is None or spread <= 0:
        return 0.0
    sims, errors = _moments(cell, as_pairs(matches))
    if errors.size == 0:
        return 0.0
    total_sim = float(sims.sum())
    if total_sim <= 0:
        return 0.0
    z = errors / spread
    performance = np.exp(-2.0 * z * z)
    q = float((sims * performance).sum() / total_sim)
    return q * evidence(total_sim)


def _moments(cell, pairs: list[tuple[int, float]]):
    """(similarities, mean errors) of the matched moments with any scored run near them. Runs
    within the tolerance window of one moment are averaged, never counted twice, and a calendar
    day matched more than once counts once (its first match). Zero-similarity matches drop out."""
    issued, mae, _ = cell
    seen_days: set[int] = set()
    match_ts, match_sim = [], []
    for ts, sim in pairs:
        day = ts // 86400
        if day in seen_days or sim <= 0:
            continue
        seen_days.add(day)
        match_ts.append(ts)
        match_sim.append(sim)
    empty = (np.array([]), np.array([]))
    if not match_ts:
        return empty
    near = np.abs(issued[None, :] - np.array(match_ts)[:, None]) <= _MATCH_HOUR_TOLERANCE_SEC
    runs = near.sum(axis=1)
    has_runs = runs > 0
    if not has_runs.any():
        return empty
    errors = (near * mae[None, :]).sum(axis=1)[has_runs] / runs[has_runs]
    return np.array(match_sim)[has_runs], errors


def compute_trends(now: dict, prior: dict | None,
                    features: list[str] = _DEFAULT_FEATURES) -> dict[str, float | None]:
    """One trend delta per feature column (default _DEFAULT_FEATURES), keyed
    '<col>_trend': now[col] - prior[col], or None if prior is missing or
    either value is None. wind_direction uses a signed veering delta instead
    of a plain difference, since direction wraps at 360. precip_accum_day is
    a since-local-midnight counter, so a plain diff across a midnight
    rollover would read as a large, fake drop in precipitation -- that
    one delta is None whenever now and prior fall on different local
    dates; precip's own trend has no such reset and isn't gated.

    Public: also called by full_state_analog.py to build its own
    trend-augmented candidate pools (trajectory-analog and full
    trend+snapshot members), not just by find_default_matches below."""
    if prior is None:
        return {f"{col}_trend": None for col in features}
    now_date = datetime.datetime.fromtimestamp(now["timestamp"]).date()
    prior_date = datetime.datetime.fromtimestamp(prior["timestamp"]).date()
    trends: dict[str, float | None] = {}
    for col in features:
        now_v = now.get(col)
        prior_v = prior.get(col)
        if now_v is None or prior_v is None:
            trends[f"{col}_trend"] = None
        elif col == "wind_direction":
            trends[f"{col}_trend"] = _similarity.signed_arc_delta(now_v, prior_v)
        elif col == "precip_accum_day" and now_date != prior_date:
            trends[f"{col}_trend"] = None
        else:
            trends[f"{col}_trend"] = now_v - prior_v
    return trends


def find_default_matches(conn_in, current_ts: int) -> list[tuple[int, float]]:
    """The shared fingerprint search used by every model. Computed once per forecast
    run by cmd_forecast. Returns (timestamp, similarity) pairs for the closest
    _MATCH_POOL past moments (one snapshot per past day, nearest to now's clock time),
    dropping any whose similarity is 0. [] on a fresh database or when nothing
    recorded is similar to current conditions, which is a real possibility: the first
    unprecedented storm should return no matches at all.

    Similarity is 1 at distance 0, falls as exp(-(d / _SIM_SCALE)^2), and is exactly 0
    beyond _SIM_MAX_DISTANCE or when temperature, dewpoint or pressure differ from now
    by more than _CRITICAL_LIMITS. A member with its own idea of similarity can ignore
    these similarities and pass its own to confidence_for_cell."""
    current = db.nearest_tempest_obs(conn_in, current_ts, window_sec=1800)
    # no lookback cap -- a fixed window permanently defeats analog matching's
    # purpose once history exceeds it: a once-a-year event's prior occurrence
    # becomes unmatchable forever. Full-history cost is negligible at plausible
    # data volumes (checked to 10yr / ~3650 candidate days on the 8GB machine).
    candidates = db.full_analog_candidates(conn_in, current_ts, lookback_sec=None)
    if current is None or not candidates:
        return []
    current = {**dict(current), "timestamp": current_ts}
    prior_current = db.nearest_tempest_obs(conn_in, current_ts - _TREND_WINDOW_SEC, window_sec=1800)
    current.update(compute_trends(current, prior_current))
    candidates = [dict(c) for c in candidates]
    for c in candidates:
        prior_c = db.nearest_tempest_obs(conn_in, c["timestamp"] - _TREND_WINDOW_SEC, window_sec=1800)
        c.update(compute_trends(c, prior_c))
    features = _DEFAULT_FEATURES + _TREND_FEATURES
    weights = [ALL_FEATURE_WEIGHTS[col] for col in features]
    sigmas = _similarity.norm_sigmas(candidates, features)
    norm = distance_norm(features, weights, sigmas)
    cands_with_dist = [(None if d is None else d / norm, c)
                        for d, c in ((_similarity.distance(current, c, features, sigmas, weights), c)
                                     for c in candidates)]
    nearest = _similarity.select_k_nearest(cands_with_dist, _MATCH_POOL)
    matches = []
    for d, c in nearest:
        sim = default_similarity(d, current, c)
        if sim > 0:
            matches.append((c["timestamp"], sim))
    return matches


def distance_norm(features: list[str], weights: list[float], sigmas: dict) -> float:
    """Root of the total weight of the features that have a usable sigma. Dividing
    _similarity.distance by it makes distance a root mean weighted squared z."""
    total = sum(w for f, w in zip(features, weights) if sigmas.get(f) is not None)
    return math.sqrt(total) if total > 0 else 1.0


def default_similarity(distance: float, current: dict, candidate: dict) -> float:
    if distance > _SIM_MAX_DISTANCE:
        return 0.0
    for col, limit in _CRITICAL_LIMITS.items():
        a, b = current.get(col), candidate.get(col)
        if a is not None and b is not None and abs(a - b) > limit:
            return 0.0
    return math.exp(-((distance / _SIM_SCALE) ** 2))


def member_confidences(member_history: dict[int, list[dict]] | None,
                        default_matches: list | None,
                        member_ids: list[int], variable: str, lead_hours: int,
                        matched_ts_by_mid: dict[int, list] | None = None) -> dict[int, float]:
    """
    Shared per-member confidence computation, called once per (variable,
    lead_hours) cell. member_history/default_matches being None degrades
    every member to 0.0, which every combine_pattern function already
    treats as "no basis to trust this member right now."

    matched_ts_by_mid, when given, overrides default_matches for specific
    member_ids -- only analog.py and full_state_analog.py use this, since
    every other model's members all search the identical shared default
    fingerprint and would otherwise repeat the same override entry for
    every one of their members.
    """
    result = {}
    for mid in member_ids:
        history = (member_history or {}).get(mid, [])
        matched_ts = (matched_ts_by_mid or {}).get(mid, default_matches or [])
        result[mid] = confidence_for_cell(history, variable, lead_hours, matched_ts)
    return result


def _equal_mean_fallback(pairs: list[tuple[int, float]],
                          confidences: dict[int, float | None]) -> tuple[float | None, float | None]:
    """Today's plain equal-average behavior, paired with a plain average
    confidence for reporting. Shared by combine_pattern's own missing-weight
    fallback and by _inject's own zero-influence fallback, so this logic
    can't drift apart across its call sites under later edits."""
    vals = [v for _, v in pairs]
    confs = [c for mid, _ in pairs if (c := confidences.get(mid)) is not None]
    return (sum(vals) / len(vals) if vals else None, average_confidence(confs))


def _inject(pairs: list[tuple[int, float]], member_weights: dict[int, float],
            confidences: dict[int, float | None]) -> tuple[float | None, float | None]:
    """
    Shared core for combine_pattern, given the members that already have a
    real weight. pairs: [(member_id, value), ...]. Not called directly by
    models.
    """
    if not pairs:
        return None, None
    known = [c for mid, _ in pairs if (c := confidences.get(mid)) is not None]
    # no member in the group reports any confidence at all -- total absence
    # of evidence, same "nothing to base trust on" case confidence_for_cell's
    # own no-evidence branch returns 0.0 for, not a neutral 0.5 guess
    group_avg = sum(known) / len(known) if known else 0.0
    infl = []
    for mid, v in pairs:
        w = member_weights[mid]
        c = confidences.get(mid)
        eff = max(c if c is not None else group_avg, _CONFIDENCE_FLOOR)
        infl.append((w * eff, v, c))
    total_infl = sum(i for i, _, _ in infl)
    if total_infl <= 0:
        return _equal_mean_fallback(pairs, confidences)
    mean = sum(i * v for i, v, _ in infl) / total_infl
    raw_confs = [(i, c) for i, _, c in infl if c is not None]
    conf_den = sum(i for i, _ in raw_confs)
    group_confidence = (sum(i * c for i, c in raw_confs) / conf_den if conf_den > 0
                         else average_confidence([c for _, c in raw_confs])) if raw_confs else None
    return mean, group_confidence


def combine_pattern(valid_pairs: list[tuple[int, float]], member_weights: dict[int, float | None],
                     confidences: dict[int, float | None]) -> tuple[float | None, float | None]:
    """
    The one combination shape every weighted model in this plan uses:
    drop only the members missing a weight, keep a weighted (and now
    confidence-adjusted) mean of the rest. member_weights: {member_id:
    weight or None}, built by the caller against its own weights dict and
    key shape. If every member in valid_pairs lacks a weight, falls back
    to a plain equal average (paired with a plain average confidence).
    """
    weighted_pairs = [(mid, v) for mid, v in valid_pairs if member_weights.get(mid) is not None]
    if not weighted_pairs:
        return _equal_mean_fallback(valid_pairs, confidences)
    return _inject(weighted_pairs, member_weights, confidences)


def average_confidence(confidences: list[float | None]) -> float | None:
    """Plain, unweighted average of the non-None values, or None if every
    value is None."""
    vals = [c for c in confidences if c is not None]
    return sum(vals) / len(vals) if vals else None
