# models/_confidence.py

import datetime
import math
from collections import defaultdict

import db
import models._similarity as _similarity

_CONFIDENCE_WINDOW_DAYS = 400    # independent of tune's own lookback window
_CONFIDENCE_PSEUDOCOUNT = 8      # k in the n/(n+k) trust multiplier: trust reaches 0.5
                                 # once n matched-and-scored days exist, and keeps
                                 # climbing (never hard-caps) as n grows further
_MIN_HISTORY_DAYS = 5            # a cell's history must span MORE than this many distinct days
_MATCH_DISTANCE_THRESHOLD = 8.0  # analog candidates farther than this (in sigma-normalized
                                 # distance units, see _similarity.distance) aren't a real
                                 # match at all -- conditions genuinely unlike anything
                                 # recorded should be able to return zero matches
_MATCH_MAX_CANDIDATES = 50       # of the candidates within threshold, keep only the closest
                                 # this many -- bounds compute/noise as more days accumulate
                                 # without acting as a similarity requirement itself
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
    "relative_humidity", "battery", "lightning_avg_distance",
    "lightning_strike_last_distance", "nc_rain",
]  # every raw column find_default_matches fetches for a snapshot match.
   # battery/lightning_avg_distance/lightning_strike_last_distance/nc_rain
   # were added 2026-09-18 (wxlog migration 004) -- excluded from this list:
   # heat_index, wind_chill, delta_t (formulas over columns already here,
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
    "station_pressure": 1.2,
    "wind_avg": 0.6,
    "wind_direction": 0.6,
    "wind_gust": 0.4,
    "solar_radiation": 0.5,
    "uv_index": 0.3,
    "precip_accum_day": 0.5,
    "lightning_count": 0.3,
    "precip": 0.4,
    "wind_lull": 0.3,
    "relative_humidity": 0.8,
    "battery": 0.05,
    "lightning_avg_distance": 0.3,
    "lightning_strike_last_distance": 0.3,
    "nc_rain": 0.4,
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
                           # get a flat discount off their snapshot
                           # counterpart's weight rather than their own
                           # separately hand-picked number
TREND_FEATURE_WEIGHTS = {f"{col}_trend": w * _TREND_WEIGHT_SCALE for col, w in FEATURE_WEIGHTS.items()}
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
# days that looked like today. blended_confidence compares matched-day error
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


def matched_day_spreads(conn_in, matched_ts: list[int]) -> dict[tuple[str, int], float]:
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
    for ts in matched_ts:
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


def confidence_for_cell(history: list[dict], variable: str, lead_hours: int,
                         matched_ts: list[int]) -> float:
    """
    history is every scored row for one (model_id, member_id) pair (dicts
    with variable/lead_hours/issued_at/mae; see db.model_error_history).

    Filters history to this (variable, lead_hours) cell. Returns 0.0 if the
    cell has no scored history at all, or if its history spans
    _MIN_HISTORY_DAYS distinct calendar days OR FEWER -- no baseline yet
    means no basis to claim anything but zero confidence. Every forecast
    row gets a real confidence number; this function never returns None.

    Otherwise, the yardstick is the natural day-to-day spread of what
    reality actually did, lead_hours out, on days that looked like today
    (see set_spread) -- never this member's own historical average, which
    would make "confident" mean "better than my usual mess" instead of
    "actually good," and never a specific reference model's error either,
    which would tie confidence to that model's own lucky or unlucky
    stretches. A cell with no known spread yet gets the same 0.0 treatment
    as too-thin history, via blended_confidence's own guard. Each timestamp in matched_ts
    is a matched day's own nearest-clock-time analog snapshot (see
    find_default_matches); only
    this model's scored runs within _MATCH_HOUR_TOLERANCE_SEC of that
    specific timestamp count as that match's evidence -- not every run
    from that whole calendar day, which would dilute the pool with runs
    from unrelated hours. Each matched calendar day is counted at most
    once even if matched_ts repeats a day.

    matched_ts may be empty (no analog day was close enough to count as a
    real match) or nonempty but yield zero usable evidence (matched days
    exist but this model has no scored run near their clock time) -- both
    cases mean the same thing to blended_confidence: no evidence to judge
    confidence from.

    IMPORTANT for every caller: this function's result is specific to one
    (variable, lead_hours) cell. Any caller storing results across
    multiple cells must key that storage by (variable, lead_hours)
    together, never by variable alone.
    """
    cell_rows = [r for r in history if r["variable"] == variable and r["lead_hours"] == lead_hours]
    if not cell_rows:
        return 0.0
    distinct_days = len({r["issued_at"] // 86400 for r in cell_rows})
    if distinct_days <= _MIN_HISTORY_DAYS:
        return 0.0
    spread = _spread.get((variable, lead_hours))
    seen_days: set[int] = set()
    matched_errors: list[float] = []
    for ts in matched_ts:
        day = ts // 86400
        if day in seen_days:
            continue
        seen_days.add(day)
        matched_errors.extend(
            r["mae"] for r in cell_rows
            if abs(r["issued_at"] - ts) <= _MATCH_HOUR_TOLERANCE_SEC
        )
    return blended_confidence(matched_errors, spread, _CONFIDENCE_PSEUDOCOUNT)


def blended_confidence(matched_errors: list[float], spread: float | None,
                        k: int) -> float:
    """
    Returns 0.0 when spread is None or <= 0 -- no natural-variability
    yardstick to measure against at all (too few matched days had an
    observation near the target time to compute one; see
    matched_day_spreads). No basis to claim anything but zero confidence;
    this function never returns None. Otherwise:

        matched_avg = sum(matched_errors) / len(matched_errors)
        z = matched_avg / spread
        raw = exp(-2 * z * z)
        trust = len(matched_errors) / (len(matched_errors) + k)
        return trust * raw

    z compares this member's typical error on days like today against how
    much reality itself naturally varies on those same days: z near 0 means
    the error is small next to normal day-to-day wobble, z past ~1-2 means
    the error is large compared to what genuinely different weather looks
    like. `raw`'s curve -- steep, squared-exponential decay -- was chosen
    over gentler alternatives (a two-sided normal survival function, a
    logistic) by backtesting real ensemble output on real scored history,
    not picked a priori: see the 2026-09-23 confidence message-board
    thread. Steep won decisively at the real-ensemble level (37.8% MAE
    reduction over the prior reference-scale/1-over-(1+x) design) with
    overwhelming statistical significance (p well under 1e-30 on two
    independently tested models), and its advantage over the gentler
    curves grew with model/member count rather than shrinking. `trust` is
    a multiplier on that claim, not a blend toward it -- it scales
    confidence DOWN toward zero as evidence thins, rather than blending it
    toward a neutral 0.5 guess. Zero matched evidence (trust=0,
    matched_errors empty) returns exactly 0.0: no analog day was ever a
    close enough match, or matched days exist but this member has no
    scored run near their clock time -- either way, there is no basis to
    claim any confidence at all, not a coin-flip default. As n grows,
    trust climbs toward 1.0 (no hard ceiling) and the result converges on
    `raw`. A cell with fewer samples than k reports LESS than half of what
    raw alone would justify, regardless of which direction raw points --
    thin evidence deserves a muted claim in either direction, not a
    neutral one. Result is always in [0, 1): 0.0 when there's no yardstick
    or no matched evidence, otherwise strictly positive and never reaching
    1.0 for finite n.
    """
    if spread is None or spread <= 0:
        return 0.0
    if not matched_errors:
        return 0.0
    matched_avg = sum(matched_errors) / len(matched_errors)
    z = matched_avg / spread
    raw = math.exp(-2.0 * z * z)
    trust = len(matched_errors) / (len(matched_errors) + k)
    return trust * raw


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


def find_default_matches(conn_in, current_ts: int) -> list[int]:
    """The shared fingerprint search used by every model. Computed once
    per forecast run by cmd_forecast. Returns [] on a fresh database, or
    when nothing recorded so far is even within _MATCH_DISTANCE_THRESHOLD
    of current conditions -- a real possibility, not just an edge case:
    the first genuinely unprecedented cold snap or storm this station has
    seen should return no matches at all, not the 20 nearest regardless of
    how dissimilar they actually are. Of whatever clears the threshold,
    keeps only the closest _MATCH_MAX_CANDIDATES."""
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
    cands_with_dist = [(_similarity.distance(current, c, features, sigmas, weights), c)
                        for c in candidates]
    within_threshold = [(d, c) for d, c in cands_with_dist
                         if d is not None and d <= _MATCH_DISTANCE_THRESHOLD]
    nearest = _similarity.select_k_nearest(within_threshold, _MATCH_MAX_CANDIDATES)
    return [c["timestamp"] for _, c in nearest]


def member_confidences(member_history: dict[int, list[dict]] | None,
                        default_matches: list[int] | None,
                        member_ids: list[int], variable: str, lead_hours: int,
                        matched_ts_by_mid: dict[int, list[int]] | None = None) -> dict[int, float]:
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
    # of evidence, same "nothing to base trust on" case blended_confidence's
    # own empty-matches branch returns 0.0 for, not a neutral 0.5 guess
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
