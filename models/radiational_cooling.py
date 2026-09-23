# radiational_cooling: the textbook clear-plus-calm-plus-dry setup for strong
# overnight cooling, built as one explicit small model instead of left implicit
# and scattered across several others (airmass_diurnal's clearness scaling,
# dry_airmass_diurnal's dewpoint-depression age, surface_signs' cloud/wind/dp
# categories individually).
#
# From the 2026-09-22 "Synoptic Signal Ideas" brainstorm, radiational_cooling
# section. Night-only by design (see _is_night) -- during the day the
# mechanism this model reasons about doesn't apply, so every member abstains.
#
# members:
#   1  cooling_potential_index          joint (wind calm, cloud, dewpoint
#                                        depression) trigger state
#   2  wind_lull_frequency              wind_lull isn't used by name anywhere
#                                        else in barogram yet -- refines
#                                        member 1's calm-wind read from a
#                                        single trailing average into an
#                                        actual lull frequency
#   3  metro_heat_retention_correction  learned residual: actual cooling rate
#                                        vs member 1's own predicted rate,
#                                        the standing Twin-Cities-proximity gap
#   4  self_correction                  standard self-correction member
#                                        (models/_self_correction.py) --
#                                        member_id=0 minus this model's own
#                                        learned bias

import statistics

import db
import models._confidence as _confidence
import models._self_correction as _self_correction
from models._climo_weights import LEAD_HOURS
from models._utils import _sector
from models.surface_signs import (
    _build_solar_climo,
    _find_nearest_ts,
    _obs_in_window,
    _solar_cloud_category,
)

MODEL_ID = 28
MODEL_NAME = "radiational_cooling"
NEEDS_CONN_IN = True
NEEDS_CONN_OUT = True
NEEDS_WEIGHTS = True
NEEDS_ALL_OBS = True
NEEDS_MATCH_HISTORY = True

_SELF_CORRECTION_MEMBER = 4

_SIGNAL_WINDOW_SEC = 3 * 3600  # trailing window for wind average and lull frequency
_FUTURE_LOOKUP_SEC = 900
_MIN_SAMPLES = 3
_MIN_WIND_SAMPLES = 2

_SOLAR_FLOOR_W = 5.0          # same day/night cutoff surface_signs uses
# how far back to hunt for the last daytime cloud read. 20h, not 15h: a
# pre-dawn obs's most recent daylight hours are themselves low-sun-angle
# (late afternoon/dusk), where surface_signs' _solar_cloud_category already
# returns None on purpose (climo mean too low there to classify reliably) --
# a shorter lookback would leave every pre-dawn obs stuck at None. 20h reaches
# back to the previous day's high-sun midday, which does classify reliably.
_CLOUD_LOOKBACK_SEC = 20 * 3600

_CALM_WIND_MS = 2.0    # trailing wind_avg mean below this: calm
_WINDY_WIND_MS = 5.0   # above this: windy; between the two: moderate

_LULL_FLOOR_MS = 0.5   # a reading counts as a "lull" if wind_lull <= this
_LULL_FREQUENT = 0.6   # fraction of window readings that are lulls, above: frequent
_LULL_RARE = 0.2       # below: rare; between: occasional

_DP_MOIST_C = 3.0   # dewpoint depression below this: moist
_DP_DRY_C = 8.0     # above this: dry; between: moderate

# radiational cooling is a temperature/dewpoint mechanism -- pressure isn't part
# of the story here, unlike the full 3-variable VARIABLES dict most models use
_VARIABLES = {"temperature": "air_temp", "dewpoint": "dew_point"}


def _is_night(row):
    sr = row["solar_radiation"]
    return sr is None or sr <= _SOLAR_FLOOR_W


def _wind_calm_category(window_obs):
    vals = [r["wind_avg"] for r in window_obs if r["wind_avg"] is not None]
    if len(vals) < _MIN_WIND_SAMPLES:
        return None
    mean = sum(vals) / len(vals)
    if mean < _CALM_WIND_MS:
        return "calm"
    if mean > _WINDY_WIND_MS:
        return "windy"
    return "moderate"


def _lull_frequency(window_obs):
    vals = [r["wind_lull"] for r in window_obs if r.get("wind_lull") is not None]
    if len(vals) < _MIN_WIND_SAMPLES:
        return None
    lulls = sum(1 for v in vals if v <= _LULL_FLOOR_MS)
    return lulls / len(vals)


def _lull_frequency_category(freq):
    if freq is None:
        return None
    if freq > _LULL_FREQUENT:
        return "frequent"
    if freq < _LULL_RARE:
        return "rare"
    return "occasional"


def _dp_depression_category(row):
    t, d = row["air_temp"], row["dew_point"]
    if t is None or d is None:
        return None
    depression = t - d
    if depression < _DP_MOIST_C:
        return "moist"
    if depression > _DP_DRY_C:
        return "dry"
    return "moderate"


def _persisted_cloud_category(sorted_ts, by_ts, solar_climo, ts):
    """Cloud cover persists across sunset more often than not, so a night obs
    borrows the most recent daytime cloud read within _CLOUD_LOOKBACK_SEC
    rather than reporting None for the whole night (_solar_cloud_category
    itself is None whenever solar_radiation is at/below the day/night floor)."""
    window = _obs_in_window(sorted_ts, by_ts, ts - _CLOUD_LOOKBACK_SEC, ts)
    for row in reversed(window):
        cat = _solar_cloud_category(row, solar_climo)
        if cat is not None:
            return cat
    return None


def _cooling_index_category(sorted_ts, by_ts, solar_climo, ts, row=None):
    window = _obs_in_window(sorted_ts, by_ts, ts - _SIGNAL_WINDOW_SEC, ts)
    wind = _wind_calm_category(window)
    cloud = _persisted_cloud_category(sorted_ts, by_ts, solar_climo, ts)
    dp = _dp_depression_category(row if row is not None else by_ts[ts])
    if None in (wind, cloud, dp):
        return None
    return (wind, cloud, dp)


def _build_conditionals(signal_fn, night_ts, by_ts, sorted_ts):
    """Same bucket-history/average-future-delta pattern as surface_signs'
    _build_signal_conditionals, restricted to nighttime timestamps only --
    this model's whole mechanism doesn't apply during the day."""
    all_cols = list(_VARIABLES.values())
    accum = {}
    for ts in night_ts:
        cat = signal_fn(ts)
        if cat is None:
            continue
        row_now = by_ts[ts]
        for lead in LEAD_HOURS:
            ts_fut = _find_nearest_ts(sorted_ts, ts + lead * 3600, _FUTURE_LOOKUP_SEC)
            if ts_fut is None:
                continue
            row_fut = by_ts[ts_fut]
            for col in all_cols:
                v_now = row_now[col]
                v_fut = row_fut[col]
                if v_now is not None and v_fut is not None:
                    accum.setdefault((cat, col, lead), []).append(v_fut - v_now)
    return {k: sum(v) / len(v) for k, v in accum.items() if len(v) >= _MIN_SAMPLES}


def _build_retention_correction(night_ts, by_ts, sorted_ts, index_conds, index_cat_fn):
    """Residual = actual future delta minus what member 1's own transfer
    function would have predicted, averaged per (column, lead). Learns the
    standing metro-heat-retention gap directly rather than assuming a value."""
    all_cols = list(_VARIABLES.values())
    accum = {}
    for ts in night_ts:
        cat = index_cat_fn(ts)
        if cat is None:
            continue
        row_now = by_ts[ts]
        for lead in LEAD_HOURS:
            ts_fut = _find_nearest_ts(sorted_ts, ts + lead * 3600, _FUTURE_LOOKUP_SEC)
            if ts_fut is None:
                continue
            row_fut = by_ts[ts_fut]
            for col in all_cols:
                predicted_delta = index_conds.get((cat, col, lead))
                if predicted_delta is None:
                    continue
                v_now, v_fut = row_now[col], row_fut[col]
                if v_now is not None and v_fut is not None:
                    actual_delta = v_fut - v_now
                    accum.setdefault((col, lead), []).append(actual_delta - predicted_delta)
    return {k: sum(v) / len(v) for k, v in accum.items() if len(v) >= _MIN_SAMPLES}


def run(obs, issued_at, *, conn_in, conn_out=None, weights=None, all_obs=None,
        member_history=None, default_matches=None):
    if all_obs is None:
        all_obs = db.tempest_obs_in_range(conn_in, 0, issued_at)

    by_ts = {row["timestamp"]: row for row in all_obs}
    sorted_ts = sorted(by_ts)
    night_ts = [ts for ts in sorted_ts if _is_night(by_ts[ts])]
    solar_climo = _build_solar_climo(all_obs)

    def index_cat_fn(ts, row=None):
        return _cooling_index_category(sorted_ts, by_ts, solar_climo, ts, row=row)

    def lull_cat_fn(ts):
        window = _obs_in_window(sorted_ts, by_ts, ts - _SIGNAL_WINDOW_SEC, ts)
        return _lull_frequency_category(_lull_frequency(window))

    index_conds = _build_conditionals(index_cat_fn, night_ts, by_ts, sorted_ts)
    lull_conds = _build_conditionals(lull_cat_fn, night_ts, by_ts, sorted_ts)
    retention_corrections = _build_retention_correction(
        night_ts, by_ts, sorted_ts, index_conds, index_cat_fn
    )

    live_night = _is_night(obs)
    live_index_cat = index_cat_fn(obs["timestamp"], row=obs) if live_night else None
    live_window = _obs_in_window(
        sorted_ts, by_ts, obs["timestamp"] - _SIGNAL_WINDOW_SEC, obs["timestamp"]
    )
    live_lull_cat = (
        _lull_frequency_category(_lull_frequency(live_window)) if live_night else None
    )

    _MEMBERS = [
        (1, "cooling_potential_index"),
        (2, "wind_lull_frequency"),
        (3, "metro_heat_retention_correction"),
    ]
    all_member_ids = [mid for mid, _ in _MEMBERS]

    confidence_cache = {
        (variable, lead): _confidence.member_confidences(
            member_history, default_matches, all_member_ids + [_SELF_CORRECTION_MEMBER], variable, lead
        )
        for variable in _VARIABLES
        for lead in LEAD_HOURS
    }

    def member1_value(col, lead, obs_val):
        if live_index_cat is None or obs_val is None:
            return None
        delta = index_conds.get((live_index_cat, col, lead))
        return obs_val + delta if delta is not None else None

    def member2_value(col, lead, obs_val):
        if live_lull_cat is None or obs_val is None:
            return None
        delta = lull_conds.get((live_lull_cat, col, lead))
        return obs_val + delta if delta is not None else None

    def member3_value(col, lead, obs_val):
        base = member1_value(col, lead, obs_val)
        if base is None:
            return None
        correction = retention_corrections.get((col, lead))
        return base + correction if correction is not None else None

    member_value_fns = {1: member1_value, 2: member2_value, 3: member3_value}

    rows = []
    values_by_var_lead = {}
    for mid, _ in _MEMBERS:
        value_fn = member_value_fns[mid]
        for variable, col in _VARIABLES.items():
            obs_val = obs[col]
            for lead in LEAD_HOURS:
                value = value_fn(col, lead, obs_val)
                values_by_var_lead.setdefault((variable, lead), []).append((mid, value))
                rows.append({
                    "model_id": MODEL_ID,
                    "model": MODEL_NAME,
                    "member_id": mid,
                    "issued_at": issued_at,
                    "valid_at": obs["timestamp"] + lead * 3600,
                    "lead_hours": lead,
                    "variable": variable,
                    "value": value,
                    "confidence": confidence_cache[(variable, lead)].get(mid),
                })

    for variable, col in _VARIABLES.items():
        for lead in LEAD_HOURS:
            valid_at = obs["timestamp"] + lead * 3600
            cell_confidences = confidence_cache[(variable, lead)]
            valid_pairs = [
                (mid, v) for mid, v in values_by_var_lead[(variable, lead)] if v is not None
            ]

            if not valid_pairs:
                mean, group_confidence = None, None
            elif weights:
                member_weights = {
                    mid: weights.get((mid, variable, lead, _sector(valid_at)))
                    for mid, _ in valid_pairs
                }
                confidences = {mid: cell_confidences.get(mid) for mid, _ in valid_pairs}
                mean, group_confidence = _confidence.combine_pattern(
                    valid_pairs, member_weights, confidences
                )
            else:
                mean = sum(v for _, v in valid_pairs) / len(valid_pairs)
                group_confidence = _confidence.average_confidence(
                    [cell_confidences.get(mid) for mid, _ in valid_pairs]
                )
            spread = (
                statistics.pstdev([v for _, v in valid_pairs])
                if len(valid_pairs) > 1 else None
            )

            rows.append({
                "model_id": MODEL_ID,
                "model": MODEL_NAME,
                "member_id": 0,
                "issued_at": issued_at,
                "valid_at": valid_at,
                "lead_hours": lead,
                "variable": variable,
                "value": mean,
                "spread": spread,
                "confidence": group_confidence,
            })

            corrected = _self_correction.corrected_value(
                conn_out, MODEL_ID, variable, lead, mean, issued_at
            )
            rows.append({
                "model_id": MODEL_ID,
                "model": MODEL_NAME,
                "member_id": _SELF_CORRECTION_MEMBER,
                "issued_at": issued_at,
                "valid_at": valid_at,
                "lead_hours": lead,
                "variable": variable,
                "value": corrected,
                "confidence": cell_confidences.get(_SELF_CORRECTION_MEMBER),
            })

    return rows
