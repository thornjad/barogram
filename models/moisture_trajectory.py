# moisture_trajectory: every existing model reads dewpoint; almost none read
# relative_humidity as its own dimension, and nothing tracks the moisture
# picture's rate of change, only its level. This model's members each read a
# different moisture-trend signal and forecast where temperature/dewpoint head
# next, the same bucket-history/average-future-delta pattern as
# wind_veer_detector and frontal_trigger (members 1-3 and 5) plus a direct
# regression extrapolation for member 4.
#
# From the shared "Synoptic Signal Ideas" model-ideas brainstorm,
# moisture_trajectory section (Part A).
#
# members:
#   1  rh_dd_divergence      relative_humidity trend vs dewpoint-depression
#                            (air_temp - dew_point) trend over the same 2h
#                            window. RH climbing while depression stays flat
#                            is the fog/frost-setup signature this misses
#                            when depression alone looks unremarkable.
#   2  moisture_convergence  dew_point trend plus wind_direction steadiness
#                            (reuses wind_veer_detector's rotation category).
#                            Dewpoint rising on steady wind reads as
#                            advection; rising while the wind veers/backs
#                            reads as local (evapotranspiration/drainage).
#   3  dd_closing_rate       rate of change of the dewpoint depression itself,
#                            not its level (dry_airmass_diurnal already scales
#                            on level) -- a rapidly closing spread overnight
#                            caps further cooling once saturation is reached.
#   4  saturation_countdown  degree-1 extrapolation of the depression itself
#                            (same regression machinery as pressure_tendency),
#                            floored at zero -- temperature can't fall below
#                            dew_point. Forecasts temperature only. Tempest has
#                            no cloud-cover column, so "clear" isn't checkable
#                            here; calm wind (own obs) is the achievable proxy
#                            gate, and the member abstains entirely without it.
#   5  delta_t_trend         rate of change of delta_t (wet-bulb depression,
#                            station-derived, wxlog migration 004). Direct
#                            evaporative-drying-power measure distinct from
#                            dewpoint depression -- factors in wind and
#                            pressure too. Partial firmware coverage means
#                            this member abstains far more often than the
#                            others; it can seed a live signal but doesn't yet
#                            have enough matched history to backfill cleanly.
#   6  self_correction       standard self-correction member (models/_self_correction.py)
#                            -- member_id=0 minus this model's own learned bias

import statistics

import db
import models._confidence as _confidence
import models._self_correction as _self_correction
from models._climo_weights import LEAD_HOURS
from models._utils import _sector
from models.pressure_tendency import _poly_eval, _poly_fit
from models.surface_signs import _find_nearest_ts, _obs_in_window
from models.wind_veer_detector import _rotation_category

MODEL_ID = 29
MODEL_NAME = "moisture_trajectory"
NEEDS_CONN_IN = True
NEEDS_CONN_OUT = True
NEEDS_WEIGHTS = True
NEEDS_ALL_OBS = True
NEEDS_MATCH_HISTORY = True

_SELF_CORRECTION_MEMBER = 6

VAR_COL = {
    "temperature": "air_temp",
    "dewpoint": "dew_point",
}

_LOOKUP_SEC = 600
_FUTURE_LOOKUP_SEC = 900
_MIN_SAMPLES = 3

_RH_WINDOW_SEC = 2 * 3600
_RH_RISE_PCT = 5.0     # % relative_humidity rise over the window to count as "climbing"
_DD_FLAT_C = 0.5       # depression change below this reads as "flat" for member 1

_CONV_WINDOW_SEC = 3 * 3600
_TD_RISE_C = 0.3       # dew_point rise/fall over the window to count as moistening/drying

_CLOSE_WINDOW_SEC = 3 * 3600
_DD_FAST_CLOSE_C = -1.5
_DD_SLOW_CLOSE_C = -0.5
_DD_WIDEN_C = 0.5

_DT_WINDOW_SEC = 3 * 3600
_DT_TREND_C = 0.5      # delta_t change over the window to count as accelerating/decelerating

_SATURATION_WINDOW_SEC = 2 * 3600
_CALM_WIND_MS = 1.5    # matches surface_signs/synoptic_state_machine's own "calm" floor

_MEMBERS = [
    (1, "rh_dd_divergence"),
    (2, "moisture_convergence"),
    (3, "dd_closing_rate"),
    (4, "saturation_countdown"),
    (5, "delta_t_trend"),
]
_ALL_MEMBER_IDS = [mid for mid, _ in _MEMBERS]
_BUCKET_MEMBER_IDS = [1, 2, 3, 5]  # member 4 is a direct regression, not a bucket lookup


def _dd(row: dict) -> float | None:
    """Dewpoint depression (air_temp - dew_point), or None if either is missing."""
    t, td = row.get("air_temp"), row.get("dew_point")
    return t - td if t is not None and td is not None else None


def _rh_dd_category(row_now: dict | None, row_past: dict | None) -> str | None:
    if row_now is None or row_past is None:
        return None
    rh_now, rh_past = row_now.get("relative_humidity"), row_past.get("relative_humidity")
    dd_now, dd_past = _dd(row_now), _dd(row_past)
    if rh_now is None or rh_past is None or dd_now is None or dd_past is None:
        return None
    d_rh = rh_now - rh_past
    d_dd = dd_now - dd_past
    if d_rh >= _RH_RISE_PCT and abs(d_dd) < _DD_FLAT_C:
        return "divergent_rh_rise"
    if d_rh >= _RH_RISE_PCT and d_dd <= -_DD_FLAT_C:
        return "concurrent_moistening"
    if d_rh <= -_RH_RISE_PCT:
        return "drying"
    return "steady"


def _convergence_category(row_now: dict | None, row_past: dict | None, window: list) -> str | None:
    if row_now is None or row_past is None:
        return None
    td_now, td_past = row_now.get("dew_point"), row_past.get("dew_point")
    if td_now is None or td_past is None:
        return None
    d_td = td_now - td_past
    rot_cat = _rotation_category(window, 0.0)
    if d_td >= _TD_RISE_C:
        if rot_cat == "steady":
            return "advection_moistening"
        if rot_cat in ("veering", "backing"):
            return "local_moistening"
        return None  # can't tell advection from local without a wind-steadiness read
    if d_td <= -_TD_RISE_C:
        return "drying"
    return "steady"


def _closing_category(row_now: dict | None, row_past: dict | None) -> str | None:
    if row_now is None or row_past is None:
        return None
    dd_now, dd_past = _dd(row_now), _dd(row_past)
    if dd_now is None or dd_past is None:
        return None
    d_dd = dd_now - dd_past
    if d_dd <= _DD_FAST_CLOSE_C:
        return "closing_fast"
    if d_dd <= _DD_SLOW_CLOSE_C:
        return "closing_slow"
    if d_dd >= _DD_WIDEN_C:
        return "widening"
    return "steady"


def _delta_t_category(dt_now: float | None, dt_past: float | None) -> str | None:
    if dt_now is None or dt_past is None:
        return None
    d = dt_now - dt_past
    if d >= _DT_TREND_C:
        return "drying_accelerating"
    if d <= -_DT_TREND_C:
        return "drying_decelerating"
    return "steady"


def _live_categories(obs, by_ts, sorted_ts, dt_by_ts, dt_sorted_ts) -> dict[int, str | None]:
    obs_ts = obs["timestamp"]

    ts_rh_past = _find_nearest_ts(sorted_ts, obs_ts - _RH_WINDOW_SEC, _LOOKUP_SEC)
    row_rh_past = by_ts.get(ts_rh_past) if ts_rh_past is not None else None

    ts_conv_past = _find_nearest_ts(sorted_ts, obs_ts - _CONV_WINDOW_SEC, _LOOKUP_SEC)
    row_conv_past = by_ts.get(ts_conv_past) if ts_conv_past is not None else None
    conv_window = _obs_in_window(sorted_ts, by_ts, obs_ts - _CONV_WINDOW_SEC, obs_ts)

    ts_close_past = _find_nearest_ts(sorted_ts, obs_ts - _CLOSE_WINDOW_SEC, _LOOKUP_SEC)
    row_close_past = by_ts.get(ts_close_past) if ts_close_past is not None else None

    dt_now_ts = _find_nearest_ts(dt_sorted_ts, obs_ts, _LOOKUP_SEC)
    dt_now = dt_by_ts.get(dt_now_ts) if dt_now_ts is not None else None
    dt_past_ts = _find_nearest_ts(dt_sorted_ts, obs_ts - _DT_WINDOW_SEC, _LOOKUP_SEC)
    dt_past = dt_by_ts.get(dt_past_ts) if dt_past_ts is not None else None

    return {
        1: _rh_dd_category(obs, row_rh_past),
        2: _convergence_category(obs, row_conv_past, conv_window),
        3: _closing_category(obs, row_close_past),
        5: _delta_t_category(dt_now, dt_past),
    }


def _build_conditionals(all_obs: list[dict], dt_rows: list[dict]) -> dict[int, dict]:
    """One pass over history: per-(member, category, column, lead) conditional
    mean future delta, the standard bucket/average pattern every categorical
    model in this ensemble uses (see wind_veer_detector, frontal_trigger)."""
    by_ts = {row["timestamp"]: row for row in all_obs}
    sorted_ts = sorted(by_ts)
    dt_by_ts = {row["timestamp"]: row["delta_t"] for row in dt_rows}
    dt_sorted_ts = sorted(dt_by_ts)

    accum: dict[int, dict] = {mid: {} for mid in _BUCKET_MEMBER_IDS}

    for ts in sorted_ts:
        row_now = by_ts[ts]
        cats = _live_categories(row_now, by_ts, sorted_ts, dt_by_ts, dt_sorted_ts)

        for lead in LEAD_HOURS:
            ts_fut = _find_nearest_ts(sorted_ts, ts + lead * 3600, _FUTURE_LOOKUP_SEC)
            if ts_fut is None:
                continue
            row_fut = by_ts[ts_fut]
            for mid, cat in cats.items():
                if cat is None:
                    continue
                for col in VAR_COL.values():
                    v_now = row_now.get(col)
                    v_fut = row_fut.get(col)
                    if v_now is not None and v_fut is not None:
                        accum[mid].setdefault((cat, col, lead), []).append(v_fut - v_now)

    return {
        mid: {k: sum(v) / len(v) for k, v in cell.items() if len(v) >= _MIN_SAMPLES}
        for mid, cell in accum.items()
    }


def run(obs, issued_at, *, conn_in, conn_out=None, weights=None, all_obs=None,
        member_history=None, default_matches=None) -> list[dict]:
    if all_obs is None:
        all_obs = db.tempest_obs_in_range(conn_in, 0, issued_at)
    dt_rows = db.tempest_delta_t_in_range(conn_in, 0, issued_at)

    conds = _build_conditionals(all_obs, dt_rows)

    by_ts = {row["timestamp"]: row for row in all_obs}
    sorted_ts = sorted(by_ts)
    dt_by_ts = {row["timestamp"]: row["delta_t"] for row in dt_rows}
    dt_sorted_ts = sorted(dt_by_ts)
    obs_ts = obs["timestamp"]

    live_cats = _live_categories(obs, by_ts, sorted_ts, dt_by_ts, dt_sorted_ts)

    # member 4: saturation countdown -- degree-1 fit of the depression itself over
    # a short window, extrapolated forward and floored at zero, gated to calm wind
    # (the closest checkable proxy for the "clear" condition this needs but can't
    # verify directly from Tempest alone)
    temp4_by_lead: dict[int, float | None] = {lead: None for lead in LEAD_HOURS}
    wind_now = obs.get("wind_avg")
    obs_td = obs.get("dew_point")
    if wind_now is not None and wind_now <= _CALM_WIND_MS and obs_td is not None:
        sat_window = [
            r for r in all_obs
            if r["timestamp"] >= obs_ts - _SATURATION_WINDOW_SEC and _dd(r) is not None
        ]
        if len(sat_window) >= 2:
            t_vals = [(r["timestamp"] - obs_ts) / 3600.0 for r in sat_window]
            dd_vals = [_dd(r) for r in sat_window]
            coefs = _poly_fit(t_vals, dd_vals, 1)
            if coefs is not None:
                for lead in LEAD_HOURS:
                    dd_pred = max(0.0, _poly_eval(coefs, float(lead)))
                    temp4_by_lead[lead] = obs_td + dd_pred

    def _member_value(mid: int, variable: str, col: str, lead: int) -> float | None:
        if mid == 4:
            return temp4_by_lead[lead] if variable == "temperature" else None
        cat = live_cats[mid]
        obs_val = obs.get(col)
        if cat is None or obs_val is None:
            return None
        delta = conds[mid].get((cat, col, lead))
        return obs_val + delta if delta is not None else None

    cell_confidences_by_var_lead = {
        (variable, lead): _confidence.member_confidences(
            member_history, default_matches, _ALL_MEMBER_IDS + [_SELF_CORRECTION_MEMBER], variable, lead
        )
        for variable in VAR_COL
        for lead in LEAD_HOURS
    }

    rows = []
    for mid, _name in _MEMBERS:
        for variable, col in VAR_COL.items():
            for lead in LEAD_HOURS:
                rows.append({
                    "model_id": MODEL_ID,
                    "model": MODEL_NAME,
                    "member_id": mid,
                    "issued_at": issued_at,
                    "valid_at": obs_ts + lead * 3600,
                    "lead_hours": lead,
                    "variable": variable,
                    "value": _member_value(mid, variable, col, lead),
                    "confidence": cell_confidences_by_var_lead[(variable, lead)].get(mid),
                })

    for variable, col in VAR_COL.items():
        for lead in LEAD_HOURS:
            valid_at = obs_ts + lead * 3600
            cell_confidences = cell_confidences_by_var_lead[(variable, lead)]
            valid_pairs = [
                (mid, v) for mid, _name in _MEMBERS
                if (v := _member_value(mid, variable, col, lead)) is not None
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
