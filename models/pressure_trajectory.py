# pressure_trajectory: pressure is an intermediate signal here, not a forecast
# target the ensemble cares about on its own -- same philosophy as
# pressure_tendency, pressure_trend_cascade, and pressure_consensus_transfer.
# A pressure signal gets classified (or, for member 3, extrapolated), then a
# transfer function learned from the station's own history maps that state to
# the expected temperature and dewpoint delta. Every member also emits its own
# view of the pressure variable itself, the same way pressure_trend_cascade's
# regression members do -- categorical members via the same bucket/average
# delta lookup applied to every VARIABLES column, member 3 via its own decay
# model's predicted pressure directly.
#
# Every member reads station_pressure through models._pressure_tide first,
# which subtracts this station's own learned twice-daily solar pressure tide
# before any tendency/trend/trough signal is computed -- see that module's
# docstring. Transfer-function *targets* (temperature, dewpoint, and the
# pressure value itself when a member emits one) stay in real, tide-included
# space; only the *inputs* to categorization get detided.
#
# members:
#   1  pressure_jerk         second derivative of station_pressure (this
#                             window's 3h tendency rate minus the prior 3h
#                             tendency rate), not just the tendency rate
#                             pressure_trend_cascade already extrapolates.
#                             Categorized into accelerating_fall /
#                             decelerating_fall / accelerating_rise /
#                             decelerating_rise / steady, used as a transfer
#                             key the same shape as pressure_tendency's
#                             zambretti category-to-delta table. Best 1-6h.
#   2  trend_agreement       whether the 1h/6h/24h tendency rates agree in
#                             sign. Doubles as its own transfer key
#                             (agree_rising/agree_falling/diverging) AND as a
#                             confidence gate on this model's own ensemble
#                             row: full trust when the windows agree, damped
#                             when they diverge. Best 6-18h.
#   3  post_frontal_ringing  after a confirmed frontal passage (a pressure
#                             trough, see _detect_troughs, coincident with a
#                             wind veer/backing), forecasts pressure decaying
#                             back toward its pre-event baseline rather than
#                             extrapolating flat. Event-gated: abstains
#                             entirely outside the ringing window. Best
#                             3-12h.
#   4  days_since_front      hours since the most recent pressure trough
#                             (any trough, no veer confirmation required --
#                             generalizes dry_airmass_diurnal's dewpoint-
#                             depression-age idea to any front), bucketed
#                             and used as an airmass-age transfer key. Best
#                             12-24h.
#   5  front_phase_state     combines jerk + trend_agreement + ringing state
#                             + days-since-front into one categorical state:
#                             pre_frontal / at_passage / post_frontal_recovery
#                             / quiescent. The primary transfer key -- every
#                             other member here is one of its inputs. Best
#                             1-18h.
#   6  self_correction       standard self-correction member
#                             (models/_self_correction.py) -- member_id=0
#                             minus this model's own learned bias

import bisect
import math
import statistics

import db
import models._confidence as _confidence
import models._pressure_tide as _pressure_tide
import models._self_correction as _self_correction
from models._climo_weights import LEAD_HOURS, VARIABLES
from models._utils import _sector
from models.pressure_trend_cascade import _build_delta_transfer_fns
from models.surface_signs import _find_nearest_ts, _obs_in_window
from models.wind_veer_detector import _rotation_category

MODEL_ID = 27
MODEL_NAME = "pressure_trajectory"
NEEDS_CONN_IN = True
NEEDS_CONN_OUT = True
NEEDS_WEIGHTS = True
NEEDS_ALL_OBS = True
NEEDS_MATCH_HISTORY = True

_SELF_CORRECTION_MEMBER = 6

_RATE_WINDOW_SEC = 3 * 3600   # 3h, matches pressure_tendency's zambretti window
_LOOKUP_SEC = 600             # +/- 10 min
_FUTURE_LOOKUP_SEC = 900      # +/- 15 min
_MIN_SAMPLES = 3

# member 1: jerk thresholds, both in hPa/h (steady) and hPa/h^2-ish (the jerk
# itself is a difference of two rates 3h apart, not divided by that 3h, since
# only its sign and rough magnitude relative to this threshold matter for a
# categorical bucket -- tune sorts out how much the category is worth)
_JERK_STEADY_HPA_PER_H = 0.033      # ~pressure_tendency's _SLOW (0.1/3h) per hour
_JERK_THRESHOLD = 0.03

# member 2: multi-window trend agreement
_AGREEMENT_WINDOWS_H = (1, 6, 24)
_AGREEMENT_SLOW_HPA_PER_H = 0.033   # same dead-zone as member 1's steady threshold
_DIVERGING_CONFIDENCE_MULT = 0.6    # damps this model's own ensemble-row confidence
                                     # when the three windows disagree in sign

# members 3-5: trough detection shared by post_frontal_ringing and
# days_since_front; ringing layers a wind-veer confirmation on top of the same
# trough list days_since_front uses unconfirmed, generalizing it to any front.
_TROUGH_HALF_WINDOW_H = 6     # local-minimum window on each side of a candidate
_TROUGH_PROMINENCE_HPA = 1.0  # min rise out of the trough on both sides to count
_RINGING_MAX_AGE_H = 12       # stop emitting once this many hours past the trough
_RINGING_SETTLING_AGE_H = 3   # age past which ringing counts as "settling"
_RINGING_VEER_WINDOW_H = 6    # window around the trough checked for a veer/back
_RINGING_BASELINE_LOOKBACK_H = (24, 12)  # pre-event baseline window: [trough-24h, trough-12h]
_RINGING_DECAY_LAMBDA = 0.15  # per hour; same e-folding family as pressure_trend_cascade's damped_extrap

_FRONT_AGE_BUCKETS = (  # (age_hours_exclusive_upper_bound, bucket_name)
    (6, "at_trough"),
    (24, "recovering"),
    (72, "aging"),
    (math.inf, "stale"),
)

_MEMBERS = [
    (1, "pressure_jerk"),
    (2, "trend_agreement"),
    (3, "post_frontal_ringing"),
    (4, "days_since_front"),
    (5, "front_phase_state"),
]
_ALL_MEMBER_IDS = [mid for mid, _ in _MEMBERS]


def _tendency_rate(now_ts, now_pressure, by_ts, sorted_ts, profile, window_sec=_RATE_WINDOW_SEC):
    """hPa/h rate over the trailing window_sec ending at now_ts, on tide-detided
    pressure so the tide's own twice-daily wiggle doesn't masquerade as
    tendency. now_pressure is the raw reading at now_ts, passed explicitly so
    this works for a live obs that isn't itself a row in by_ts."""
    if now_pressure is None:
        return None
    ts_past = _find_nearest_ts(sorted_ts, now_ts - window_sec, _LOOKUP_SEC)
    if ts_past is None:
        return None
    p_now = _pressure_tide.detided(now_pressure, now_ts, profile)
    p_past = _pressure_tide.detided(by_ts[ts_past]["station_pressure"], ts_past, profile)
    if p_now is None or p_past is None:
        return None
    hours = (now_ts - ts_past) / 3600.0
    if hours <= 0:
        return None
    return (p_now - p_past) / hours


def _jerk_category(now_ts, now_pressure, by_ts, sorted_ts, profile):
    rate_now = _tendency_rate(now_ts, now_pressure, by_ts, sorted_ts, profile)
    if rate_now is None:
        return None
    ts_prior = _find_nearest_ts(sorted_ts, now_ts - _RATE_WINDOW_SEC, _LOOKUP_SEC)
    if ts_prior is None:
        return None
    rate_past = _tendency_rate(ts_prior, by_ts[ts_prior]["station_pressure"], by_ts, sorted_ts, profile)
    if rate_past is None:
        return None
    jerk = rate_now - rate_past
    if rate_now < -_JERK_STEADY_HPA_PER_H:
        return "accelerating_fall" if jerk <= -_JERK_THRESHOLD else "decelerating_fall"
    if rate_now > _JERK_STEADY_HPA_PER_H:
        return "accelerating_rise" if jerk >= _JERK_THRESHOLD else "decelerating_rise"
    return "steady"


def _agreement_category(now_ts, now_pressure, by_ts, sorted_ts, profile):
    signs = []
    for h in _AGREEMENT_WINDOWS_H:
        rate = _tendency_rate(now_ts, now_pressure, by_ts, sorted_ts, profile, h * 3600)
        if rate is None:
            return None
        if rate > _AGREEMENT_SLOW_HPA_PER_H:
            signs.append("rising")
        elif rate < -_AGREEMENT_SLOW_HPA_PER_H:
            signs.append("falling")
        else:
            signs.append("steady")
    if signs[0] == signs[1] == signs[2] and signs[0] != "steady":
        return f"agree_{signs[0]}"
    return "diverging"


def _detect_troughs(sorted_ts, by_ts, profile):
    """Local minima in tide-detided station_pressure across the full history: a
    point lower than every other point within _TROUGH_HALF_WINDOW_H on both
    sides, by at least _TROUGH_PROMINENCE_HPA on each side. Airmass-turnover
    markers shared by days_since_front (used as-is) and post_frontal_ringing
    (which layers a wind-veer confirmation on top). Returns a sorted list of
    trough timestamps."""
    detided = [
        (t, p) for t in sorted_ts
        if (p := _pressure_tide.detided(by_ts[t]["station_pressure"], t, profile)) is not None
    ]
    half_window_sec = _TROUGH_HALF_WINDOW_H * 3600
    troughs = []
    for i, (t_i, p_i) in enumerate(detided):
        lo = bisect.bisect_left(detided, (t_i - half_window_sec, -math.inf))
        hi = bisect.bisect_right(detided, (t_i + half_window_sec, math.inf))
        neighborhood = detided[lo:hi]
        left = [p for t, p in neighborhood if t < t_i]
        right = [p for t, p in neighborhood if t > t_i]
        if not left or not right:
            continue
        if (p_i <= min(left) - _TROUGH_PROMINENCE_HPA
                and p_i <= min(right) - _TROUGH_PROMINENCE_HPA):
            troughs.append(t_i)
    return troughs


def _confirmed_ringing_troughs(troughs, by_ts, sorted_ts):
    """Subset of troughs coincident with a wind veer/backing within
    _RINGING_VEER_WINDOW_H -- the frontal-passage signature that distinguishes
    a ringing event from any other pressure trough."""
    confirmed = []
    for t in troughs:
        window = _obs_in_window(
            sorted_ts, by_ts, t - _RINGING_VEER_WINDOW_H * 3600, t + _RINGING_VEER_WINDOW_H * 3600
        )
        if _rotation_category(window, 0.0) in ("veering", "backing"):
            confirmed.append(t)
    return confirmed


def _ringing_state_at(now_ts, confirmed_troughs):
    """None if no confirmed ringing trough is within _RINGING_MAX_AGE_H of
    now_ts. Otherwise {trough_ts, age_h, settling}: settling is True once the
    ringing has passed _RINGING_SETTLING_AGE_H, distinguishing "just started"
    from "settling" for front_phase_state."""
    idx = bisect.bisect_right(confirmed_troughs, now_ts) - 1
    if idx < 0:
        return None
    trough_ts = confirmed_troughs[idx]
    age_h = (now_ts - trough_ts) / 3600.0
    if age_h > _RINGING_MAX_AGE_H:
        return None
    return {"trough_ts": trough_ts, "age_h": age_h, "settling": age_h > _RINGING_SETTLING_AGE_H}


def _age_bucket_at(now_ts, troughs):
    """Bucketed hours since the most recent trough (any trough, unconfirmed) at
    or before now_ts. None if no trough has ever been recorded yet."""
    idx = bisect.bisect_right(troughs, now_ts) - 1
    if idx < 0:
        return None
    age_h = (now_ts - troughs[idx]) / 3600.0
    for max_age, name in _FRONT_AGE_BUCKETS:
        if age_h < max_age:
            return name
    return _FRONT_AGE_BUCKETS[-1][1]


def _ringing_baseline(trough_ts, by_ts, sorted_ts, profile):
    """Mean tide-detided pressure over [trough_ts-24h, trough_ts-12h] -- the
    pre-event level the ringing decays back toward."""
    lo = trough_ts - _RINGING_BASELINE_LOOKBACK_H[0] * 3600
    hi = trough_ts - _RINGING_BASELINE_LOOKBACK_H[1] * 3600
    window = _obs_in_window(sorted_ts, by_ts, lo, hi)
    vals = [
        p for r in window
        if (p := _pressure_tide.detided(r["station_pressure"], r["timestamp"], profile)) is not None
    ]
    return sum(vals) / len(vals) if vals else None


def _ringing_decay(obs_detided, baseline, lead_h):
    """OU-style decay of the current detided pressure's distance from baseline,
    the same shape as pressure_tendency's _apply_mean_reversion but with its
    own faster, event-scoped lambda."""
    if obs_detided is None or baseline is None:
        return None
    damp = math.exp(-_RINGING_DECAY_LAMBDA * lead_h)
    return baseline + (obs_detided - baseline) * damp


def _front_phase(jerk_cat, agreement_cat, ringing_state, age_bucket):
    """pre_frontal: falling and accelerating, no ringing yet. at_passage:
    ringing just started (not yet settling) while the trend windows still
    disagree. post_frontal_recovery: ringing settling, or no ringing but the
    airmass is still young. quiescent: no front signature at all and nothing
    else is actively signaling. None (abstain) for any other combination
    rather than forcing a state that doesn't fit one of the four."""
    ringing_active = ringing_state is not None
    settling = ringing_active and ringing_state["settling"]
    if ringing_active and not settling and agreement_cat == "diverging":
        return "at_passage"
    if settling or (not ringing_active and age_bucket in ("at_trough", "recovering")):
        return "post_frontal_recovery"
    if not ringing_active and jerk_cat == "accelerating_fall":
        return "pre_frontal"
    if (not ringing_active and age_bucket in (None, "stale")
            and agreement_cat is not None and agreement_cat != "diverging"
            and jerk_cat == "steady"):
        return "quiescent"
    return None


def _build_conditionals(sorted_ts, by_ts, profile, troughs, confirmed_troughs):
    """One pass over history: conditional mean delta per (category, column,
    lead) for each of members 1, 2, 4, and 5 -- the standard bucket/average
    pattern every categorical model in this ensemble uses. Member 3 has no
    conditionals table; it forecasts pressure directly via its own decay model
    and reuses pressure_trend_cascade's continuous transfer functions."""
    all_cols = list(VARIABLES.values())
    jerk_accum, agree_accum, age_accum, phase_accum = {}, {}, {}, {}

    for ts in sorted_ts:
        row_now = by_ts[ts]
        now_pressure = row_now["station_pressure"]
        jerk_cat = _jerk_category(ts, now_pressure, by_ts, sorted_ts, profile)
        agreement_cat = _agreement_category(ts, now_pressure, by_ts, sorted_ts, profile)
        ringing_state = _ringing_state_at(ts, confirmed_troughs)
        age_bucket = _age_bucket_at(ts, troughs)
        phase_cat = _front_phase(jerk_cat, agreement_cat, ringing_state, age_bucket)

        for lead in LEAD_HOURS:
            ts_fut = _find_nearest_ts(sorted_ts, ts + lead * 3600, _FUTURE_LOOKUP_SEC)
            if ts_fut is None:
                continue
            row_fut = by_ts[ts_fut]
            for col in all_cols:
                v_now = row_now[col]
                v_fut = row_fut[col]
                if v_now is None or v_fut is None:
                    continue
                delta = v_fut - v_now
                if jerk_cat is not None:
                    jerk_accum.setdefault((jerk_cat, col, lead), []).append(delta)
                if agreement_cat is not None:
                    agree_accum.setdefault((agreement_cat, col, lead), []).append(delta)
                if age_bucket is not None:
                    age_accum.setdefault((age_bucket, col, lead), []).append(delta)
                if phase_cat is not None:
                    phase_accum.setdefault((phase_cat, col, lead), []).append(delta)

    def _finish(accum):
        return {k: sum(v) / len(v) for k, v in accum.items() if len(v) >= _MIN_SAMPLES}

    return _finish(jerk_accum), _finish(agree_accum), _finish(age_accum), _finish(phase_accum)


def run(obs, issued_at, *, conn_in, conn_out=None, weights=None, all_obs=None,
        member_history=None, default_matches=None):
    if all_obs is None:
        all_obs = db.tempest_obs_in_range(conn_in, 0, issued_at)

    profile = _pressure_tide.build_profile(all_obs)
    continuous_transfer_fns = _build_delta_transfer_fns(all_obs)

    by_ts = {row["timestamp"]: row for row in all_obs}
    sorted_ts = sorted(by_ts)

    troughs = _detect_troughs(sorted_ts, by_ts, profile)
    confirmed_troughs = _confirmed_ringing_troughs(troughs, by_ts, sorted_ts)
    jerk_conds, agree_conds, age_conds, phase_conds = _build_conditionals(
        sorted_ts, by_ts, profile, troughs, confirmed_troughs
    )

    obs_ts = obs["timestamp"]
    obs_pressure = obs["station_pressure"]

    live_jerk = _jerk_category(obs_ts, obs_pressure, by_ts, sorted_ts, profile)
    live_agreement = _agreement_category(obs_ts, obs_pressure, by_ts, sorted_ts, profile)
    live_ringing = _ringing_state_at(obs_ts, confirmed_troughs)
    live_age_bucket = _age_bucket_at(obs_ts, troughs)
    live_phase = _front_phase(live_jerk, live_agreement, live_ringing, live_age_bucket)

    live_baseline = (
        _ringing_baseline(live_ringing["trough_ts"], by_ts, sorted_ts, profile)
        if live_ringing is not None else None
    )
    obs_detided = _pressure_tide.detided(obs_pressure, obs_ts, profile)

    def _ringing_pressure(lead):
        if live_ringing is None or live_baseline is None or obs_detided is None:
            return None
        detided_pred = _ringing_decay(obs_detided, live_baseline, float(lead))
        return _pressure_tide.retided(detided_pred, obs_ts + lead * 3600, profile)

    def _member_value(mid, variable, col, lead):
        obs_val = obs[col]
        if mid == 1:
            if live_jerk is None or obs_val is None:
                return None
            delta = jerk_conds.get((live_jerk, col, lead))
            return obs_val + delta if delta is not None else None
        if mid == 2:
            if live_agreement is None or obs_val is None:
                return None
            delta = agree_conds.get((live_agreement, col, lead))
            return obs_val + delta if delta is not None else None
        if mid == 3:
            pred_pressure = _ringing_pressure(lead)
            if pred_pressure is None:
                return None
            if col == "station_pressure":
                return pred_pressure
            if obs_pressure is None or obs_val is None:
                return None
            tf = continuous_transfer_fns.get((col, lead))
            if tf is None:
                return None
            slope, intercept = tf
            return obs_val + slope * (pred_pressure - obs_pressure) + intercept
        if mid == 4:
            if live_age_bucket is None or obs_val is None:
                return None
            delta = age_conds.get((live_age_bucket, col, lead))
            return obs_val + delta if delta is not None else None
        # mid == 5: front_phase_state
        if live_phase is None or obs_val is None:
            return None
        delta = phase_conds.get((live_phase, col, lead))
        return obs_val + delta if delta is not None else None

    cell_confidences_by_var_lead = {
        (variable, lead): _confidence.member_confidences(
            member_history, default_matches, _ALL_MEMBER_IDS + [_SELF_CORRECTION_MEMBER], variable, lead
        )
        for variable in VARIABLES
        for lead in LEAD_HOURS
    }

    rows = []
    for mid, _name in _MEMBERS:
        for variable, col in VARIABLES.items():
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

    for variable, col in VARIABLES.items():
        for lead in LEAD_HOURS:
            valid_at = obs_ts + lead * 3600
            cell_confidences = cell_confidences_by_var_lead[(variable, lead)]
            valid_pairs = [
                (mid, v) for mid, _ in _MEMBERS
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

            # member 2 (trend_agreement) doubles as a confidence gate on this
            # model's own ensemble row -- full trust when the 1h/6h/24h
            # windows agree, damped when they diverge. See module docstring.
            if group_confidence is not None:
                gate = (
                    1.0 if live_agreement is not None and live_agreement != "diverging"
                    else _DIVERGING_CONFIDENCE_MULT
                )
                group_confidence *= gate

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
