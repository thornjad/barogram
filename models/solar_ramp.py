# solar_ramp: forecasts temperature directly off how fast solar_radiation and the
# dewpoint depression are moving right now, instead of off pressure or a fixed
# diurnal curve. Forecasts temperature only.
#
# Motivated by the 2026-09-16 09:00 run: pressure_trend_cascade and wind_veer_detector
# both predicted a temp drop that morning because their transfer functions and
# classifiers pool history without regard to solar heating in progress (see those
# models' member 5/6 and 4/5 additions, same date). airmass_diurnal already has an
# extensive clearness-index member family (9-11, 13, 15-16) that scales a
# climatological diurnal curve by today's solar clearness — but it still missed this
# run by a wide margin, so the gap isn't "no solar awareness anywhere in the
# ensemble," it's specifically "no member regresses temperature's own recent rate
# directly against the rate solar/dewpoint signals are moving," the same kind of
# fixed-lag-free regression pressure_tendency does for pressure.
#
# members:
#   1  solar_temp_transfer      transfer function: temp delta over [t, t+lead] vs.
#                                solar_radiation's own trailing-1h delta, conditioned
#                                by time-of-day sector (same sector split as
#                                pressure_trend_cascade's sector_conditioned_extrap)
#   2  dewpoint_depression_ramp transfer function: temp delta vs. the trailing-1h
#                                change in dewpoint depression (temp - dewpoint) --
#                                a widening depression is a heating/mixing signal
#                                independent of the solar sensor
#   3  self_correction           standard self-correction member (models/_self_correction.py):
#                                member_id=0 minus this model's own learned historical bias

import statistics

import db
import models._confidence as _confidence
import models._self_correction as _self_correction
from models._climo_weights import LEAD_HOURS
from models._utils import _sector
from models.pressure_tendency import _find_nearest_ts, _ols1

MODEL_ID = 23
MODEL_NAME = "solar_ramp"
NEEDS_CONN_IN = True
NEEDS_CONN_OUT = True
NEEDS_WEIGHTS = True
NEEDS_ALL_OBS = True
NEEDS_MATCH_HISTORY = True

VARIABLE = "temperature"
_COL = "air_temp"

_RAMP_WINDOW_SEC = 3600     # trailing 1h, matches pressure_trend_cascade's ramp check
_LOOKUP_SEC = 600           # +/- 10 min, finding the trailing-window anchor obs
_FUTURE_LOOKUP_SEC = 900    # +/- 15 min
_MIN_SAMPLES = 3

_MEMBERS = [
    (1, "solar_temp_transfer"),
    (2, "dewpoint_depression_ramp"),
]
_ALL_MEMBER_IDS = [mid for mid, _ in _MEMBERS]
_SELF_CORRECTION_MEMBER = 3
_CONFIDENCE_MEMBER_IDS = _ALL_MEMBER_IDS + [_SELF_CORRECTION_MEMBER]


def _depression(row):
    if row["air_temp"] is None or row["dew_point"] is None:
        return None
    return row["air_temp"] - row["dew_point"]


def _predictor(mid, row_now, row_past):
    if row_now is None or row_past is None:
        return None
    if mid == 1:
        s_now, s_past = row_now.get("solar_radiation"), row_past.get("solar_radiation")
        if s_now is None or s_past is None:
            return None
        return s_now - s_past
    # mid == 2
    d_now, d_past = _depression(row_now), _depression(row_past)
    if d_now is None or d_past is None:
        return None
    return d_now - d_past


def _build_transfer_fns(all_obs):
    """member 1: {(lead, sector): (slope, intercept)}, keyed by the sector of the
    observation the ramp is measured from. member 2: {lead: (slope, intercept)},
    pooled -- a widening dewpoint depression is inherently a daytime-heating signal,
    so it isn't confounded by time-of-day the way a bare pressure delta is."""
    by_ts = {row["timestamp"]: row for row in all_obs}
    sorted_ts = sorted(by_ts)

    result_1: dict[tuple[int, int], tuple[float, float]] = {}
    result_2: dict[int, tuple[float, float]] = {}

    for lead in LEAD_HOURS:
        lead_sec = lead * 3600
        by_sector: dict[int, tuple[list[float], list[float]]] = {}
        depression_xs, depression_ys = [], []

        for ts in sorted_ts:
            row_now = by_ts[ts]
            ts_past = _find_nearest_ts(sorted_ts, ts - _RAMP_WINDOW_SEC, _LOOKUP_SEC)
            row_past = by_ts.get(ts_past) if ts_past is not None else None
            if row_past is None:
                continue
            ts_fut = _find_nearest_ts(sorted_ts, ts + lead_sec, _FUTURE_LOOKUP_SEC)
            if ts_fut is None:
                continue
            row_fut = by_ts[ts_fut]
            t_now, t_fut = row_now[_COL], row_fut[_COL]
            if t_now is None or t_fut is None:
                continue
            delta_t = t_fut - t_now

            x1 = _predictor(1, row_now, row_past)
            if x1 is not None:
                sector = _sector(ts)
                xs, ys = by_sector.setdefault(sector, ([], []))
                xs.append(x1)
                ys.append(delta_t)

            x2 = _predictor(2, row_now, row_past)
            if x2 is not None:
                depression_xs.append(x2)
                depression_ys.append(delta_t)

        for sector, (xs, ys) in by_sector.items():
            if len(xs) >= _MIN_SAMPLES:
                tf = _ols1(xs, ys)
                if tf is not None:
                    result_1[(lead, sector)] = tf

        if len(depression_xs) >= _MIN_SAMPLES:
            tf = _ols1(depression_xs, depression_ys)
            if tf is not None:
                result_2[lead] = tf

    return result_1, result_2


def run(obs, issued_at, *, conn_in, conn_out=None, weights=None, all_obs=None,
        member_history=None, default_matches=None):
    if all_obs is None:
        all_obs = db.tempest_obs_in_range(conn_in, 0, issued_at)

    tf_solar, tf_depression = _build_transfer_fns(all_obs)

    by_ts = {row["timestamp"]: row for row in all_obs}
    sorted_ts = sorted(by_ts)
    ts_past = _find_nearest_ts(sorted_ts, obs["timestamp"] - _RAMP_WINDOW_SEC, _LOOKUP_SEC)
    row_past = by_ts.get(ts_past) if ts_past is not None else None
    obs_sector = _sector(obs["timestamp"])

    x1 = _predictor(1, obs, row_past)
    x2 = _predictor(2, obs, row_past)
    obs_val = obs[_COL]

    member_vals: dict[tuple[int, int], float | None] = {}
    for lead in LEAD_HOURS:
        if obs_val is None or x1 is None:
            member_vals[(1, lead)] = None
        else:
            tf = tf_solar.get((lead, obs_sector))
            member_vals[(1, lead)] = obs_val + tf[0] * x1 + tf[1] if tf is not None else None

        if obs_val is None or x2 is None:
            member_vals[(2, lead)] = None
        else:
            tf = tf_depression.get(lead)
            member_vals[(2, lead)] = obs_val + tf[0] * x2 + tf[1] if tf is not None else None

    cell_confidences_by_lead = {
        lead: _confidence.member_confidences(
            member_history, default_matches, _CONFIDENCE_MEMBER_IDS, VARIABLE, lead
        )
        for lead in LEAD_HOURS
    }

    rows = []
    for mid, _name in _MEMBERS:
        for lead in LEAD_HOURS:
            rows.append({
                "model_id": MODEL_ID,
                "model": MODEL_NAME,
                "member_id": mid,
                "issued_at": issued_at,
                "valid_at": obs["timestamp"] + lead * 3600,
                "lead_hours": lead,
                "variable": VARIABLE,
                "value": member_vals[(mid, lead)],
                "confidence": cell_confidences_by_lead[lead].get(mid),
            })

    for lead in LEAD_HOURS:
        valid_at = obs["timestamp"] + lead * 3600
        cell_confidences = cell_confidences_by_lead[lead]
        valid_pairs = [
            (mid, member_vals[(mid, lead)])
            for mid in _ALL_MEMBER_IDS
            if member_vals[(mid, lead)] is not None
        ]
        if not valid_pairs:
            mean, group_confidence = None, None
        elif weights:
            member_weights = {
                mid: weights.get((mid, VARIABLE, lead, _sector(valid_at)))
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

        spread = statistics.pstdev([v for _, v in valid_pairs]) if len(valid_pairs) > 1 else None

        rows.append({
            "model_id": MODEL_ID,
            "model": MODEL_NAME,
            "member_id": 0,
            "issued_at": issued_at,
            "valid_at": valid_at,
            "lead_hours": lead,
            "variable": VARIABLE,
            "value": mean,
            "spread": spread,
            "confidence": group_confidence,
        })

        corrected = _self_correction.corrected_value(
            conn_out, MODEL_ID, VARIABLE, lead, mean, issued_at
        )
        rows.append({
            "model_id": MODEL_ID,
            "model": MODEL_NAME,
            "member_id": _SELF_CORRECTION_MEMBER,
            "issued_at": issued_at,
            "valid_at": valid_at,
            "lead_hours": lead,
            "variable": VARIABLE,
            "value": corrected,
            "confidence": cell_confidences.get(_SELF_CORRECTION_MEMBER),
        })

    return rows
