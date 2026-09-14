# dewpoint_tendency: mirrors pressure_tendency's regression approach, but applied to
# dewpoint's own trailing slope instead of pressure. Forecasts dewpoint only.
#
# Motivated by the 2026-09-12 dry-airmass intrusion: the earliest the dewpoint itself
# showed anything was a peak-and-turn at 05:15 local, ~45 minutes before the 06:00
# forecast run — later than the pressure and wind-veer precursors (see
# pressure_trend_cascade and wind_veer_detector), but still nothing in the ensemble
# picked up on it, because no member extrapolated dewpoint's own recent trend. Every
# other dewpoint model either holds it flat (persistence-like) or anchors to a
# diurnal/climatological curve. This model's whole job is: what did the last hour or
# three actually do, and where is that headed — nothing more.
#
# Reuses pressure_tendency's polynomial-fit, exponential-weighting, and OU mean-
# reversion machinery directly — none of that numerics is pressure-specific.
#
# members:
#   1  linear_1h       degree-1 fit over a 1h window, mean-reverted extrapolation
#   2  linear_3h       degree-1 fit over a 3h window, mean-reverted extrapolation
#   3  linear_3h_hl45  same 3h window, recency-weighted with a 45-minute half-life

import statistics

import db
import models._confidence as _confidence
from models._climo_weights import LEAD_HOURS
from models._utils import _sector
from models.pressure_tendency import _apply_mean_reversion, _exp_weights, _poly_eval, _poly_fit

MODEL_ID = 22
MODEL_NAME = "dewpoint_tendency"
NEEDS_CONN_IN = True
NEEDS_WEIGHTS = True
NEEDS_ALL_OBS = True
NEEDS_MATCH_HISTORY = True

VARIABLE = "dewpoint"
_COL = "dew_point"
_REVERSION_LAMBDA = 0.10  # per hour, matches pressure_tendency's OU e-folding

# (member_id, name, window_hours, half_life_minutes or None)
_MEMBERS = [
    (1, "linear_1h", 1, None),
    (2, "linear_3h", 3, None),
    (3, "linear_3h_hl45", 3, 45),
]
_ALL_MEMBER_IDS = [mid for mid, *_ in _MEMBERS]


def run(obs, issued_at, *, conn_in, weights=None, all_obs=None,
        member_history=None, default_matches=None):
    if all_obs is None:
        all_obs = db.tempest_obs_in_range(conn_in, 0, issued_at)

    td_hist = [r[_COL] for r in all_obs if r[_COL] is not None]
    td_mean = sum(td_hist) / len(td_hist) if td_hist else None

    member_vals: dict[tuple[int, int], float | None] = {}

    for mid, _name, window_h, hl_min in _MEMBERS:
        start_ts = issued_at - window_h * 3600
        window_obs = [r for r in all_obs if r["timestamp"] >= start_ts and r[_COL] is not None]

        if len(window_obs) < 2:
            for lead in LEAD_HOURS:
                member_vals[(mid, lead)] = None
            continue

        t_vals = [(r["timestamp"] - issued_at) / 3600.0 for r in window_obs]
        td_vals = [r[_COL] for r in window_obs]
        w = _exp_weights(t_vals, hl_min / 60.0) if hl_min is not None else None
        coefs = _poly_fit(t_vals, td_vals, 1, w)

        if coefs is None:
            for lead in LEAD_HOURS:
                member_vals[(mid, lead)] = None
            continue

        for lead in LEAD_HOURS:
            raw = _poly_eval(coefs, float(lead))
            member_vals[(mid, lead)] = (
                _apply_mean_reversion(raw, td_mean, float(lead)) if td_mean is not None else raw
            )

    cell_confidences_by_lead = {
        lead: _confidence.member_confidences(
            member_history, default_matches, _ALL_MEMBER_IDS, VARIABLE, lead
        )
        for lead in LEAD_HOURS
    }

    rows = []
    for mid, _name, _window_h, _hl_min in _MEMBERS:
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

    return rows
