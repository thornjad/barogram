# weighted climatological mean: historical (month, hour) bucket mean with
# recency weighting. each member uses a different weighting hypothesis.
# member_id=0 is the performance-weighted mean of all members when weights are
# available, otherwise equal-weighted.
#
# members 10-12 (added 2026-09-22) step outside the recency-weighting family:
#   10  sector_climatology     bucket mean further conditioned on the current
#                              8-point wind sector (NW cold/dry vs SE warm/humid
#                              advection), falling back to the plain bucket mean
#                              when the sector-filtered pool is too thin
#   11  harmonic_regression    low-order Fourier fit (2 annual + 2 diurnal
#                              harmonics) to the full air_temp history, projected
#                              forward -- a smooth alternative to the (month, hour)
#                              bucket for temperature only; dewpoint and pressure
#                              are left unfit since the signal was only proposed
#                              for air_temp
#   12  continuous_doy_phase   bucket mean with the hard month match replaced by
#                              a von Mises (circular Gaussian) kernel over day-of-
#                              year phase, avoiding the step artifact discrete
#                              month buckets create at month boundaries
#   13  self_correction        standard self-correction member
#                              (models/_self_correction.py) -- member_id=0 minus
#                              this model's own learned historical bias

import datetime as dt
import math
import statistics

import numpy as np

import db
import models._confidence as _confidence
import models._self_correction as _self_correction
from models._climo_weights import LEAD_HOURS, MEMBERS as _MEMBERS, VARIABLES, weighted_mean as _weighted_mean
from models._utils import _sector

MODEL_ID = 3
MODEL_NAME = "weighted_climatological_mean"
NEEDS_CONN_IN = True
NEEDS_CONN_OUT = True
NEEDS_WEIGHTS = True
NEEDS_ALL_OBS = True
NEEDS_MATCH_HISTORY = True

_ALL_MEMBER_IDS = [mid for mid, _, _ in _MEMBERS] + [10, 11, 12]
_SELF_CORRECTION_MEMBER = 13
_CONFIDENCE_MEMBER_IDS = _ALL_MEMBER_IDS + [_SELF_CORRECTION_MEMBER]

# member 10: sector climatology
_MIN_SECTOR_SAMPLES = 8

def _wind_sector8(degrees: float) -> int:
    """8-point compass sector for a wind direction in degrees (0=N, clockwise).
    Same convention as diurnal_curve.py's own local copy."""
    return int((degrees + 22.5) / 45) % 8

def _sector_climatology_mean(bucket, col: str, live_sector: int | None):
    if live_sector is not None:
        filtered = [
            r[col] for r in bucket
            if r[col] is not None and r["wind_direction"] is not None
            and _wind_sector8(r["wind_direction"]) == live_sector
        ]
        if len(filtered) >= _MIN_SECTOR_SAMPLES:
            return sum(filtered) / len(filtered)
    all_vals = [r[col] for r in bucket if r[col] is not None]
    return sum(all_vals) / len(all_vals) if all_vals else None

# member 11: harmonic regression (temperature only, per the proposal)
_ANNUAL_HARMONICS = 2
_DIURNAL_HARMONICS = 2
_HARMONIC_MIN_OBS = 60  # enough to constrain 9 coefficients without overfitting

def _day_of_year_frac(ts: int) -> float:
    d = dt.datetime.fromtimestamp(ts)
    return (d.timetuple().tm_yday - 1 + d.hour / 24 + d.minute / 1440) / 365.25

def _harmonic_terms(ts: int) -> list[float]:
    TWO_PI = 2 * math.pi
    doy = _day_of_year_frac(ts)
    d = dt.datetime.fromtimestamp(ts)
    hour = d.hour + d.minute / 60
    terms = [1.0]
    for k in range(1, _ANNUAL_HARMONICS + 1):
        terms.append(math.sin(TWO_PI * k * doy))
        terms.append(math.cos(TWO_PI * k * doy))
    for j in range(1, _DIURNAL_HARMONICS + 1):
        terms.append(math.sin(TWO_PI * j * hour / 24))
        terms.append(math.cos(TWO_PI * j * hour / 24))
    return terms

def _fit_harmonic(all_obs, col: str):
    rows = [(r["timestamp"], r[col]) for r in all_obs if r[col] is not None]
    if len(rows) < _HARMONIC_MIN_OBS:
        return None
    X = np.array([_harmonic_terms(ts) for ts, _ in rows])
    y = np.array([v for _, v in rows])
    coeffs = np.linalg.lstsq(X, y, rcond=None)[0]
    if not np.all(np.isfinite(coeffs)):
        return None
    return coeffs

def _eval_harmonic(coeffs, ts: int) -> float:
    return float(np.dot(coeffs, _harmonic_terms(ts)))

# member 12: continuous day-of-year phase
_SEASONAL_KAPPA = 8.0  # von Mises concentration; ~20-day effective smoothing radius

def _doy_theta(ts: int) -> float:
    return 2 * math.pi * _day_of_year_frac(ts)

def _seasonal_weighted_mean(all_obs, col: str, target_hour: int, target_theta: float):
    total_w = 0.0
    total_wv = 0.0
    for row in all_obs:
        v = row[col]
        if v is None:
            continue
        if dt.datetime.fromtimestamp(row["timestamp"]).hour != target_hour:
            continue
        w = math.exp(_SEASONAL_KAPPA * (math.cos(target_theta - _doy_theta(row["timestamp"])) - 1.0))
        total_w += w
        total_wv += w * v
    return total_wv / total_w if total_w > 0 else None

def run(obs, issued_at: int, *, conn_in, conn_out=None, weights=None, all_obs=None, member_history=None,
        default_matches=None) -> list[dict]:
    if all_obs is None:
        all_obs = db.tempest_obs_in_range(conn_in, 0, issued_at)

    live_sector = (
        _wind_sector8(obs["wind_direction"]) if obs.get("wind_direction") is not None else None
    )
    harmonic_fits = {
        variable: (_fit_harmonic(all_obs, col) if variable == "temperature" else None)
        for variable, col in VARIABLES.items()
    }

    rows = []
    climo_cache: dict[tuple[int, int], list] = {}
    for lead in LEAD_HOURS:
        valid_at = obs["timestamp"] + lead * 3600
        t = dt.datetime.fromtimestamp(valid_at)
        key = (t.month, t.hour)
        if key not in climo_cache:
            climo_cache[key] = db.climo_bucket_obs(conn_in, t.month, t.hour)
        bucket = climo_cache[key]

        member_vals = {}  # member_id -> {variable: value | None}

        variable_confidences = {
            variable: _confidence.member_confidences(
                member_history, default_matches, _CONFIDENCE_MEMBER_IDS, variable, lead
            )
            for variable in VARIABLES
        }

        for mid, name, weight_fn in _MEMBERS:
            vals = {}
            for variable, col in VARIABLES.items():
                vals[variable] = _weighted_mean(bucket, col, issued_at, weight_fn) if bucket else None
                rows.append({
                    "model_id": MODEL_ID,
                    "model": MODEL_NAME,
                    "member_id": mid,
                    "issued_at": issued_at,
                    "valid_at": valid_at,
                    "lead_hours": lead,
                    "variable": variable,
                    "value": vals[variable],
                    "confidence": variable_confidences[variable].get(mid),
                })
            member_vals[mid] = vals

        sector_vals = {}
        for variable, col in VARIABLES.items():
            sector_vals[variable] = _sector_climatology_mean(bucket, col, live_sector) if bucket else None
            rows.append({
                "model_id": MODEL_ID,
                "model": MODEL_NAME,
                "member_id": 10,
                "issued_at": issued_at,
                "valid_at": valid_at,
                "lead_hours": lead,
                "variable": variable,
                "value": sector_vals[variable],
                "confidence": variable_confidences[variable].get(10),
            })
        member_vals[10] = sector_vals

        harmonic_vals = {}
        for variable, col in VARIABLES.items():
            fit = harmonic_fits[variable]
            harmonic_vals[variable] = _eval_harmonic(fit, valid_at) if fit is not None else None
            rows.append({
                "model_id": MODEL_ID,
                "model": MODEL_NAME,
                "member_id": 11,
                "issued_at": issued_at,
                "valid_at": valid_at,
                "lead_hours": lead,
                "variable": variable,
                "value": harmonic_vals[variable],
                "confidence": variable_confidences[variable].get(11),
            })
        member_vals[11] = harmonic_vals

        target_theta = _doy_theta(valid_at)
        phase_vals = {}
        for variable, col in VARIABLES.items():
            phase_vals[variable] = _seasonal_weighted_mean(all_obs, col, t.hour, target_theta)
            rows.append({
                "model_id": MODEL_ID,
                "model": MODEL_NAME,
                "member_id": 12,
                "issued_at": issued_at,
                "valid_at": valid_at,
                "lead_hours": lead,
                "variable": variable,
                "value": phase_vals[variable],
                "confidence": variable_confidences[variable].get(12),
            })
        member_vals[12] = phase_vals

        # member_id=0: weighted mean + spread across all members
        for variable in VARIABLES:
            cell_confidences = variable_confidences[variable]
            valid_pairs = [
                (mid, member_vals[mid][variable])
                for mid in _ALL_MEMBER_IDS
                if member_vals[mid][variable] is not None
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
            spread = statistics.pstdev([v for _, v in valid_pairs]) if len(valid_pairs) > 1 else None
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
