# trend_climo_blend: temperature/dewpoint trend-persistence blended toward
# climatology, weight decaying over lead by three competing formulas (linear,
# exponential, logarithmic) at three rates each, crossed with seven ways of
# picking the trend's starting basis (own trailing linear slope at 3/6/12h,
# flat current-value persistence, or the mean of the K nearest analog days'
# current value). Every variant answers the same question -- how fast should
# "the trend continues" give way to "it reverts to normal" -- differently,
# and tune sorts out which combination actually earns its keep.
#
# forecast(lead) = weight(lead) * basis(lead) + (1 - weight(lead)) * climo_future(lead)
#
# basis(lead) is a linear extrapolation (intercept + slope*lead) for the
# trend-window members, and a lead-independent constant for the flat and
# analog members -- they don't claim to know a slope, just a starting level.
# No separate mean-reversion term (unlike pressure_tendency/dewpoint_tendency):
# the blend toward climo_future already does that job structurally.
#
# members:
#   1-9   trend{3,6,12}_{lin,exp,log}   own trailing-slope basis x blend family, moderate rate
#   10-12 flat_{lin,exp,log}            flat current-value basis x blend family, moderate rate
#   13-21 analogk{5,10,20}_{lin,exp,log} K-nearest-analog-mean basis x blend family, moderate rate
#   22-27 flat_{lin,exp,log}_{fast,slow} flat basis, same three families at faster/slower rates
#   28    climo_only                    weight forced to 0 -- pure climo, no blend
#   29    self_correction               standard self-correction member (models/_self_correction.py)

import datetime as dt
import math
import statistics

import db
import models._confidence as _confidence
import models._self_correction as _self_correction
import models._similarity as _similarity
from models._climo_weights import LEAD_HOURS
from models._utils import _sector
from models.pressure_tendency import _ols1

MODEL_ID = 30
MODEL_NAME = "trend_climo_blend"
NEEDS_CONN_IN = True
NEEDS_CONN_OUT = True
NEEDS_WEIGHTS = True
NEEDS_ALL_OBS = True
NEEDS_MATCH_HISTORY = True

VARIABLES = {
    "temperature": "air_temp",
    "dewpoint": "dew_point",
}

_ANALOG_FEATURES = ["air_temp", "dew_point", "station_pressure", "wind_avg"]

# (basis_key, kind, param) -- kind is "trend" (param=window_hours),
# "flat" (param=None), or "analog" (param=k)
_BASES = [
    ("trend3",    "trend",  3),
    ("trend6",    "trend",  6),
    ("trend12",   "trend",  12),
    ("flat",      "flat",   None),
    ("analogk5",  "analog", 5),
    ("analogk10", "analog", 10),
    ("analogk20", "analog", 20),
]
_BASIS_KIND = {key: kind for key, kind, _ in _BASES}


def _weight_linear(lead, horizon):
    return max(0.0, 1.0 - lead / horizon)


def _weight_exp(lead, k):
    return math.exp(-k * lead)


def _weight_log(lead, k):
    return 1.0 / (1.0 + k * math.log(1.0 + lead))


_DECAY_FNS = {"lin": _weight_linear, "exp": _weight_exp, "log": _weight_log}

# rate_tag -> family -> rate param (horizon hours for lin, per-hour k for exp/log)
_RATES = {
    "mod":  {"lin": 12.0, "exp": 0.15, "log": 0.4},
    "fast": {"lin": 6.0,  "exp": 0.35, "log": 1.0},
    "slow": {"lin": 24.0, "exp": 0.05, "log": 0.15},
}

# (member_id, name, basis_key, decay_family, rate_tag)
_MEMBERS = [
    (1,  "trend3_lin",    "trend3",    "lin", "mod"),
    (2,  "trend3_exp",    "trend3",    "exp", "mod"),
    (3,  "trend3_log",    "trend3",    "log", "mod"),
    (4,  "trend6_lin",    "trend6",    "lin", "mod"),
    (5,  "trend6_exp",    "trend6",    "exp", "mod"),
    (6,  "trend6_log",    "trend6",    "log", "mod"),
    (7,  "trend12_lin",   "trend12",   "lin", "mod"),
    (8,  "trend12_exp",   "trend12",   "exp", "mod"),
    (9,  "trend12_log",   "trend12",   "log", "mod"),
    (10, "flat_lin",      "flat",      "lin", "mod"),
    (11, "flat_exp",      "flat",      "exp", "mod"),
    (12, "flat_log",      "flat",      "log", "mod"),
    (13, "analogk5_lin",  "analogk5",  "lin", "mod"),
    (14, "analogk5_exp",  "analogk5",  "exp", "mod"),
    (15, "analogk5_log",  "analogk5",  "log", "mod"),
    (16, "analogk10_lin", "analogk10", "lin", "mod"),
    (17, "analogk10_exp", "analogk10", "exp", "mod"),
    (18, "analogk10_log", "analogk10", "log", "mod"),
    (19, "analogk20_lin", "analogk20", "lin", "mod"),
    (20, "analogk20_exp", "analogk20", "exp", "mod"),
    (21, "analogk20_log", "analogk20", "log", "mod"),
    (22, "flat_lin_fast", "flat",      "lin", "fast"),
    (23, "flat_lin_slow", "flat",      "lin", "slow"),
    (24, "flat_exp_fast", "flat",      "exp", "fast"),
    (25, "flat_exp_slow", "flat",      "exp", "slow"),
    (26, "flat_log_fast", "flat",      "log", "fast"),
    (27, "flat_log_slow", "flat",      "log", "slow"),
]
_CLIMO_ONLY_MEMBER = 28
_SELF_CORRECTION_MEMBER = 29
_ALL_MEMBER_IDS = [mid for mid, *_ in _MEMBERS] + [_CLIMO_ONLY_MEMBER]
_CONFIDENCE_MEMBER_IDS = _ALL_MEMBER_IDS + [_SELF_CORRECTION_MEMBER]

_MIN_TREND_POINTS = 2


def _trend_basis(all_obs, issued_at, window_h, col):
    """(slope, intercept) per-hour linear fit over the trailing window, or None."""
    start_ts = issued_at - window_h * 3600
    pairs = [
        ((r["timestamp"] - issued_at) / 3600.0, r[col])
        for r in all_obs
        if r["timestamp"] >= start_ts and r[col] is not None
    ]
    if len(pairs) < _MIN_TREND_POINTS:
        return None
    return _ols1([t for t, _ in pairs], [y for _, y in pairs])


def _analog_mean(sorted_candidates, k, col):
    valid = [c[col] for c in sorted_candidates[:k] if c[col] is not None]
    return sum(valid) / len(valid) if valid else None


def run(obs, issued_at, *, conn_in, conn_out=None, weights=None, all_obs=None,
        member_history=None, default_matches=None) -> list[dict]:
    if all_obs is None:
        all_obs = db.tempest_obs_in_range(conn_in, 0, issued_at)

    trend_fits = {}
    for basis_key, kind, window_h in _BASES:
        if kind != "trend":
            continue
        for variable, col in VARIABLES.items():
            trend_fits[(basis_key, variable)] = _trend_basis(all_obs, issued_at, window_h, col)

    candidates = db.analog_candidates(conn_in, obs["timestamp"])
    obs_vec = {col: obs[col] for col in _ANALOG_FEATURES}
    sigmas = _similarity.norm_sigmas(candidates, _ANALOG_FEATURES)
    cands_with_dist = [
        (_similarity.distance(obs_vec, cand, _ANALOG_FEATURES, sigmas), cand)
        for cand in candidates
    ]
    sorted_candidates = [
        cand for _, cand in _similarity.select_k_nearest(cands_with_dist, len(cands_with_dist))
    ]
    analog_means = {}
    for basis_key, kind, k in _BASES:
        if kind != "analog":
            continue
        for variable, col in VARIABLES.items():
            analog_means[(basis_key, variable)] = _analog_mean(sorted_candidates, k, col)

    def _basis_value(basis_key, kind, variable, col, lead):
        if kind == "trend":
            fit = trend_fits[(basis_key, variable)]
            if fit is None:
                return None
            slope, intercept = fit
            return intercept + slope * lead
        if kind == "flat":
            return obs[col]
        return analog_means[(basis_key, variable)]

    climo_cache: dict[tuple[int, int], dict] = {}

    def _climo(valid_at):
        t = dt.datetime.fromtimestamp(valid_at)
        key = (t.month, t.hour)
        if key not in climo_cache:
            climo_cache[key] = db.climo_bucket_means(conn_in, t.month, t.hour)
        return climo_cache[key]

    rows = []
    for variable, col in VARIABLES.items():
        cell_confidences_by_lead = {
            lead: _confidence.member_confidences(
                member_history, default_matches, _CONFIDENCE_MEMBER_IDS, variable, lead
            )
            for lead in LEAD_HOURS
        }

        for lead in LEAD_HOURS:
            valid_at = obs["timestamp"] + lead * 3600
            climo_val = _climo(valid_at).get(variable)
            cell_confidences = cell_confidences_by_lead[lead]

            member_vals = {}
            for mid, name, basis_key, family, rate_tag in _MEMBERS:
                kind = _BASIS_KIND[basis_key]
                basis_val = _basis_value(basis_key, kind, variable, col, lead)
                if basis_val is None or climo_val is None:
                    val = None
                else:
                    rate = _RATES[rate_tag][family]
                    w = _DECAY_FNS[family](lead, rate)
                    val = w * basis_val + (1.0 - w) * climo_val
                member_vals[mid] = val
                rows.append({
                    "model_id": MODEL_ID,
                    "model": MODEL_NAME,
                    "member_id": mid,
                    "issued_at": issued_at,
                    "valid_at": valid_at,
                    "lead_hours": lead,
                    "variable": variable,
                    "value": val,
                    "confidence": cell_confidences.get(mid),
                })

            member_vals[_CLIMO_ONLY_MEMBER] = climo_val
            rows.append({
                "model_id": MODEL_ID,
                "model": MODEL_NAME,
                "member_id": _CLIMO_ONLY_MEMBER,
                "issued_at": issued_at,
                "valid_at": valid_at,
                "lead_hours": lead,
                "variable": variable,
                "value": climo_val,
                "confidence": cell_confidences.get(_CLIMO_ONLY_MEMBER),
            })

            valid_pairs = [
                (mid, member_vals[mid])
                for mid in _ALL_MEMBER_IDS
                if member_vals[mid] is not None
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
