# weighted climatological mean: historical (month, hour) bucket mean with
# recency weighting. each member uses a different weighting hypothesis.
# member_id=0 is the performance-weighted mean of all members when weights are
# available, otherwise equal-weighted.

import datetime as dt
import statistics
import time

import db
import models._confidence as _confidence
from models._climo_weights import LEAD_HOURS, MEMBERS as _MEMBERS, VARIABLES, weighted_mean as _weighted_mean
from models._utils import _sector

MODEL_ID = 3
MODEL_NAME = "weighted_climatological_mean"
NEEDS_CONN_IN = True
NEEDS_WEIGHTS = True
NEEDS_MATCH_HISTORY = True

_ALL_MEMBER_IDS = [mid for mid, _, _ in _MEMBERS]

def run(obs, issued_at: int, *, conn_in, weights=None, member_history=None,
        default_matches=None) -> list[dict]:
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
                member_history, default_matches, _ALL_MEMBER_IDS, variable, lead
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

        # member_id=0: weighted mean + spread across members 1-9
        for variable in VARIABLES:
            cell_confidences = variable_confidences[variable]
            valid_pairs = [
                (mid, member_vals[mid][variable])
                for mid, _, _ in _MEMBERS
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

    return rows
