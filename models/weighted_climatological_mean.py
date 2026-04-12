# weighted climatological mean: historical (month, hour) bucket mean with
# recency weighting. each member uses a different weighting hypothesis.
# member_id=0 is the equal-weighted mean of all members.

import datetime as dt
import statistics

import db
from models._climo_weights import LEAD_HOURS, MEMBERS as _MEMBERS, VARIABLES, weighted_mean as _weighted_mean

MODEL_ID = 3
MODEL_NAME = "weighted_climatological_mean"
NEEDS_CONN_IN = True


def run(obs, issued_at: int, *, conn_in) -> list[dict]:
    rows = []
    for lead in LEAD_HOURS:
        valid_at = obs["timestamp"] + lead * 3600
        t = dt.datetime.fromtimestamp(valid_at)
        bucket = db.climo_bucket_obs(conn_in, t.month, t.hour)

        member_vals = {}  # member_id -> {variable: value | None}

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
                })
            member_vals[mid] = vals

        # member_id=0: equal-weighted mean + spread across members 1-9
        for variable in VARIABLES:
            member_values = [
                member_vals[mid][variable]
                for mid, _, _ in _MEMBERS
                if member_vals[mid][variable] is not None
            ]
            mean = sum(member_values) / len(member_values) if member_values else None
            spread = statistics.pstdev(member_values) if len(member_values) > 1 else None
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
            })

    return rows
