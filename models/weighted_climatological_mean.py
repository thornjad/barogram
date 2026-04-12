# weighted climatological mean: historical (month, hour) bucket mean with
# recency weighting. each member uses a different weighting hypothesis.
# member_id=0 is the equal-weighted mean of all members.

import datetime as dt
import math
import statistics

import db

MODEL_ID = 3
MODEL_NAME = "weighted_climatological_mean"
NEEDS_CONN_IN = True

LEAD_HOURS = [6, 12, 18, 24]

VARIABLES = {
    "temperature": "air_temp",
    "humidity": "relative_humidity",
    "pressure": "station_pressure",
    "wind_speed": "wind_avg",
}

# (member_id, name, weight_fn(age_days) -> float)
# tier members assign the highest applicable tier weight
_MEMBERS = [
    (1, "today-only",       lambda a: 20.0 if a < 1 else 1.0),
    (2, "week-only",        lambda a: 7.0 if a < 7 else 1.0),
    (3, "month-only",       lambda a: 7.0 if a < 30 else 1.0),
    (4, "week+month",       lambda a: 20.0 if a < 7 else (7.0 if a < 30 else 1.0)),
    (5, "today+week+month", lambda a: 50.0 if a < 1 else (20.0 if a < 7 else (7.0 if a < 30 else 1.0))),
    (6, "exp-steep",        lambda a: math.exp(-0.50 * a)),
    (7, "exp-fast",         lambda a: math.exp(-0.20 * a)),
    (8, "exp-moderate",     lambda a: math.exp(-0.10 * a)),
    (9, "exp-gentle",       lambda a: math.exp(-0.03 * a)),
]


def _weighted_mean(obs_rows, col, issued_at, weight_fn):
    total_w = 0.0
    total_wv = 0.0
    for row in obs_rows:
        v = row[col]
        if v is None:
            continue
        age = (issued_at - row["timestamp"]) / 86400.0
        w = weight_fn(age)
        total_w += w
        total_wv += w * v
    return total_wv / total_w if total_w > 0 else None


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
