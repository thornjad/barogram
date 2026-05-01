import math

LEAD_HOURS = [6, 12, 18, 24]

VARIABLES = {
    "temperature": "air_temp",
    "dewpoint": "dew_point",
    "pressure": "station_pressure",
}

# (member_id, name, weight_fn(age_days) -> float)
# tier members assign the highest applicable tier weight
MEMBERS = [
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


def weighted_mean(obs_rows, col, issued_at, weight_fn):
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
