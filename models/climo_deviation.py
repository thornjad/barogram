# climo_deviation: weighted climo baseline + current anomaly, with static and
# exponentially-decaying variants as member groups.
# deviation = current_obs - climo_now (per member weight function).
# static group: forecast = future_baseline + deviation.
# decay groups: forecast = future_baseline + deviation * exp(-k * lead_hours).
# member_id=0 is the performance-weighted mean of all members when weights are
# available, otherwise equal-weighted.

import datetime as dt
import math
import statistics

import db
from models._climo_weights import LEAD_HOURS, MEMBERS as _BASE_MEMBERS, VARIABLES, weighted_mean as _weighted_mean

MODEL_ID = 4
MODEL_NAME = "climo_deviation"
NEEDS_CONN_IN = True
NEEDS_WEIGHTS = True

# (id_offset, decay_k or None for static, member name prefix)
_GROUPS = [
    (0,  None, "s"),    # static: member IDs 1-9
    (9,  0.03, "d03"),  # decay k=0.03: member IDs 10-18
    (18, 0.05, "d05"),  # decay k=0.05: member IDs 19-27
    (27, 0.10, "d10"),  # decay k=0.10: member IDs 28-36
]


def run(obs, issued_at: int, *, conn_in, weights=None) -> list[dict]:
    now = dt.datetime.fromtimestamp(obs["timestamp"])
    now_bucket = db.climo_bucket_obs(conn_in, now.month, now.hour)

    # compute deviation per (actual_mid, variable) at issue time
    deviations = {}
    for offset, k, prefix in _GROUPS:
        for mid, wname, wfn in _BASE_MEMBERS:
            actual_mid = offset + mid
            devs = {}
            for variable, col in VARIABLES.items():
                climo_now = _weighted_mean(now_bucket, col, issued_at, wfn) if now_bucket else None
                obs_val = obs[col]
                devs[variable] = (obs_val - climo_now) if (climo_now is not None and obs_val is not None) else None
            deviations[actual_mid] = devs

    rows = []
    for lead in LEAD_HOURS:
        valid_at = obs["timestamp"] + lead * 3600
        t = dt.datetime.fromtimestamp(valid_at)
        bucket = db.climo_bucket_obs(conn_in, t.month, t.hour)

        member_vals = {}

        for offset, k, prefix in _GROUPS:
            for mid, wname, wfn in _BASE_MEMBERS:
                actual_mid = offset + mid
                vals = {}
                for variable, col in VARIABLES.items():
                    future_base = _weighted_mean(bucket, col, issued_at, wfn) if bucket else None
                    dev = deviations[actual_mid][variable]
                    if future_base is not None and dev is not None:
                        if k is None:
                            vals[variable] = future_base + dev
                        else:
                            vals[variable] = future_base + dev * math.exp(-k * lead)
                    else:
                        vals[variable] = None
                    rows.append({
                        "model_id": MODEL_ID,
                        "model": MODEL_NAME,
                        "member_id": actual_mid,
                        "issued_at": issued_at,
                        "valid_at": valid_at,
                        "lead_hours": lead,
                        "variable": variable,
                        "value": vals[variable],
                    })
                member_vals[actual_mid] = vals

        # member_id=0: weighted mean + spread across all 36 members
        all_member_ids = [offset + mid for offset, k, prefix in _GROUPS for mid, _, _ in _BASE_MEMBERS]
        for variable in VARIABLES:
            valid_pairs = [
                (mid, member_vals[mid][variable])
                for mid in all_member_ids
                if member_vals[mid][variable] is not None
            ]
            if not valid_pairs:
                mean = None
            elif weights:
                w_pairs = [(weights.get((mid, variable, lead), None), v) for mid, v in valid_pairs]
                if any(w is None for w, _ in w_pairs):
                    # incomplete weights for this group — fall back to equal weighting
                    mean = sum(v for _, v in valid_pairs) / len(valid_pairs)
                else:
                    total_w = sum(w for w, _ in w_pairs)
                    mean = sum(w * v for w, v in w_pairs) / total_w
            else:
                mean = sum(v for _, v in valid_pairs) / len(valid_pairs)
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
            })

    return rows
