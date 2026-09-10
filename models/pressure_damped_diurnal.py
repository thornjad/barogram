# pressure_damped_diurnal: fork of airmass_diurnal that goes one step further on the
# pressure side. airmass_diurnal computes a 3h pressure tendency (dp_dt) only to
# nudge its clearness-projection member; here dp_dt (or a classification of it) is
# the primary signal damping the whole diurnal amplitude, on the theory that a
# falling barometer usually means increasing cloud cover suppressing daytime heating,
# and a rising one means clearer skies allowing fuller diurnal swing.
#
# member_id=0 is the performance-weighted mean of all members when weights are
# available, otherwise equal-weighted. temperature only adjusts by pressure trend;
# dewpoint gets base climatology + anchor with no adjustment (same convention as
# airmass_diurnal, which never adjusts dewpoint either).
#
# members:
#   1  linear_damp     amplitude scaled linearly by 3h pressure tendency rate
#   2  threshold_damp  binary: |24h-projected pressure change| past a threshold applies
#                       a fixed damping/boosting factor, else amplitude untouched
#   3  airmass_pressure_joint  clearness index x pressure-trend bucket joint state,
#                       each combination gets its own fixed multiplier

import datetime as dt
import statistics

import db
from models._utils import _sector
from models.airmass_diurnal import _hour_means, _interp_hm, clearness_index

MODEL_ID = 17
MODEL_NAME = "pressure_damped_diurnal"
NEEDS_CONN_IN = True
NEEDS_WEIGHTS = True
NEEDS_LOCATION = True

from models._climo_weights import LEAD_HOURS

# pressure intentionally omitted as an output — this model uses it only as a signal
VAR_COL = {
    "temperature": "air_temp",
    "dewpoint":    "dew_point",
}

_K_MEAN = 0.55
_LINEAR_DAMP_SENSITIVITY = 0.12   # amplitude multiplier change per hPa/h of 3h tendency
_THRESH_HPA = 3.0                # 24h-projected change past which threshold_damp fires
_THRESH_FACTOR_FALL = 0.7        # amplitude multiplier when falling past threshold
_THRESH_FACTOR_RISE = 1.2        # amplitude multiplier when rising past threshold
_JOINT_RISE = 0.3                # hPa/h boundary for the joint model's rising/falling buckets
_JOINT_MULT = {
    # (pressure_bucket, clear) -> multiplier;  pressure_bucket in {"falling","steady","rising"}
    ("falling", True):  0.75,
    ("falling", False): 0.85,
    ("steady",  True):  1.00,
    ("steady",  False): 0.95,
    ("rising",  True):  1.20,
    ("rising",  False): 1.05,
}

_MEMBER_NAMES = [
    (1, "linear_damp"),
    (2, "threshold_damp"),
    (3, "airmass_pressure_joint"),
]
_ALL_MEMBER_IDS = [mid for mid, _ in _MEMBER_NAMES]


def _local_hour_float(ts: int) -> float:
    d = dt.datetime.fromtimestamp(ts)
    return d.hour + d.minute / 60.0 + d.second / 3600.0


def _pressure_bucket(dp_dt: float) -> str:
    if dp_dt >= _JOINT_RISE:
        return "rising"
    if dp_dt <= -_JOINT_RISE:
        return "falling"
    return "steady"


def run(obs, issued_at: int, *, conn_in, weights=None, location=None) -> list[dict]:
    if location is None:
        location = db.tempest_station_location(conn_in)
    lat = location[0] if location else None

    t_now = _local_hour_float(obs["timestamp"])

    raw_30d = db.tempest_obs_in_range(conn_in, issued_at - 30 * 86400, issued_at)
    hm: dict[str, dict[int, float] | None] = {
        variable: _hour_means(raw_30d, col) for variable, col in VAR_COL.items()
    }
    t_hm = hm["temperature"]
    t_daily_mean = sum(t_hm.values()) / len(t_hm) if t_hm else None

    recent_3h = db.tempest_obs_in_range(conn_in, obs["timestamp"] - 3 * 3600, obs["timestamp"])
    p_vals = [r["station_pressure"] for r in recent_3h if r["station_pressure"] is not None]
    dp_dt = (p_vals[-1] - p_vals[0]) / 3.0 if len(p_vals) >= 2 else 0.0

    k = clearness_index(obs["solar_radiation"], lat, obs["timestamp"]) if lat else None
    k_adj = (k - _K_MEAN) if k is not None else None
    is_clear = (k is not None and k >= _K_MEAN)
    p_bucket = _pressure_bucket(dp_dt)

    rows = []
    for lead in LEAD_HOURS:
        valid_at = obs["timestamp"] + lead * 3600
        t_valid = _local_hour_float(valid_at)
        projected_change_24h = dp_dt * 24.0

        member_vals: dict[int, dict[str, float | None]] = {}

        for mid, _name in _MEMBER_NAMES:
            member_vals[mid] = {}
            for variable, col in VAR_COL.items():
                hm_v = hm[variable]
                if hm_v is None:
                    member_vals[mid][variable] = None
                    rows.append({
                        "model_id": MODEL_ID, "model": MODEL_NAME, "member_id": mid,
                        "issued_at": issued_at, "valid_at": valid_at,
                        "lead_hours": lead, "variable": variable, "value": None,
                    })
                    continue

                T_base_valid = _interp_hm(hm_v, t_valid)
                T_base_now = _interp_hm(hm_v, t_now)
                if T_base_valid is None or T_base_now is None:
                    member_vals[mid][variable] = None
                    rows.append({
                        "model_id": MODEL_ID, "model": MODEL_NAME, "member_id": mid,
                        "issued_at": issued_at, "valid_at": valid_at,
                        "lead_hours": lead, "variable": variable, "value": None,
                    })
                    continue

                obs_val = obs[col]
                anchor = (obs_val - T_base_now) if obs_val is not None else 0.0

                T_adj = 0.0
                if variable == "temperature" and t_daily_mean is not None:
                    dev = T_base_valid - t_daily_mean

                    if mid == 1:
                        factor = max(0.3, min(1.7, 1.0 + _LINEAR_DAMP_SENSITIVITY * dp_dt))
                        T_adj = dev * (factor - 1.0)
                    elif mid == 2:
                        if abs(projected_change_24h) >= _THRESH_HPA:
                            factor = _THRESH_FACTOR_FALL if dp_dt < 0 else _THRESH_FACTOR_RISE
                            T_adj = dev * (factor - 1.0)
                    elif mid == 3:
                        factor = _JOINT_MULT[(p_bucket, is_clear)]
                        T_adj = dev * (factor - 1.0)

                value = T_base_valid + anchor + T_adj
                member_vals[mid][variable] = value
                rows.append({
                    "model_id": MODEL_ID, "model": MODEL_NAME, "member_id": mid,
                    "issued_at": issued_at, "valid_at": valid_at,
                    "lead_hours": lead, "variable": variable, "value": value,
                })

        for variable in VAR_COL:
            valid_pairs = [
                (mid, member_vals[mid][variable])
                for mid in _ALL_MEMBER_IDS
                if member_vals[mid][variable] is not None
            ]
            if not valid_pairs:
                mean = None
            elif weights:
                w_pairs = [
                    (weights.get((mid, variable, lead, _sector(valid_at)), None), v)
                    for mid, v in valid_pairs
                ]
                if any(w is None for w, _ in w_pairs):
                    mean = sum(v for _, v in valid_pairs) / len(valid_pairs)
                else:
                    total_w = sum(w for w, _ in w_pairs)
                    mean = sum(w * v for w, v in w_pairs) / total_w
            else:
                mean = sum(v for _, v in valid_pairs) / len(valid_pairs)
            spread = (
                statistics.pstdev([v for _, v in valid_pairs])
                if len(valid_pairs) > 1 else None
            )
            rows.append({
                "model_id": MODEL_ID, "model": MODEL_NAME, "member_id": 0,
                "issued_at": issued_at, "valid_at": valid_at,
                "lead_hours": lead, "variable": variable,
                "value": mean, "spread": spread,
            })

    return rows
