# persistence model: forecast = current observed value for all lead times
# simplest possible baseline; every other model is evaluated against this one
#
# member 1 (trend_persistence, added 2026-09-14) extends the flat baseline with the
# last hour's rate of change, decayed toward zero over the lead time rather than
# extrapolated forever. Motivated by the 2026-09-12 dry-airmass-intrusion miss: at
# issue time the obs were already cooling/drying, and flat persistence alone scored
# far better than the ensemble that run because it didn't fight that trend — this
# member tries to do slightly better than flat by leaning into the trend briefly
# before fading back to it.

import math

import db
import models._confidence as _confidence
from models._climo_weights import LEAD_HOURS

MODEL_ID = 1
MODEL_NAME = "persistence"
NEEDS_CONN_IN = True
NEEDS_MATCH_HISTORY = True

# barogram variable name -> tempest_obs column name
VARIABLES = {
    "temperature": "air_temp",
    "dewpoint": "dew_point",
    "pressure": "station_pressure",
}

_TREND_WINDOW_SEC = 3600   # 1h trailing slope
_TREND_LOOKUP_SEC = 600    # +/- 10 min tolerance
_TREND_DECAY_LAMBDA = 0.231  # per hour; ~3h half-life for the trend's contribution


def run(obs, issued_at: int, *, conn_in, member_history=None, default_matches=None) -> list[dict]:
    rows = []

    obs_1h_ago = db.nearest_tempest_obs(
        conn_in, obs["timestamp"] - _TREND_WINDOW_SEC, window_sec=_TREND_LOOKUP_SEC
    )

    for lead in LEAD_HOURS:
        valid_at = obs["timestamp"] + lead * 3600
        for variable, col in VARIABLES.items():
            now_val = obs[col]

            confidence_0 = _confidence.confidence_for_cell(
                (member_history or {}).get(0, []), variable, lead, default_matches or []
            )
            rows.append({
                "model_id": MODEL_ID,
                "model": MODEL_NAME,
                "member_id": 0,
                "issued_at": issued_at,
                "valid_at": valid_at,
                "lead_hours": lead,
                "variable": variable,
                "value": now_val,
                "confidence": confidence_0,
            })

            trend_val = None
            if now_val is not None and obs_1h_ago is not None and obs_1h_ago[col] is not None:
                rate = now_val - obs_1h_ago[col]  # per hour, window is 1h
                weight = math.exp(-_TREND_DECAY_LAMBDA * lead)
                trend_val = now_val + rate * lead * weight

            confidence_1 = _confidence.confidence_for_cell(
                (member_history or {}).get(1, []), variable, lead, default_matches or []
            )
            rows.append({
                "model_id": MODEL_ID,
                "model": MODEL_NAME,
                "member_id": 1,
                "issued_at": issued_at,
                "valid_at": valid_at,
                "lead_hours": lead,
                "variable": variable,
                "value": trend_val,
                "confidence": confidence_1,
            })

    return rows
