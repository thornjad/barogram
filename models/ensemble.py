# barogram_ensemble: meta-ensemble combining member_id=0 from all base models.
# base models are discovered dynamically via the models table (type='base', id<100)
# so adding a new base model requires no changes here.

import math

import db
import models._confidence as _confidence
from models._utils import _sector

MODEL_ID = 100
MODEL_NAME = "barogram_ensemble"
MODEL_TYPE = "ensemble"
NEEDS_CONN_OUT = True
NEEDS_WEIGHTS = True


def run(obs, issued_at: int, *, conn_out, weights=None) -> list[dict]:
    """Combine member_id=0 forecasts from all base models into one ensemble.

    Each contributing base model becomes one member of this ensemble. weights
    is a dict keyed by (member_id, variable, lead_hours, sector); sector is
    derived from valid_at hour. Absent keys fall back to equal weighting.
    Produces one member row per base model plus a member_id=0 row (weighted,
    confidence-adjusted mean + spread) per (variable, lead_hours). Confidence
    comes straight from ensemble_inputs, since each base model's own
    member_id=0 row already carries its own confidence -- no separate
    NEEDS_MATCH_HISTORY computation needed here.
    """
    db.sync_ensemble_members(conn_out)

    inputs = db.ensemble_inputs(conn_out, issued_at)
    if not inputs:
        return []

    # group by (variable, lead_hours) -> {model_id: (value, valid_at, confidence)}
    cells: dict = {}
    for row in inputs:
        if row["value"] is None:
            continue
        key = (row["variable"], row["lead_hours"])
        cells.setdefault(key, {})[row["model_id"]] = (row["value"], row["valid_at"], row["confidence"])

    rows = []
    for (variable, lead_hours), model_values in cells.items():
        if not model_values:
            continue

        # one member row per contributing base model (member_id == base model_id)
        for model_id, (value, valid_at, confidence) in model_values.items():
            rows.append({
                "model_id": MODEL_ID,
                "model": MODEL_NAME,
                "member_id": model_id,
                "issued_at": issued_at,
                "valid_at": valid_at,
                "lead_hours": lead_hours,
                "variable": variable,
                "value": value,
                "confidence": confidence,
            })

        cell_valid_at = next(iter(model_values.values()))[1]
        sector = _sector(cell_valid_at)
        valid_pairs = [(mid, v) for mid, (v, _, _) in model_values.items()]
        member_weights = {
            mid: (weights.get((mid, variable, lead_hours, sector)) if weights else None)
            for mid in model_values
        }
        confidences = {mid: c for mid, (_, _, c) in model_values.items()}
        mean, group_confidence = _confidence.combine_pattern(valid_pairs, member_weights, confidences)

        vals = [v for v, _, _ in model_values.values()]
        spread = math.sqrt(sum((v - mean) ** 2 for v in vals) / len(vals))

        rows.append({
            "model_id": MODEL_ID,
            "model": MODEL_NAME,
            "member_id": 0,
            "issued_at": issued_at,
            "valid_at": cell_valid_at,
            "lead_hours": lead_hours,
            "variable": variable,
            "value": mean,
            "spread": spread,
            "confidence": group_confidence,
        })

    return rows
