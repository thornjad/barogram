# barogram_ensemble: meta-ensemble combining member_id=0 from all base models.
# base models are discovered dynamically via the models table (type='base', id<100)
# so adding a new base model requires no changes here.

import math
import time

import db


def _sector(valid_at: int) -> int:
    h = time.localtime(valid_at).tm_hour
    if h < 6:  return 0
    if h < 12: return 1
    if h < 18: return 2
    return 3

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
    Produces one member row per base model plus a member_id=0 row (weighted
    mean + spread) per (variable, lead_hours).
    """
    db.sync_ensemble_members(conn_out)

    inputs = db.ensemble_inputs(conn_out, issued_at)
    if not inputs:
        return []

    # group by (variable, lead_hours) -> {model_id: (value, valid_at)}
    cells: dict = {}
    for row in inputs:
        if row["value"] is None:
            continue
        key = (row["variable"], row["lead_hours"])
        cells.setdefault(key, {})[row["model_id"]] = (row["value"], row["valid_at"])

    rows = []
    for (variable, lead_hours), model_values in cells.items():
        if not model_values:
            continue

        # one member row per contributing base model (member_id == base model_id)
        for model_id, (value, valid_at) in model_values.items():
            rows.append({
                "model_id": MODEL_ID,
                "model": MODEL_NAME,
                "member_id": model_id,
                "issued_at": issued_at,
                "valid_at": valid_at,
                "lead_hours": lead_hours,
                "variable": variable,
                "value": value,
            })

        # weighted mean; fall back to equal weight when weights dict is absent/sparse
        cell_valid_at = next(iter(model_values.values()))[1]
        sector = _sector(cell_valid_at)
        raw_w = {
            mid: (weights.get((mid, variable, lead_hours, sector), 1.0) if weights else 1.0)
            for mid in model_values
        }
        total_w = sum(raw_w.values())
        mean = sum(raw_w[mid] * v for mid, (v, _) in model_values.items()) / total_w

        vals = [v for v, _ in model_values.values()]
        spread = math.sqrt(sum((v - mean) ** 2 for v in vals) / len(vals))

        valid_at = next(iter(model_values.values()))[1]
        rows.append({
            "model_id": MODEL_ID,
            "model": MODEL_NAME,
            "member_id": 0,
            "issued_at": issued_at,
            "valid_at": valid_at,
            "lead_hours": lead_hours,
            "variable": variable,
            "value": mean,
            "spread": spread,
        })

    return rows
