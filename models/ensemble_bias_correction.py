# ensemble_bias_correction: learns a per-(variable, lead_hours) bias correction
# from barogram_ensemble's (model 100) own past scored history, then applies
# it to a same-cycle recomputation of model 100's raw blend.
#
# Runs one slot before model 100 each cycle (see barogram.py's _MODELS
# ordering), so it cannot read model 100's own output for this run yet --
# instead it recomputes the identical weighted blend from the same
# base-model inputs and the same tuned weights, via
# models.ensemble.blend_cells, rather than duplicating that math here.
#
# Since barogram_ensemble applies zero correction of its own, its historical
# member_id=0 rows already ARE the uncorrected blend to learn a correction
# against: bias is the mean signed error (value - observed) over model 100's
# own scored history for a cell, and this model's value is raw_blend - bias.
# It re-enters barogram_ensemble next cycle as an ordinary member (member_id
# 101, via sync_ensemble_members) -- tune's normal skill-score weighting
# decides how much to trust it, no special-casing if the correction turns
# out not to help.

import db
import models._confidence as _confidence
import models.ensemble as barogram_ensemble

MODEL_ID = 101
MODEL_NAME = "ensemble_bias_correction"
NEEDS_CONN_OUT = True
NEEDS_MATCH_HISTORY = True


def run(obs, issued_at: int, *, conn_out, member_history=None, default_matches=None) -> list[dict]:
    inputs = db.ensemble_inputs(conn_out, issued_at)
    if not inputs:
        return []

    weights = db.load_weights(conn_out, barogram_ensemble.MODEL_ID)
    blended = barogram_ensemble.blend_cells(inputs, weights)

    since = issued_at - _confidence._CONFIDENCE_WINDOW_DAYS * 86400
    rows = []
    for (variable, lead_hours), cell in blended.items():
        raw_value = cell["mean"]
        if raw_value is None:
            continue

        history = db.model_signed_error_history(
            conn_out, barogram_ensemble.MODEL_ID, 0, variable, lead_hours, since
        )
        # error is stored as value - observed (see score.py); subtracting the
        # mean corrects for the ensemble's typical historical overshoot/undershoot
        bias = sum(r["error"] for r in history) / len(history) if history else 0.0

        confidence = _confidence.confidence_for_cell(
            (member_history or {}).get(0, []), variable, lead_hours, default_matches or []
        )
        rows.append({
            "model_id": MODEL_ID,
            "model": MODEL_NAME,
            "member_id": 0,
            "issued_at": issued_at,
            "valid_at": cell["valid_at"],
            "lead_hours": lead_hours,
            "variable": variable,
            "value": raw_value - bias,
            "confidence": confidence,
        })

    return rows
