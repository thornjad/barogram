# standard self-correction member: a model's own member_id=0 forecast plus a
# learned bias from that same model's own historical scored error at
# (variable, lead_hours) -- the same idea models/external_corrected.py and
# models/ensemble_bias_correction.py already use, generalized into a shared
# helper so any base model can add it as one more ordinary member instead of
# duplicating the bias math. It must be called with the model's OWN
# member_id=0, never another model's -- the whole point is a model learning
# to counteract its own systematic error.
#
# Gated behind _MIN_SAMPLES so a cell with too little scored history doesn't
# apply a wild correction learned from noise; those cells get value=None,
# same cold-start behavior every other member gets.

import db

_MIN_SAMPLES = 3
_WINDOW_DAYS = 60


def corrected_value(conn_out, model_id: int, variable: str, lead_hours: int,
                     raw_value: float | None, issued_at: int) -> float | None:
    """Bias-corrected reading of model_id's own member_id=0 value for one
    (variable, lead_hours) cell.

    Returns None when raw_value is None, conn_out isn't available, or fewer
    than _MIN_SAMPLES scored rows exist yet for this cell -- there's nothing
    to learn a correction from yet.
    """
    if raw_value is None or conn_out is None:
        return None
    since = issued_at - _WINDOW_DAYS * 86400
    history = db.model_signed_error_history(conn_out, model_id, 0, variable, lead_hours, since)
    if len(history) < _MIN_SAMPLES:
        return None
    bias = sum(r["error"] for r in history) / len(history)
    return raw_value - bias
