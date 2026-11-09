# standard self-correction member: a model's own member_id=0 forecast plus a
# learned bias from that same model's own historical scored error at
# (variable, lead_hours) -- the same idea models/external_corrected.py uses,
# generalized into a shared helper so any model (the ensemble included) can add
# it as one more ordinary member instead of duplicating the bias math. It must be called with the model's OWN raw mix
# (the value it would output without self-correction), never another model's --
# the whole point is a model learning to counteract its own systematic error.
#
# The bias is learned from the model's stored raw mix (member RAW_MIX_ID) when
# the model has one, because its member 0 then holds the final value, which
# already contains this correction and would teach it back to itself. A model
# without a raw mix member still has a raw member 0 and learns from that.
#
# Gated behind _MIN_SAMPLES so a cell with too little scored history doesn't
# apply a wild correction learned from noise; those cells get value=None,
# same cold-start behavior every other member gets.

import db

_MIN_SAMPLES = 3
_WINDOW_DAYS = 60


def corrected_value(conn_out, model_id: int, variable: str, lead_hours: int,
                     raw_value: float | None, issued_at: int,
                     window_days: int = _WINDOW_DAYS) -> float | None:
    """Bias-corrected reading of model_id's own raw mix value for one
    (variable, lead_hours) cell.

    Returns None when raw_value is None, conn_out isn't available, or fewer
    than _MIN_SAMPLES scored rows exist yet for this cell -- there's nothing
    to learn a correction from yet.
    """
    if raw_value is None or conn_out is None:
        return None
    since = issued_at - window_days * 86400
    source_member = db.RAW_MIX_ID if db.has_raw_mix(conn_out, model_id) else 0
    history = db.model_signed_error_history(
        conn_out, model_id, source_member, variable, lead_hours, since
    )
    if len(history) < _MIN_SAMPLES:
        return None
    bias = sum(r["error"] for r in history) / len(history)
    return raw_value - bias
