# _pressure_sanity: shared clamp for the models that predict absolute
# station/sea-level pressure by extrapolation or transfer function, so a bad
# fit (runaway polynomial, degenerate slope/intercept) can't emit a
# non-physical pressure into the ensemble. Bound is the standard
# meteorological extreme range for sea-level pressure: 870 hPa (Typhoon Tip,
# 1979) to 1085 hPa (Agata, Siberia, 1968). A forecast outside that range is
# wrong regardless of station or lead time, so no tighter station-specific
# bound is used.

_MIN_HPA = 870.0
_MAX_HPA = 1085.0


def clamp(value: float | None) -> float | None:
    """Clamp a predicted pressure (hPa) into the physically plausible range.
    None passes through unchanged -- there's nothing to clamp."""
    if value is None:
        return value
    return min(_MAX_HPA, max(_MIN_HPA, value))
