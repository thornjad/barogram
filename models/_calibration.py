# models/_calibration.py
#
# Turns a member's evidence score into a confidence that means what it says:
# the measured rate at which forecasts with that kind of evidence stayed inside
# the hit tolerance, over many separate days. See docs/confidence.md.

MIN_DAYS = 14                # a cell needs this many distinct issue-days before it speaks
AGGREGATE_N_BIN = 3          # n_bin value reserved for aggregate (member 0) rows
AGGREGATE_FORM_BIN = 1
_AGGREGATE_OFFSET = 100      # conf_bin of an aggregate row is 100 + its score bin
_N_SCORE_BINS = 10

# error band that counts as a hit, same at every lead: an error of 10 degrees is
# 10 degrees whether it was forecast 1 hour or 24 hours out. 1.11 C is 2 F.
_TOLERANCE = {"temperature": 1.11, "dewpoint": 1.11, "pressure": 1.5}


def tolerance(variable: str, lead_hours: int) -> float:
    return _TOLERANCE[variable]


def is_hit(variable: str, lead_hours: int, mae: float) -> bool:
    return mae <= tolerance(variable, lead_hours)


def n_bin(n_ev: int) -> int:
    """How much matched-moment evidence stands behind a score: 1-3, 4-10, 11+."""
    if n_ev <= 3:
        return 0
    if n_ev <= 10:
        return 1
    return 2


def score_bin(score: float) -> int:
    return max(0, min(int(score * _N_SCORE_BINS), _N_SCORE_BINS - 1))


def pack(n_bin_: int, form_bin_: int, score_bin_: int) -> int:
    """One small integer holding a leaf row's evidence score, example count bin
    and recent-form bin, stored on the forecast row (0 to 89)."""
    return (n_bin_ * 3 + form_bin_) * 10 + score_bin_


def aggregate_bin(score_bin_: int) -> int:
    return _AGGREGATE_OFFSET + score_bin_


def unpack(conf_bin: int) -> tuple[int, int, int]:
    if conf_bin >= _AGGREGATE_OFFSET:
        return AGGREGATE_N_BIN, AGGREGATE_FORM_BIN, conf_bin - _AGGREGATE_OFFSET
    score_bin_ = conf_bin % 10
    n_form = conf_bin // 10
    return n_form // 3, n_form % 3, score_bin_


def _value(days: int, p_hat: float) -> float | None:
    """A cell's rate, or None while it has fewer than MIN_DAYS distinct days.
    The effective sample size is days, not rows: many members share one weather
    day, so a single day with thousands of rows is still one day of evidence."""
    if days < MIN_DAYS:
        return None
    return min(1.0, max(0.0, (p_hat * days + 1.0) / (days + 2.0)))


def _pav(points: list[tuple[int, float, int]]) -> dict[int, float]:
    """Pool-adjacent-violators over (bin, value, weight) points in ascending bin
    order: a bin whose value is lower than the one below it is merged with it
    (weighted mean), so output never decreases as the score rises and a thin high
    estimate cannot lift the bins above it."""
    blocks: list[list] = []  # [sum value*weight, sum weight, [bins]]
    for b, v, w in points:
        blocks.append([v * w, w, [b]])
        while len(blocks) > 1 and blocks[-2][0] / blocks[-2][1] > blocks[-1][0] / blocks[-1][1]:
            last = blocks.pop()
            blocks[-1][0] += last[0]
            blocks[-1][1] += last[1]
            blocks[-1][2] += last[2]
    out = {}
    for sum_vw, sum_w, bins in blocks:
        for b in bins:
            out[b] = sum_vw / sum_w
    return out


def _series(cells: dict[int, tuple[int, float]]) -> list[float | None] | None:
    """Ten-entry score-bin vector from active cells only: PAV over them, then each
    inactive bin takes the nearest lower active value. Bins below the lowest
    active one stay None. Returns None when no cell is active."""
    points = []
    for b in range(_N_SCORE_BINS):
        cell = cells.get(b)
        if cell is None:
            continue
        v = _value(*cell)
        if v is not None:
            points.append((b, v, cell[0]))
    if not points:
        return None
    pooled = _pav(points)
    vec: list[float | None] = [None] * _N_SCORE_BINS
    last = None
    for b in range(_N_SCORE_BINS):
        if b in pooled:
            last = pooled[b]
        vec[b] = last
    return vec


class CalibrationTable:
    """fine: {(variable, lead_hours, n_bin, form_bin, score_bin): (days, p_hat)}
    coarse: {(variable, lead_hours, score_bin): (days, p_hat)}, summed over the leaf
    n and form bins. Aggregate cells (n_bin 3) never use the coarse series."""

    def __init__(self, fine: dict, coarse: dict):
        self._fine = fine
        self._coarse = coarse
        self._vectors: dict[tuple, list[float] | None] = {}

    def _vector(self, variable, lead_hours, n_bin_, form_bin_):
        key = (variable, lead_hours, n_bin_, form_bin_)
        if key in self._vectors:
            return self._vectors[key]
        fine_cells = {b: self._fine[(*key, b)] for b in range(_N_SCORE_BINS) if (*key, b) in self._fine}
        fine_vec = _series(fine_cells)
        coarse_vec = None
        if n_bin_ != AGGREGATE_N_BIN:
            coarse_cells = {b: self._coarse[(variable, lead_hours, b)] for b in range(_N_SCORE_BINS)
                            if (variable, lead_hours, b) in self._coarse}
            coarse_vec = _series(coarse_cells)
        vec = self._combine(fine_vec, coarse_vec)
        self._vectors[key] = vec
        return vec

    @staticmethod
    def _combine(fine_vec, coarse_vec):
        base = fine_vec if fine_vec is not None else coarse_vec
        if base is None:
            return None
        lowest = next(v for v in base if v is not None)
        out = []
        for b in range(_N_SCORE_BINS):
            v = base[b]
            if v is None:
                # below every active cell: never above the lowest active value
                cv = coarse_vec[b] if fine_vec is not None and coarse_vec is not None else None
                v = min(cv, lowest) if cv is not None else lowest
            out.append(min(1.0, max(0.0, v)))
        return out

    def lookup(self, variable: str, lead_hours: int, n_bin_: int, form_bin_: int,
               score_bin_: int) -> float | None:
        if not 0 <= score_bin_ < _N_SCORE_BINS:
            return None
        vec = self._vector(variable, lead_hours, n_bin_, form_bin_)
        return None if vec is None else vec[score_bin_]


def build_table(fine: dict, coarse: dict) -> CalibrationTable:
    return CalibrationTable(fine, coarse)
