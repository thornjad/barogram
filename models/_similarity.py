# shared analog-matching primitives: normalized feature distance and
# k-nearest selection, generalized from models/analog.py's own logic so
# other models can match against the same shared default fingerprint.

import math
import statistics


def norm_sigmas(candidates: list[dict], features: list[str]) -> dict[str, float | None]:
    """Per-feature population std dev across candidates; None means skip the feature."""
    sigmas = {}
    for col in features:
        vals = [r[col] for r in candidates if r[col] is not None]
        if len(vals) < 2:
            sigmas[col] = None
        else:
            sigma = statistics.pstdev(vals)
            sigmas[col] = sigma if sigma > 0 else None
    return sigmas


def distance(current: dict, candidate: dict, features: list[str],
             sigmas: dict[str, float | None], weights: list[float] | None = None) -> float | None:
    """Weighted Euclidean distance in sigma-normalized feature space.
    weights defaults to 1.0 per feature when omitted."""
    if weights is None:
        weights = [1.0] * len(features)
    total = 0.0
    used = 0
    for i, col in enumerate(features):
        sigma = sigmas[col]
        if sigma is None:
            continue
        o = current[col]
        c = candidate[col]
        if o is None or c is None:
            continue
        z = (o - c) / sigma
        total += weights[i] * z * z
        used += 1
    if used == 0:
        return None
    return math.sqrt(total)


def select_k_nearest(cands_with_dist: list[tuple], k: int) -> list[tuple]:
    """Return up to K (distance, candidate) pairs sorted by distance ascending."""
    valid = [(d, c) for d, c in cands_with_dist if d is not None]
    valid.sort(key=lambda x: x[0])
    return valid[:k]
