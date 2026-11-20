# meta_forecast (model 300): the best forecast for this yard from everything
# barogram knows. It reads the rows the other model classes just wrote for this
# run (the ensemble, the three external models, the base models' pressure) plus
# current conditions, and combines them as a multi-member model. tune weights
# the members like any other model's. type='meta', so the ensemble never reads it.
#
# members:
#   1-4    pass-throughs of nws, tempest_forecast, external_corrected, the ensemble
#   11-18  derived members that each combine or correct those sources differently
#   91/92  self-correction (60 and 14 day windows), folded into member 0 by sc_blend
#   100+n  pass-through of base model n, registered at run time when emitted:
#          persistence (101) for every variable, other base models for pressure only,
#          since the ensemble emits no pressure
# the static members are registered by the migration; the 100-series by run().

import bisect
import math
import statistics

import db
import models._confidence as _confidence
import models._self_correction as _self_correction
from models._utils import _sector

MODEL_ID = 300
MODEL_NAME = "meta_forecast"
MODEL_TYPE = "meta"
NEEDS_CONN_OUT = True
NEEDS_WEIGHTS = True
NEEDS_MATCH_HISTORY = True

# member id -> (source model id, member name): pass-through of that model's member 0
PASS_THROUGH_SOURCES = {
    1: (200, "nws"),
    2: (201, "tempest_forecast"),
    3: (202, "external_corrected"),
    4: (100, "barogram_ensemble"),
}

DERIVED_MEMBERS = {
    11: "median_sources",
    12: "hot_hand",
    13: "spread_gated_ensemble",
    14: "ensemble_residual",
    15: "obs_anchor",
    16: "lagged_consensus",
    17: "ensemble_gated",
    18: "confidence_argmax",
}

SELF_CORRECTION_MEMBER = 91
SELF_CORRECTION_FAST_MEMBER = 92

STATIC_MEMBERS = {
    **{member_id: name for member_id, (_model_id, name) in PASS_THROUGH_SOURCES.items()},
    **DERIVED_MEMBERS,
    SELF_CORRECTION_MEMBER: "self_correction",
    SELF_CORRECTION_FAST_MEMBER: "self_correction_fast",
}

BASE_MEMBER_OFFSET = 100
PERSISTENCE_MODEL_ID = 1

_SOURCE_MODEL_IDS = tuple(model_id for model_id, _name in PASS_THROUGH_SOURCES.values())
# the cell's valid_at comes from the first of these present; they are on the hour
# (202 first) while the ensemble anchors to the latest observation
_VALID_AT_PREFERENCE = (202, 200, 201, 100)
_FAST_WINDOW_DAYS = 14
_HOT_HAND_DAYS = 5
_HOT_HAND_MIN_ROWS = 6        # rows a pool member needs in the cell before its MAE counts
_SPREAD_REF_DAYS = 30
_SPREAD_REF_MIN_ROWS = 20
_NEUTRAL_GATE = 0.5           # spread gate when there is too little spread history
_GATE_WEIGHT = 0.5            # share of the ensemble's pull that member 13 passes through
_MIN_MAE = 1e-6
# models whose confidence is their own random draw (models/bogo.py sets
# STATES_OWN_MEMBER_ZERO_CONFIDENCE), so it cannot be compared with the others'
_SELF_CONFIDENCE_MODELS = (12,)
_FIT_DAYS = 90
_MIN_FIT_PAIRS = 30
_SHRINK_PAIRS = 20            # pseudo-pairs of shrinkage toward zero in the fitted coefficients
_GAMMA_BOUNDS = (-0.5, 1.0)
_RHO_BOUNDS = (0.0, 1.5)
_ANCHOR_WINDOW_SEC = 7200     # how far back the recent-202-error feature looks
_LAG_LOOKBACK_SEC = 7 * 3600
_LAG_MAX_RUNS = 2
_LAG_MIN_GAP_SEC = 3600       # earlier runs must be at least this far from this run and each other
_LAG_VALID_TOLERANCE_SEC = 600


def base_member_id(model_id: int) -> int:
    """Member id of the pass-through of a base model."""
    return BASE_MEMBER_OFFSET + model_id


def _is_source(row) -> bool:
    """A row 300 uses as a member: the four fixed sources for temperature and
    dewpoint, persistence for every variable, and any other base model for
    pressure only (the ensemble covers the rest of their forecasts for
    temperature and dewpoint)."""
    model_id = row["model_id"]
    if model_id in _SOURCE_MODEL_IDS:
        return row["variable"] != "pressure"
    return model_id == PERSISTENCE_MODEL_ID or row["variable"] == "pressure"


def _load_cells(conn_out, issued_at: int) -> dict:
    """{(variable, lead_hours): {model_id: row}} of this run's usable source rows.
    A source row with no value is dropped here so nothing downstream sees it."""
    rows = db.meta_inputs(conn_out, issued_at, issued_at,
                          model_ids=_SOURCE_MODEL_IDS, model_types=("base",))
    cells: dict = {}
    for row in rows:
        if row["value"] is None or not _is_source(row):
            continue
        cells.setdefault((row["variable"], row["lead_hours"]), {})[row["model_id"]] = row
    return cells


def _cell_valid_at(cell: dict) -> int:
    for model_id in _VALID_AT_PREFERENCE:
        if model_id in cell:
            return cell[model_id]["valid_at"]
    return min(row["valid_at"] for row in cell.values())


def _member_row(member_id: int, issued_at: int, valid_at: int, lead: int, variable: str,
                value, confidence=None) -> dict:
    return {
        "model_id": MODEL_ID,
        "model": MODEL_NAME,
        "member_id": member_id,
        "issued_at": issued_at,
        "valid_at": valid_at,
        "lead_hours": lead,
        "variable": variable,
        "value": value,
        "confidence": confidence,
    }


def _pass_through_rows(cell: dict, issued_at: int, variable: str, lead: int,
                       registered: dict) -> list[dict]:
    rows = []
    for member_id, (model_id, _name) in PASS_THROUGH_SOURCES.items():
        src = cell.get(model_id)
        if src is not None:
            rows.append(_member_row(member_id, issued_at, src["valid_at"], lead, variable,
                                    src["value"], src["confidence"]))
    for model_id, src in cell.items():
        if model_id in _SOURCE_MODEL_IDS:
            continue
        member_id = base_member_id(model_id)
        registered[member_id] = src["model"]
        rows.append(_member_row(member_id, issued_at, src["valid_at"], lead, variable,
                                src["value"], src["confidence"]))
    return rows


def _source_model(member_id: int) -> int:
    """The model a pool member is a pass-through of."""
    if member_id in PASS_THROUGH_SOURCES:
        return PASS_THROUGH_SOURCES[member_id][0]
    return member_id - BASE_MEMBER_OFFSET


def _pool(member_rows: list[dict], variable: str) -> list[dict]:
    """The pass-through rows that median_sources, hot_hand and confidence_argmax
    draw from: the four fixed sources for temperature and dewpoint, every base
    model pass-through for pressure."""
    if variable == "pressure":
        return [r for r in member_rows if r["member_id"] > BASE_MEMBER_OFFSET]
    return [r for r in member_rows if r["member_id"] in PASS_THROUGH_SOURCES]


def _recent_mae(conn_out, issued_at: int, model_ids) -> dict:
    """{(model_id, variable, lead_hours): (mean mae, row count)} over the last
    _HOT_HAND_DAYS days of each model's scored member 0, loaded once per run."""
    since = issued_at - _HOT_HAND_DAYS * 86400
    totals: dict = {}
    for model_id in model_ids:
        for row in db.model_error_history(conn_out, model_id, 0, since):
            key = (model_id, row["variable"], row["lead_hours"])
            total, count = totals.get(key, (0.0, 0))
            totals[key] = (total + row["mae"], count + 1)
    return {key: (total / count, count) for key, (total, count) in totals.items()}


def _hot_hand_value(pool: list[dict], recent_mae: dict, variable: str, lead: int):
    """Pool mean weighted by inverse recent MAE; the plain mean when any pool
    member has too few recent rows in this cell to trust its MAE."""
    if len(pool) < 2:
        return None
    stats = [recent_mae.get((_source_model(r["member_id"]), variable, lead)) for r in pool]
    values = [r["value"] for r in pool]
    if any(stat is None or stat[1] < _HOT_HAND_MIN_ROWS for stat in stats):
        return sum(values) / len(values)
    weights = [1.0 / max(stat[0], _MIN_MAE) for stat in stats]
    return sum(w * v for w, v in zip(weights, values)) / sum(weights)


def _spread_reference(conn_out, issued_at: int, variable: str) -> dict:
    """{lead_hours: median of the ensemble's member-0 spread over the last
    _SPREAD_REF_DAYS days}, for leads with enough history."""
    by_lead: dict = {}
    since = issued_at - _SPREAD_REF_DAYS * 86400
    for row in db.member_zero_history(conn_out, 100, variable, since):
        if row["spread"] is not None:
            by_lead.setdefault(row["lead_hours"], []).append(row["spread"])
    return {lead: statistics.median(spreads) for lead, spreads in by_lead.items()
            if len(spreads) >= _SPREAD_REF_MIN_ROWS}


def _spread_gate(spread: float, spread_ref) -> float:
    """1 when the ensemble members agree, 0 at twice the usual spread."""
    if spread_ref is None or spread_ref <= 0:
        return _NEUTRAL_GATE
    return max(0.0, min(1.0, 1.0 - spread / (2.0 * spread_ref)))


def _shrunk_slope(pairs: list, sign: float, bounds: tuple):
    """sign * sum(y * x) / (sum(x * x) * (1 + shrink / n)), clipped; pairs are (y, x).
    None below _MIN_FIT_PAIRS, 0.0 when x carries no signal."""
    n = len(pairs)
    if n < _MIN_FIT_PAIRS:
        return None
    sxx = sum(x * x for _, x in pairs)
    if sxx <= 0:
        return 0.0
    slope = sign * sum(y * x for y, x in pairs) / (sxx * (1.0 + _SHRINK_PAIRS / n))
    return max(bounds[0], min(bounds[1], slope))


def _fit_gamma(pairs: list):
    """gamma for v202 + gamma * (v_ens - v202), from (e_c, d) pairs with d the
    ensemble's error minus 202's: minimizes the blended error e_c + gamma * d."""
    return _shrunk_slope(pairs, -1.0, _GAMMA_BOUNDS)


def _fit_rho(pairs: list):
    """rho for v202 - rho * r, from (e_c, r) pairs: minimizes e_c - rho * r."""
    return _shrunk_slope(pairs, 1.0, _RHO_BOUNDS)


def _anchor_feature(history: list, now: int):
    """Mean error of the rows valid in (now - 2h, now]. history is (valid_at, error)
    sorted by valid_at; None when no row falls in the window."""
    lo = bisect.bisect_right(history, now - _ANCHOR_WINDOW_SEC, key=lambda row: row[0])
    hi = bisect.bisect_right(history, now, key=lambda row: row[0])
    window = history[lo:hi]
    if not window:
        return None
    return sum(error for _, error in window) / len(window)


def _load_fit(conn_out, issued_at: int, variable: str) -> dict:
    """Fitted gamma and rho per lead and the 202 error history for one variable,
    from the last _FIT_DAYS days of scored 202 and ensemble member-0 errors."""
    since = issued_at - _FIT_DAYS * 86400 - _ANCHOR_WINDOW_SEC
    window_start = issued_at - _FIT_DAYS * 86400
    c202 = db.member_zero_history(conn_out, 202, variable, since)
    ensemble = {(r["issued_at"], r["lead_hours"]): r["error"]
                for r in db.member_zero_history(conn_out, 100, variable, since)}
    history = sorted((r["valid_at"], r["error"]) for r in c202)
    anchor_at: dict = {}
    gamma_pairs: dict = {}
    rho_pairs: dict = {}
    for r in c202:
        if r["issued_at"] < window_start:
            continue
        lead, e_c = r["lead_hours"], r["error"]
        e_ens = ensemble.get((r["issued_at"], lead))
        if e_ens is not None:
            gamma_pairs.setdefault(lead, []).append((e_c, e_ens - e_c))
        if r["issued_at"] not in anchor_at:
            anchor_at[r["issued_at"]] = _anchor_feature(history, r["issued_at"])
        anchor = anchor_at[r["issued_at"]]
        if anchor is not None:
            rho_pairs.setdefault(lead, []).append((e_c, anchor))
    return {
        "gamma": {lead: _fit_gamma(pairs) for lead, pairs in gamma_pairs.items()},
        "rho": {lead: _fit_rho(pairs) for lead, pairs in rho_pairs.items()},
        "anchor": _anchor_feature(history, issued_at),
    }


def _lagged_runs(rows: list, cell_valid_at: int, issued_at: int) -> list:
    """Values of up to two earlier 202 runs for this valid time. One row per run
    (nearest valid_at, within tolerance); a run counts only if it was issued at
    least an hour before this run and before the previous kept run."""
    by_run: dict = {}
    for r in rows:
        gap = abs(r["valid_at"] - cell_valid_at)
        if gap > _LAG_VALID_TOLERANCE_SEC:
            continue
        best = by_run.get(r["issued_at"])
        if best is None or gap < abs(best["valid_at"] - cell_valid_at):
            by_run[r["issued_at"]] = r
    kept: list = []
    for run_issued in sorted(by_run, reverse=True):
        if run_issued > issued_at - _LAG_MIN_GAP_SEC:
            continue
        if kept and run_issued > kept[-1] - _LAG_MIN_GAP_SEC:
            continue
        kept.append(run_issued)
        if len(kept) == _LAG_MAX_RUNS:
            break
    return [by_run[t]["value"] for t in kept]


def _derived_values(cell: dict, pool: list[dict], variable: str, lead: int, recent_mae: dict,
                    spread_ref: dict, fit: dict, earlier: list, issued_at: int, valid_at: int) -> dict:
    """{member_id: value} of the derived members that have what they need."""
    values: dict = {}
    if len(pool) >= 3:
        values[11] = statistics.median(r["value"] for r in pool)
    hot = _hot_hand_value(pool, recent_mae, variable, lead)
    if hot is not None:
        values[12] = hot

    c202, ens = cell.get(202), cell.get(100)
    if c202 is not None and ens is not None:
        v202, v_ens = c202["value"], ens["value"]
        if ens["spread"] is not None:
            gate = _spread_gate(ens["spread"], spread_ref.get(lead))
            values[13] = v202 + _GATE_WEIGHT * gate * (v_ens - v202)
        if c202["confidence"] is not None and ens["confidence"] is not None:
            total = c202["confidence"] + ens["confidence"]
            if total > 0:
                values[17] = v202 + (ens["confidence"] / total) * (v_ens - v202)
        gamma = fit["gamma"].get(lead)
        if gamma is not None:
            values[14] = v202 + gamma * (v_ens - v202)

    if c202 is not None:
        rho = fit["rho"].get(lead)
        if rho is not None and fit["anchor"] is not None:
            values[15] = c202["value"] - rho * fit["anchor"]
        lagged = _lagged_runs(earlier, valid_at, issued_at)
        if lagged:
            values[16] = (c202["value"] + sum(lagged)) / (1 + len(lagged))

    with_confidence = [r for r in pool if r["confidence"] is not None
                       and _source_model(r["member_id"]) not in _SELF_CONFIDENCE_MODELS]
    if len(with_confidence) >= 2:
        best = max(with_confidence, key=lambda r: (r["confidence"], -r["member_id"]))
        if best["confidence"] > 0:
            values[18] = best["value"]
    return values


def run(obs, issued_at: int, *, conn_out, weights=None, member_history=None,
        default_matches=None) -> list[dict]:
    """Combine this run's source rows into one multi-member forecast.

    Member 0 is the weighted, confidence-adjusted mean of every non-SC member, as
    in models/ensemble.py; sc_blend.apply folds the two self-correction members
    into it afterwards. Members emitted for base models are registered here, so a
    base model that starts emitting pressure joins without a migration.
    """
    cells = _load_cells(conn_out, issued_at)
    registered: dict[int, str] = {}
    rows = []
    recent_mae = _recent_mae(conn_out, issued_at, {m for cell in cells.values() for m in cell})
    spread_ref: dict[str, dict] = {}
    fits: dict[str, dict] = {}
    earlier_by_variable: dict[str, list] = {}
    for r in db.meta_inputs(conn_out, issued_at - _LAG_LOOKBACK_SEC, issued_at - 1, model_ids=(202,)):
        if r["value"] is not None:
            earlier_by_variable.setdefault(r["variable"], []).append(r)

    for (variable, lead), cell in sorted(cells.items()):
        valid_at = _cell_valid_at(cell)
        sector = _sector(valid_at)
        member_rows = _pass_through_rows(cell, issued_at, variable, lead, registered)
        if not member_rows:
            continue

        if variable not in spread_ref:
            spread_ref[variable] = _spread_reference(conn_out, issued_at, variable)
        if variable not in fits:
            fits[variable] = _load_fit(conn_out, issued_at, variable)
        derived = _derived_values(cell, _pool(member_rows, variable), variable, lead, recent_mae,
                                  spread_ref[variable], fits[variable],
                                  earlier_by_variable.get(variable, []), issued_at, valid_at)
        derived_confidence = _confidence.member_confidences(
            member_history, default_matches, list(derived), variable, lead)
        member_rows.extend(
            _member_row(member_id, issued_at, valid_at, lead, variable, value,
                        derived_confidence[member_id])
            for member_id, value in sorted(derived.items()))
        rows.extend(member_rows)

        pairs = [(r["member_id"], r["value"]) for r in member_rows if r["value"] is not None]
        member_weights = {mid: (weights.get((mid, variable, lead, sector)) if weights else None)
                          for mid, _ in pairs}
        confidences = {r["member_id"]: r["confidence"] for r in member_rows}
        mean, group_confidence = _confidence.combine_pattern(pairs, member_weights, confidences)
        spread = math.sqrt(sum((v - mean) ** 2 for _, v in pairs) / len(pairs))

        zero = _member_row(0, issued_at, valid_at, lead, variable, mean, group_confidence)
        zero["spread"] = spread
        rows.append(zero)

        sc_confidence = _confidence.member_confidences(
            member_history, default_matches,
            [SELF_CORRECTION_MEMBER, SELF_CORRECTION_FAST_MEMBER], variable, lead)
        for member_id, window in ((SELF_CORRECTION_MEMBER, None),
                                  (SELF_CORRECTION_FAST_MEMBER, _FAST_WINDOW_DAYS)):
            kwargs = {} if window is None else {"window_days": window}
            corrected = _self_correction.corrected_value(
                conn_out, MODEL_ID, variable, lead, mean, issued_at, **kwargs)
            rows.append(_member_row(member_id, issued_at, valid_at, lead, variable, corrected,
                                    sc_confidence[member_id]))

    if registered:
        conn_out.executemany(
            "insert or ignore into members (model_id, member_id, name) values (?, ?, ?)",
            [(MODEL_ID, member_id, name) for member_id, name in registered.items()],
        )
    return rows
