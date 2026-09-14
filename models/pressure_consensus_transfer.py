# pressure_consensus_transfer: reads other base models' own predicted pressure for
# this run (via conn_out) and blends them into a private consensus, then feeds that
# consensus's predicted delta through a transfer function to forecast temperature
# and dewpoint. Also emits the consensus itself as this model's pressure forecast.
#
# unlike barogram_ensemble (which combines every base model's member_id=0 forecast
# per variable independently), this model cross-pollinates: a pressure consensus
# built from several sources feeds a prediction for a *different* variable.
#
# must run after any base models it reads from, in _MODELS order.
#
# members:
#   1  simple_mean_consensus  unweighted mean of source models' predicted pressure
#   2  spread_aware           same consensus mean, but the transferred temp/dewpoint
#                             delta is dampened when sources disagree (wide spread)
#   3  best_model_only        pass through a single source's prediction, picked by a
#                             fixed priority order (first available at that lead)

import statistics

import db
import models._confidence as _confidence
from models._climo_weights import LEAD_HOURS, VARIABLES
from models._utils import _sector
from models.pressure_trend_cascade import _build_delta_transfer_fns

MODEL_ID = 18
MODEL_NAME = "pressure_consensus_transfer"
NEEDS_CONN_IN = True
NEEDS_CONN_OUT = True
NEEDS_WEIGHTS = True
NEEDS_ALL_OBS = True
NEEDS_MATCH_HISTORY = True

# preference order for the best_model_only member; first one present at a given
# lead wins. anything not in this list is still eligible for the mean/spread members.
_PRIORITY = [
    "synoptic_state_machine", "full_state_analog", "analog",
    "climatological_mean", "persistence", "weighted_climatological_mean",
    "climo_deviation", "pressure_tendency", "diurnal_curve", "bogo",
]

_SPREAD_SCALE = 3.0  # hPa spread at which spread_aware fully damps the transfer

_MEMBERS = [
    (1, "simple_mean_consensus"),
    (2, "spread_aware"),
    (3, "best_model_only"),
]
_ALL_MEMBER_IDS = [mid for mid, _ in _MEMBERS]


def run(obs, issued_at, *, conn_in, conn_out, weights=None, all_obs=None,
        member_history=None, default_matches=None):
    if all_obs is None:
        all_obs = db.tempest_obs_in_range(conn_in, 0, issued_at)
    transfer_fns = _build_delta_transfer_fns(all_obs)

    source_rows = db.base_model_forecasts(conn_out, issued_at, ["pressure"])
    # exclude this model's own id in case of a re-run within the same issued_at
    source_rows = [r for r in source_rows if r["model_id"] != MODEL_ID]

    by_lead: dict[int, list[dict]] = {}
    for r in source_rows:
        if r["value"] is None:
            continue
        by_lead.setdefault(r["lead_hours"], []).append(r)

    p_now = obs["station_pressure"]
    rows = []
    member_vals: dict[tuple[int, str, int], float | None] = {}

    for lead in LEAD_HOURS:
        valid_at = obs["timestamp"] + lead * 3600
        sources = by_lead.get(lead, [])
        values = [r["value"] for r in sources]

        mean_p = sum(values) / len(values) if values else None
        spread_p = statistics.pstdev(values) if len(values) > 1 else 0.0

        best_p = None
        for name in _PRIORITY:
            match = next((r for r in sources if r["model"] == name), None)
            if match is not None:
                best_p = match["value"]
                break
        if best_p is None and sources:
            best_p = sources[0]["value"]

        preds = {1: mean_p, 2: mean_p, 3: best_p}

        cell_confidences_by_variable = {
            variable: _confidence.member_confidences(
                member_history, default_matches, _ALL_MEMBER_IDS, variable, lead
            )
            for variable in VARIABLES
        }

        for mid, _name in _MEMBERS:
            pred_pressure = preds[mid]
            member_vals[(mid, "pressure", lead)] = pred_pressure

            if pred_pressure is not None and p_now is not None:
                delta_p = pred_pressure - p_now
                if mid == 2 and spread_p > 0:
                    damp = max(0.0, 1.0 - min(1.0, spread_p / _SPREAD_SCALE))
                    delta_p *= damp
                for variable, col in VARIABLES.items():
                    if col == "station_pressure":
                        continue
                    tf = transfer_fns.get((col, lead))
                    obs_val = obs[col]
                    if tf is not None and obs_val is not None:
                        slope, intercept = tf
                        member_vals[(mid, variable, lead)] = obs_val + slope * delta_p + intercept
                    else:
                        member_vals[(mid, variable, lead)] = None
            else:
                for variable, col in VARIABLES.items():
                    if col != "station_pressure":
                        member_vals[(mid, variable, lead)] = None

            for variable in VARIABLES:
                rows.append({
                    "model_id": MODEL_ID,
                    "model": MODEL_NAME,
                    "member_id": mid,
                    "issued_at": issued_at,
                    "valid_at": valid_at,
                    "lead_hours": lead,
                    "variable": variable,
                    "value": member_vals[(mid, variable, lead)],
                    "confidence": cell_confidences_by_variable[variable].get(mid),
                })

        for variable in VARIABLES:
            cell_confidences = cell_confidences_by_variable[variable]
            valid_pairs = [
                (mid, member_vals[(mid, variable, lead)])
                for mid in _ALL_MEMBER_IDS
                if member_vals[(mid, variable, lead)] is not None
            ]
            if not valid_pairs:
                mean, group_confidence = None, None
            elif weights:
                member_weights = {
                    mid: weights.get((mid, variable, lead, _sector(valid_at)))
                    for mid, _ in valid_pairs
                }
                confidences = {mid: cell_confidences.get(mid) for mid, _ in valid_pairs}
                mean, group_confidence = _confidence.combine_pattern(
                    valid_pairs, member_weights, confidences
                )
            else:
                mean = sum(v for _, v in valid_pairs) / len(valid_pairs)
                group_confidence = _confidence.average_confidence(
                    [cell_confidences.get(mid) for mid, _ in valid_pairs]
                )
            spread = (
                statistics.pstdev([v for _, v in valid_pairs])
                if len(valid_pairs) > 1 else None
            )
            rows.append({
                "model_id": MODEL_ID,
                "model": MODEL_NAME,
                "member_id": 0,
                "issued_at": issued_at,
                "valid_at": valid_at,
                "lead_hours": lead,
                "variable": variable,
                "value": mean,
                "spread": spread,
                "confidence": group_confidence,
            })

    return rows
