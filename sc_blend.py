# blends a model's self-correction (SC) members into its member 0. a model's
# run() keeps producing its raw mix as member 0 plus its SC rows; apply() stores
# that raw mix as its own member (RAW_MIX_ID) and replaces member 0 with the raw
# mix blended with the SC members using the weights tune already stores.
#
# the raw mix already carries every other member's weight, so its weight is the
# sum of those members' stored weights, and each SC member enters at its own
# stored weight. nothing here clamps or floors a weight: a cell with no weight
# basis yet (no stored weights for the other members or for any SC) is left as
# the model produced it.

import models._confidence as _confidence
from db import RAW_MIX_ID
from models._utils import _sector


def apply(rows: list[dict], *, sc_members: list[tuple[int, str]],
          weights: dict | None) -> list[dict]:
    """Return rows with a raw_mix row added and member 0 blended, per cell.

    rows are one model's stage-one rows; sc_members are (member_id, name) pairs of
    its self-correction members; weights is the model's stored tune weights keyed
    (member_id, variable, lead_hours, sector). Input rows are never mutated.
    """
    sc_ids = {member_id for member_id, _ in sc_members}
    weights = weights or {}
    out = [dict(r) for r in rows]

    cells: dict[tuple, list[int]] = {}
    for i, r in enumerate(out):
        cells.setdefault((r["variable"], r["lead_hours"]), []).append(i)

    additions = []
    for (variable, lead), indexes in cells.items():
        member_zero = next((i for i in indexes if out[i]["member_id"] == 0), None)
        if member_zero is None:
            continue
        raw_row = out[member_zero]
        additions.append({**raw_row, "member_id": RAW_MIX_ID})

        sector = _sector(raw_row["valid_at"])
        others = [out[i] for i in indexes
                  if out[i]["member_id"] not in sc_ids and out[i]["member_id"] != 0]
        sc_rows = [out[i] for i in indexes if out[i]["member_id"] in sc_ids]
        for sc_row in sc_rows:
            if sc_row["valid_at"] != raw_row["valid_at"]:
                raise ValueError(
                    f"sc member {sc_row['member_id']} is for a different time than member 0 "
                    f"in {variable} lead {lead}"
                )
        if raw_row["value"] is None:
            continue

        def stored(member_id):
            return weights.get((member_id, variable, lead, sector))

        w_raw = sum(stored(r["member_id"]) for r in others
                    if r["value"] is not None and stored(r["member_id"]) is not None)
        sc_pairs = [r for r in sc_rows
                    if r["value"] is not None and stored(r["member_id"]) is not None]
        if not sc_pairs or w_raw <= 0:
            continue

        pairs = [(RAW_MIX_ID, raw_row["value"])] + [(r["member_id"], r["value"]) for r in sc_pairs]
        member_weights = {RAW_MIX_ID: w_raw, **{r["member_id"]: stored(r["member_id"]) for r in sc_pairs}}
        confidences = {RAW_MIX_ID: raw_row.get("confidence"),
                       **{r["member_id"]: r.get("confidence") for r in sc_pairs}}
        value, confidence = _confidence.combine_pattern(pairs, member_weights, confidences)
        raw_row["value"] = value
        raw_row["confidence"] = confidence

    return out + additions
