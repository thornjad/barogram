import copy

import pytest

import db
import models._confidence as _confidence
import models._self_correction as sc
import sc_blend
from models._utils import _sector
from tests.conftest import make_output_db

_MODEL = 4
_ISSUED = 1_700_000_000
_LEAD = 6
_VALID = _ISSUED + _LEAD * 3600
_SECTOR = _sector(_VALID)
_SC = 9
_SC_FAST = 10


def _row(member_id, value, confidence=0.5, variable="temperature", valid_at=_VALID,
         lead=_LEAD, spread=None):
    row = {
        "model_id": _MODEL, "model": "m", "member_id": member_id, "issued_at": _ISSUED,
        "valid_at": valid_at, "lead_hours": lead, "variable": variable, "value": value,
        "confidence": confidence,
    }
    if spread is not None:
        row["spread"] = spread
    return row


def _w(member_id, weight, variable="temperature", lead=_LEAD):
    return {(member_id, variable, lead, _SECTOR): weight}


def _by_member(rows, variable="temperature"):
    return {r["member_id"]: r for r in rows if r["variable"] == variable}


def _apply(rows, weights, sc_members=((_SC, "self_correction"),)):
    return sc_blend.apply(rows, sc_members=list(sc_members), weights=weights)


def _stage_one(raw=20.0, sc_value=18.0, base=((1, 19.0), (2, 21.0))):
    rows = [_row(m, v) for m, v in base]
    rows.append(_row(0, raw, confidence=0.5, spread=1.0))
    rows.append(_row(_SC, sc_value, confidence=0.5))
    return rows


def _weights(base=0.8, sc=0.2):
    weights = {}
    weights.update(_w(1, base / 2))
    weights.update(_w(2, base / 2))
    weights.update(_w(_SC, sc))
    return weights


def test_blend_is_raw_weighted_by_the_rest_and_sc_by_its_weight():
    out = _by_member(_apply(_stage_one(), _weights()))

    assert out[0]["value"] == pytest.approx(0.8 * 20.0 + 0.2 * 18.0)


def test_blend_follows_combine_pattern_with_unequal_confidences():
    rows = _stage_one()
    rows[2]["confidence"] = 0.9   # raw mix confidence on the member 0 row
    rows[3]["confidence"] = 0.1   # sc confidence

    out = _by_member(_apply(rows, _weights()))

    value, _ = _confidence.combine_pattern(
        [(db.RAW_MIX_ID, 20.0), (_SC, 18.0)],
        {db.RAW_MIX_ID: 0.8, _SC: 0.2},
        {db.RAW_MIX_ID: 0.9, _SC: 0.1},
    )
    assert out[0]["value"] == pytest.approx(value)
    # confidence only weights the value; member 0's confidence is never an average of its inputs'
    assert out[0]["confidence"] == 0.9


def test_two_sc_members_both_enter():
    rows = _stage_one() + [_row(_SC_FAST, 16.0)]
    weights = _weights(base=0.7, sc=0.2)
    weights.update(_w(_SC_FAST, 0.1))

    out = _by_member(_apply(rows, weights, ((_SC, "self_correction"),
                                            (_SC_FAST, "self_correction_fast"))))

    assert out[0]["value"] == pytest.approx(0.7 * 20.0 + 0.2 * 18.0 + 0.1 * 16.0)


def test_sc_with_no_value_is_skipped():
    rows = _stage_one(sc_value=None) + [_row(_SC_FAST, 16.0)]
    weights = _weights(base=0.7, sc=0.2)
    weights.update(_w(_SC_FAST, 0.1))

    out = _by_member(_apply(rows, weights, ((_SC, "self_correction"),
                                            (_SC_FAST, "self_correction_fast"))))

    assert out[0]["value"] == pytest.approx((0.7 * 20.0 + 0.1 * 16.0) / 0.8)


def test_raw_weight_counts_only_members_that_have_a_value():
    rows = _stage_one(base=((1, 19.0), (2, None)))   # member 2 abstained this cell

    out = _by_member(_apply(rows, _weights(base=0.8, sc=0.2)))

    # raw weight is member 1's 0.4 alone, not 0.8
    assert out[0]["value"] == pytest.approx((0.4 * 20.0 + 0.2 * 18.0) / 0.6)


def test_final_lies_between_raw_and_sc():
    out = _by_member(_apply(_stage_one(raw=20.0, sc_value=18.0), _weights()))

    assert 18.0 <= out[0]["value"] <= 20.0


@pytest.mark.parametrize("weights", [
    {},                                   # no weights yet
    None,
    {**_w(1, 0.4), **_w(2, 0.4)},         # sc has no stored weight
    {**_w(_SC, 0.2)},                     # other members have no stored weights
])
def test_member_zero_unchanged_without_a_weight_basis(weights):
    rows = _stage_one()

    out = _by_member(_apply(rows, weights))

    assert out[0] == rows[2]


def test_unchanged_when_every_sc_is_none():
    rows = _stage_one(sc_value=None)

    out = _by_member(_apply(rows, _weights()))

    assert out[0] == rows[2]


def test_raw_mix_row_is_a_copy_of_stage_one_member_zero():
    rows = _stage_one()

    out = _by_member(_apply(rows, _weights()))

    expected = {**rows[2], "member_id": db.RAW_MIX_ID}
    assert out[db.RAW_MIX_ID] == expected


def test_final_and_raw_mix_carry_the_stage_one_spread():
    out = _by_member(_apply(_stage_one(), _weights()))

    assert out[0]["spread"] == 1.0
    assert out[db.RAW_MIX_ID]["spread"] == 1.0


def test_cell_without_member_zero_gets_no_raw_mix_row():
    rows = [_row(1, 19.0), _row(_SC, 18.0)]

    out = _apply(rows, _weights())

    assert db.RAW_MIX_ID not in {r["member_id"] for r in out}
    assert out == rows


def test_cells_are_blended_independently():
    rows = _stage_one() + [
        _row(1, 8.0, variable="dewpoint"), _row(2, 10.0, variable="dewpoint"),
        _row(0, 9.0, variable="dewpoint"), _row(_SC, 7.0, variable="dewpoint"),
    ]
    weights = _weights()
    for m, w in ((1, 0.4), (2, 0.4), (_SC, 0.2)):
        weights.update(_w(m, w, variable="dewpoint"))

    out = _apply(rows, weights)

    assert _by_member(out)[0]["value"] == pytest.approx(0.8 * 20.0 + 0.2 * 18.0)
    assert _by_member(out, "dewpoint")[0]["value"] == pytest.approx(0.8 * 9.0 + 0.2 * 7.0)


def test_sc_row_for_a_different_time_raises():
    rows = _stage_one()
    rows[3]["valid_at"] = _VALID + 3600

    with pytest.raises(ValueError):
        _apply(rows, _weights())


def test_input_rows_are_left_unchanged():
    rows = _stage_one()
    before = copy.deepcopy(rows)

    _apply(rows, _weights())

    assert rows == before


def test_each_member_dewpoint_is_clamped_to_its_own_temperature():
    conn = make_output_db()
    rows = [
        _row(1, 19.0), _row(2, 21.0), _row(0, 20.0), _row(_SC, 10.0),
        _row(1, 25.0, variable="dewpoint"), _row(2, 25.0, variable="dewpoint"),
        _row(0, 25.0, variable="dewpoint"), _row(_SC, 25.0, variable="dewpoint"),
    ]
    weights = _weights()
    for m, w in ((1, 0.4), (2, 0.4), (_SC, 0.2)):
        weights.update(_w(m, w, variable="dewpoint"))

    db.insert_forecasts(conn, _apply(rows, weights))

    stored = conn.execute(
        "select member_id, variable, value from forecasts where model_id = ?", (_MODEL,)
    ).fetchall()
    value = {(r["member_id"], r["variable"]): r["value"] for r in stored}
    for member in (0, db.RAW_MIX_ID, _SC):
        assert value[(member, "dewpoint")] <= value[(member, "temperature")]


def test_downstream_models_read_the_final_member_zero():
    # models 18 and 19 read other models' member 0 from the db in the same run
    conn = make_output_db()
    rows = [
        _row(1, 1000.0, variable="pressure"), _row(2, 1004.0, variable="pressure"),
        _row(0, 1002.0, variable="pressure"), _row(_SC, 998.0, variable="pressure"),
    ]
    weights = {}
    for m, w in ((1, 0.4), (2, 0.4), (_SC, 0.2)):
        weights.update(_w(m, w, variable="pressure"))

    out = _apply(rows, weights)
    db.insert_forecasts(conn, out)

    seen = {r["model_id"]: r["value"] for r in db.base_model_forecasts(conn, _ISSUED, ["pressure"])}
    assert seen[_MODEL] == pytest.approx(0.8 * 1002.0 + 0.2 * 998.0)


def _cycle(conn, issued, raw, final_error, raw_error):
    # one forecast run: the model emits raw mix + sc, the blend stores both, then the
    # run is scored (final member 0 gets final_error, the stored raw mix raw_error)
    valid = issued + _LEAD * 3600
    sc_value = sc.corrected_value(conn, _MODEL, "temperature", _LEAD, raw, issued)
    rows = [
        _row(1, raw, valid_at=valid), _row(0, raw, valid_at=valid),
        _row(_SC, sc_value, valid_at=valid),
    ]
    for r in rows:
        r["issued_at"] = issued
    weights = {(1, "temperature", _LEAD, _sector(valid)): 0.8,
               (_SC, "temperature", _LEAD, _sector(valid)): 0.2}
    out = sc_blend.apply(rows, sc_members=[(_SC, "self_correction")], weights=weights)
    db.insert_forecasts(conn, out)
    for member, error in ((0, final_error), (db.RAW_MIX_ID, raw_error)):
        conn.execute(
            "update forecasts set observed = 0, error = ?, mae = ?, scored_at = ? "
            "where model_id = ? and member_id = ? and issued_at = ?",
            (error, abs(error), valid + 1, _MODEL, member, issued),
        )
    return sc_value


def test_sc_learns_from_the_raw_mix_across_cycles_not_from_final_member_zero():
    conn = make_output_db()
    conn.execute("insert or ignore into members (model_id, member_id, name) "
                 "values (?, -1, 'raw_mix')", (_MODEL,))
    issued = _ISSUED
    values = []
    for i in range(sc._MIN_SAMPLES + 1):
        # the raw mix runs +5 hot; the final member 0 (already corrected) only +1
        values.append(_cycle(conn, issued + i * 86400, 20.0, final_error=1.0, raw_error=5.0))

    # from the fourth cycle on, sc has a bias to apply, and it is the raw mix's +5
    assert values[0] is None
    assert values[-1] == pytest.approx(20.0 - 5.0)
