import random

import pytest

import barogram
import db
import models._confidence as _confidence
import sc_blend
from models._utils import _sector
from tests.conftest import make_input_db, make_obs
from tests.test_models_contract import _make_seeded_output_db, run_model

# models whose member 0 does not follow the stored weights, so the raw mix is not the
# weighted combination of their other member rows. full_state_analog looks weights up
# without the sector element and falls back to an equal mean.
_MEMBER_ZERO_IGNORES_STORED_WEIGHTS = {"full_state_analog"}

_SC_NAMES = ("self_correction", "self_correction_fast")


def _sc_models(conn):
    ids = {r[0] for r in conn.execute(
        "select distinct model_id from members where name in (?, ?)", _SC_NAMES)}
    return [m for m in barogram._MODELS if m.MODEL_ID in ids]


@pytest.fixture(scope="module")
def setup():
    obs = make_obs()
    issued_at = obs["timestamp"]
    conn_in = make_input_db()
    conn_out = _make_seeded_output_db(issued_at)
    return obs, issued_at, conn_in, conn_out


def _models(setup):
    return _sc_models(setup[3])


def _sc_ids(conn, model_id):
    return {r[0] for r in conn.execute(
        "select member_id from members where model_id = ? and name in (?, ?)",
        (model_id, *_SC_NAMES))}


def test_there_are_models_with_a_self_correction_member(setup):
    assert len(_models(setup)) >= 27


def test_registered_models_declare_what_the_blend_needs(setup):
    for model in _models(setup):
        for flag in ("NEEDS_WEIGHTS", "NEEDS_MATCH_HISTORY", "NEEDS_CONN_OUT"):
            assert getattr(model, flag, False), f"{model.MODEL_NAME} lacks {flag}"


def test_models_emit_no_raw_mix_and_the_blend_is_inert_without_weights(setup):
    obs, issued_at, conn_in, conn_out = setup
    for model in _models(setup):
        sc_ids = _sc_ids(conn_out, model.MODEL_ID)
        rows = run_model(model, obs, issued_at, conn_in, conn_out)
        assert db.RAW_MIX_ID not in {r.get("member_id", 0) for r in rows}, model.MODEL_NAME

        blended = sc_blend.apply(
            rows, sc_members=[(i, "self_correction") for i in sc_ids], weights={})

        triples = [(r["lead_hours"], r["variable"], r.get("member_id", 0)) for r in blended]
        assert len(triples) == len(set(triples)), f"{model.MODEL_NAME}: duplicate rows"
        zeros = {(r["lead_hours"], r["variable"]): r for r in rows if r.get("member_id", 0) == 0}
        for r in blended:
            if r.get("member_id", 0) == 0:
                assert r == zeros[(r["lead_hours"], r["variable"])], model.MODEL_NAME
        raw_mix = [r for r in blended if r.get("member_id") == db.RAW_MIX_ID]
        assert len(raw_mix) == len(zeros), model.MODEL_NAME


def test_member_zero_is_the_weighted_combination_of_the_rows_the_blend_counts(setup):
    obs, issued_at, conn_in, conn_out = setup
    for model in _models(setup):
        if model.MODEL_NAME in _MEMBER_ZERO_IGNORES_STORED_WEIGHTS:
            continue
        sc_ids = _sc_ids(conn_out, model.MODEL_ID)
        random.seed(0)  # bogo draws its members at random; both runs must draw the same
        rows = run_model(model, obs, issued_at, conn_in, conn_out)

        weights = {}
        for r in rows:
            member = r.get("member_id", 0)
            if member != 0 and member not in sc_ids and r["value"] is not None:
                weights[(member, r["variable"], r["lead_hours"], _sector(r["valid_at"]))] = 0.1
        random.seed(0)
        weighted = run_model(model, obs, issued_at, conn_in, conn_out, weights=weights)

        cells = {}
        for r in weighted:
            member = r.get("member_id", 0)
            if member == 0 or member in sc_ids or r["value"] is None:
                continue
            cells.setdefault((r["variable"], r["lead_hours"]), []).append(r)
        for r in weighted:
            if r.get("member_id", 0) != 0 or r["value"] is None or r["variable"] == "dewpoint":
                continue  # dewpoint means are clamped to temperature after combining
            others = cells.get((r["variable"], r["lead_hours"]), [])
            expected, _ = _confidence.combine_pattern(
                [(o["member_id"], o["value"]) for o in others],
                {o["member_id"]: 0.1 for o in others},
                {o["member_id"]: o.get("confidence") for o in others},
            )
            assert r["value"] == pytest.approx(expected), \
                f"{model.MODEL_NAME} {r['variable']} lead {r['lead_hours']}"
