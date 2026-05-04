"""Tests for airmass_precip model behavior with sparse/empty data."""
import models.airmass_precip as ap_mod
from tests.conftest import make_input_db, make_obs

_EXPECTED_ROWS = (len(ap_mod._MEMBER_NAMES) + 1) * len(ap_mod.LEAD_HOURS)
_MEMBER_IDS = {m[0] for m in ap_mod._MEMBER_NAMES}


class TestNoHistoricalData:
    def test_returns_expected_rows(self):
        conn_in = make_input_db()
        obs = make_obs()
        rows = ap_mod.run(obs, obs["timestamp"], conn_in=conn_in)
        assert len(rows) == _EXPECTED_ROWS

    def test_all_values_none(self):
        conn_in = make_input_db()
        obs = make_obs()
        rows = ap_mod.run(obs, obs["timestamp"], conn_in=conn_in)
        assert all(row["value"] is None for row in rows)

    def test_all_required_keys_present(self):
        required = {"model_id", "model", "issued_at", "valid_at", "lead_hours", "variable", "value"}
        conn_in = make_input_db()
        obs = make_obs()
        rows = ap_mod.run(obs, obs["timestamp"], conn_in=conn_in)
        for row in rows:
            assert required.issubset(row.keys()), f"missing keys: {required - row.keys()}"

    def test_no_duplicate_member_lead(self):
        conn_in = make_input_db()
        obs = make_obs()
        rows = ap_mod.run(obs, obs["timestamp"], conn_in=conn_in)
        pairs = [(r["lead_hours"], r.get("member_id", 0)) for r in rows]
        assert len(pairs) == len(set(pairs))

    def test_only_precip_prob_variable(self):
        conn_in = make_input_db()
        obs = make_obs()
        rows = ap_mod.run(obs, obs["timestamp"], conn_in=conn_in)
        assert all(r["variable"] == "precip_prob" for r in rows)


class TestModelConstants:
    def test_model_id(self):
        assert ap_mod.MODEL_ID == 11

    def test_model_name(self):
        assert ap_mod.MODEL_NAME == "airmass_precip"

    def test_needs_conn_in(self):
        assert ap_mod.NEEDS_CONN_IN is True

    def test_needs_weights(self):
        assert ap_mod.NEEDS_WEIGHTS is True

    def test_needs_all_obs(self):
        assert ap_mod.NEEDS_ALL_OBS is True

    def test_member_ids_contiguous(self):
        # member IDs must be 1..N with no gaps; member_id=0 is the ensemble mean
        ids = sorted(_MEMBER_IDS)
        assert ids == list(range(1, len(ids) + 1))
