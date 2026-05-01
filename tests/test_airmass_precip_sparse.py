"""Tests for airmass_precip model behavior with sparse/empty data."""
import models.airmass_precip as ap_mod
from tests.conftest import make_input_db, make_obs


class TestNoHistoricalData:
    def test_returns_36_rows(self):
        conn_in = make_input_db()
        obs = make_obs()
        rows = ap_mod.run(obs, obs["timestamp"], conn_in=conn_in)
        # 8 named members + member_id=0, each with 4 lead times = 36 rows
        assert len(rows) == 36

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

    def test_member_count(self):
        # 8 named members (1-8); member_id=0 is ensemble mean produced at runtime
        assert len(ap_mod._MEMBER_NAMES) == 8
        assert all(mid in {m[0] for m in ap_mod._MEMBER_NAMES} for mid in range(1, 9))
