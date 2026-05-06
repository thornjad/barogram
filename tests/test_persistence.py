import models.persistence as pm
from tests.conftest import make_obs


def _run(precip_accum_day=None):
    obs = make_obs()
    obs["precip_accum_day"] = precip_accum_day
    return pm.run(obs, 1_700_000_000)


def _precip_rows(rows):
    return [r for r in rows if r["variable"] == "precip_prob"]


def test_precip_prob_rain_returns_one():
    rows = _run(precip_accum_day=1.0)
    for r in _precip_rows(rows):
        assert r["value"] == 1.0, f"expected 1.0, got {r['value']}"


def test_precip_prob_dry_returns_zero():
    rows = _run(precip_accum_day=0.0)
    for r in _precip_rows(rows):
        assert r["value"] == 0.0


def test_precip_prob_none_returns_zero():
    rows = _run(precip_accum_day=None)
    for r in _precip_rows(rows):
        assert r["value"] == 0.0


def test_precip_prob_in_unit_interval():
    for accum in [None, 0.0, 0.001, 1.0, 50.0]:
        rows = _run(precip_accum_day=accum)
        for r in _precip_rows(rows):
            assert 0.0 <= r["value"] <= 1.0, f"precip_prob {r['value']} out of [0,1] for accum={accum}"


def test_precip_prob_negative_accum_returns_zero():
    """Negative accumulation (gauge reset or correction) is treated as dry."""
    rows = _run(precip_accum_day=-0.5)
    for r in _precip_rows(rows):
        assert r["value"] == 0.0


def test_lead_hours_coverage():
    rows = _run(precip_accum_day=0.0)
    leads = {r["lead_hours"] for r in _precip_rows(rows)}
    assert leads == {6, 12, 18, 24}
