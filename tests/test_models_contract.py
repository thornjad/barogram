import barogram
from tests.conftest import make_input_db, make_obs

_VALID_VARIABLES = {"temperature", "dewpoint", "pressure", "wind_speed"}
_VALID_LEAD_HOURS = {6, 12, 18, 24}
_REQUIRED_KEYS = {"model_id", "model", "issued_at", "valid_at", "lead_hours", "variable", "value"}


def test_all_models_satisfy_contract():
    obs = make_obs()
    issued_at = obs["timestamp"]
    conn_in = make_input_db()

    for model in barogram._MODELS:
        kwargs = {}
        if getattr(model, "NEEDS_CONN_IN", False):
            kwargs["conn_in"] = conn_in
        if getattr(model, "NEEDS_WEIGHTS", False):
            kwargs["weights"] = {}

        rows = model.run(obs, issued_at, **kwargs)

        assert len(rows) > 0, f"{model.MODEL_NAME}: run() returned no rows"

        seen_triples = set()
        for row in rows:
            missing = _REQUIRED_KEYS - row.keys()
            assert not missing, \
                f"{model.MODEL_NAME}: row missing keys {missing}"

            assert row["lead_hours"] in _VALID_LEAD_HOURS, \
                f"{model.MODEL_NAME}: invalid lead_hours {row['lead_hours']!r}"

            assert row["variable"] in _VALID_VARIABLES, \
                f"{model.MODEL_NAME}: invalid variable {row['variable']!r}"

            assert row["model_id"] == model.MODEL_ID, \
                f"{model.MODEL_NAME}: model_id {row['model_id']} != MODEL_ID {model.MODEL_ID}"

            triple = (row["lead_hours"], row["variable"], row.get("member_id", 0))
            assert triple not in seen_triples, \
                f"{model.MODEL_NAME}: duplicate (lead_hours, variable, member_id) {triple}"
            seen_triples.add(triple)
