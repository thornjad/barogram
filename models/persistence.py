# persistence model: forecast = current observed value for all lead times
# simplest possible baseline; every other model is evaluated against this one

MODEL_ID = 1
MODEL_NAME = "persistence"

LEAD_HOURS = [6, 12, 18, 24]

# barogram variable name -> tempest_obs column name
VARIABLES = {
    "temperature": "air_temp",
    "dewpoint": "dew_point",
    "pressure": "station_pressure",
}


def run(obs, issued_at: int) -> list[dict]:
    """
    Produce persistence forecasts from a single observation row.

    For each lead time and variable, the forecast value equals the current
    observed value. This is the null hypothesis — any useful model must
    outperform it.
    """
    rows = []
    for lead in LEAD_HOURS:
        valid_at = obs["timestamp"] + lead * 3600
        for variable, col in VARIABLES.items():
            rows.append({
                "model_id": MODEL_ID,
                "model": MODEL_NAME,
                "issued_at": issued_at,
                "valid_at": valid_at,
                "lead_hours": lead,
                "variable": variable,
                "value": obs[col],
            })
    return rows
