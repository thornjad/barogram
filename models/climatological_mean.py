# climatological mean: forecast = historical average for (month, hour) bucket
# computed from all accumulated local Tempest observations

import datetime as dt

import db

MODEL_ID = 2
MODEL_NAME = "climatological_mean"
NEEDS_CONN_IN = True

LEAD_HOURS = [6, 12, 18, 24]
MIN_OBS = 30

# barogram variable name -> tempest_obs column name
VARIABLES = {
    "temperature": "air_temp",
    "dewpoint": "dew_point",
    "pressure": "station_pressure",
    "wind_speed": "wind_avg",
}


def run(obs, issued_at: int, *, conn_in) -> list[dict]:
    rows = []
    for lead in LEAD_HOURS:
        valid_at = obs["timestamp"] + lead * 3600
        t = dt.datetime.fromtimestamp(valid_at)  # system local, matches SQLite 'localtime'
        means = db.climo_bucket_means(conn_in, t.month, t.hour, MIN_OBS)
        for variable in VARIABLES:
            rows.append({
                "model_id": MODEL_ID,
                "model": MODEL_NAME,
                "issued_at": issued_at,
                "valid_at": valid_at,
                "lead_hours": lead,
                "variable": variable,
                "value": means.get(variable),  # None when bucket is missing or thin
            })
    return rows
