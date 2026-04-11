# ensemble model: combined forecast from all base models
# stub until >= 2 base models exist; run() returns []

MODEL_ID = 100
MODEL_NAME = "ensemble"
MODEL_TYPE = "ensemble"


def run(base_forecast_rows: list[dict], issued_at: int) -> list[dict]:
    """Combine base model forecasts into a single ensemble forecast.

    Returns [] until multiple base models are available. When implemented,
    weights each base model by its rolling MAE to produce one combined
    forecast row per (variable, lead_hours).
    """
    return []
