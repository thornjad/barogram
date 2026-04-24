"""tempest_forecast: external reference model using Tempest's built-in forecast.

Fetches the WeatherFlow/Tempest better_forecast hourly data and snaps it to the
standard 6/12/18/24h lead times. Covers temperature, dewpoint, and wind speed.
Dewpoint is derived from forecasted air_temperature and relative_humidity via the
August-Roche-Magnus formula, matching how wxlog derives it from sensor data.

Requires [tempest] station_id and token in barogram.toml. Returns [] silently if
not configured, so forecast runs succeed on machines without Tempest credentials.
"""

import json
import math
import urllib.request

MODEL_ID = 201
MODEL_NAME = "tempest_forecast"
NEEDS_CONF = True

LEAD_HOURS = [6, 12, 18, 24]
# hourly data — snap to nearest within ±90 min
_SNAP_WINDOW = 5400

_API = (
    "https://swd.weatherflow.com/swd/rest/better_forecast"
    "?station_id={station_id}&token={token}"
    "&units_temp=c&units_wind=mps&units_pressure=mb&units_distance=km"
)


def _dewpoint_from_rh(temp_c: float, rh: float) -> float | None:
    """Compute dewpoint (°C) from temperature (°C) and relative humidity (%).
    Uses the August-Roche-Magnus approximation."""
    if rh is None or rh <= 0 or temp_c is None:
        return None
    lnrh = math.log(rh / 100.0)
    return (243.04 * (lnrh + (17.625 * temp_c) / (243.04 + temp_c))) / (
        17.625 - lnrh - (17.625 * temp_c) / (243.04 + temp_c)
    )


def _fetch(station_id: str, token: str) -> dict[int, dict]:
    """Fetch Tempest hourly forecast keyed by unix timestamp. Returns {} on failure."""
    try:
        url = _API.format(station_id=station_id, token=token)
        req = urllib.request.Request(url, headers={"User-Agent": "barogram/1.0"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
        result: dict[int, dict] = {}
        for entry in data.get("forecast", {}).get("hourly", []):
            ts = entry.get("time")
            if ts is None:
                continue
            temp = entry.get("air_temperature")
            rh = entry.get("relative_humidity")
            result[int(ts)] = {
                "temperature": temp,
                "dewpoint": _dewpoint_from_rh(temp, rh),
                "wind_speed": entry.get("wind_avg"),
            }
        return result
    except Exception:
        return {}


def _nearest(hourly: dict[int, dict], target: int) -> dict | None:
    """Return the hourly entry nearest to target within _SNAP_WINDOW, or None."""
    if not hourly:
        return None
    best = min(hourly, key=lambda t: abs(t - target))
    if abs(best - target) > _SNAP_WINDOW:
        return None
    return hourly[best]


def run(obs, issued_at: int, *, conf=None) -> list[dict]:
    if conf is None or not conf.tempest_token or not conf.tempest_station_id:
        return []
    hourly = _fetch(conf.tempest_station_id, conf.tempest_token)
    if not hourly:
        return []
    rows = []
    for lead in LEAD_HOURS:
        target = issued_at + lead * 3600
        entry = _nearest(hourly, target)
        if entry is None:
            continue
        for variable, key in [
            ("temperature", "temperature"),
            ("dewpoint", "dewpoint"),
            ("wind_speed", "wind_speed"),
        ]:
            if entry.get(key) is None:
                continue
            rows.append({
                "model_id": MODEL_ID,
                "model": MODEL_NAME,
                "member_id": 0,
                "issued_at": issued_at,
                "valid_at": target,
                "lead_hours": lead,
                "variable": variable,
                "value": entry[key],
            })
    return rows
