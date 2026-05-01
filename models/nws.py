"""nws: external reference model using NWS hourly forecast from api.weather.gov.

Fetches NWS hourly forecasts and snaps them to the standard 6/12/18/24h lead
times. Covers temperature and dewpoint. Pressure is skipped because NWS provides
sea-level pressure while barogram scores against station pressure, and the
elevation correction would introduce systematic bias.

No API key required. The station lat/lon is derived from the wxlog stations table.
"""

import json
import sys
import urllib.request

import db

MODEL_ID = 200
MODEL_NAME = "nws"
NEEDS_CONN_IN = True
NEEDS_LOCATION = True

LEAD_HOURS = [6, 12, 18, 24]
# hourly NWS data — snap to nearest within ±90 min
_SNAP_WINDOW = 5400

_NWS_POINTS = "https://api.weather.gov/points/{lat:.4f},{lon:.4f}"
_HEADERS = {"User-Agent": "barogram/1.0"}


def _fetch(lat: float, lon: float) -> dict[int, dict]:
    """Fetch NWS hourly forecast keyed by unix timestamp. Returns {} on failure."""
    try:
        req = urllib.request.Request(_NWS_POINTS.format(lat=lat, lon=lon), headers=_HEADERS)
        with urllib.request.urlopen(req, timeout=10) as resp:
            points = json.loads(resp.read())
        hourly_url = points["properties"]["forecastHourly"]

        req = urllib.request.Request(hourly_url, headers=_HEADERS)
        with urllib.request.urlopen(req, timeout=45) as resp:
            hourly = json.loads(resp.read())

        result: dict[int, dict] = {}
        for period in hourly["properties"]["periods"]:
            from datetime import datetime
            ts = int(datetime.fromisoformat(period["startTime"]).timestamp())
            temp = period.get("temperature")
            if temp is None:
                continue
            unit = period.get("temperatureUnit", "F")
            temp_c = (temp - 32) * 5 / 9 if unit == "F" else float(temp)
            dew_c = (period.get("dewpoint") or {}).get("value")  # already °C
            result[ts] = {"temperature": temp_c, "dewpoint": dew_c}
        return result
    except Exception as e:
        print(f"nws: fetch failed: {e}", file=sys.stderr)
        return {}


def _nearest(hourly: dict[int, dict], target: int) -> dict | None:
    """Return the hourly entry nearest to target within _SNAP_WINDOW, or None."""
    if not hourly:
        return None
    best = min(hourly, key=lambda t: abs(t - target))
    if abs(best - target) > _SNAP_WINDOW:
        return None
    return hourly[best]


def run(obs, issued_at: int, *, conn_in=None, location=None) -> list[dict]:
    loc = location if location is not None else db.tempest_station_location(conn_in)
    if loc is None:
        return []
    hourly = _fetch(loc[0], loc[1])
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
