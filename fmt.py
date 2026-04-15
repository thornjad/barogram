import math
from datetime import datetime
from zoneinfo import ZoneInfo

CENTRAL = ZoneInfo("America/Chicago")

_COMPASS = [
    "N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE",
    "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW",
]


def wind_dir(degrees: int | None) -> str:
    if degrees is None:
        return "\u2014"
    return _COMPASS[round(degrees / 22.5) % 16]


def temp(c: float | None) -> str:
    if c is None:
        return "\u2014"
    return f"{c * 9/5 + 32:.1f}\u00b0F"


def val(v, spec=".1f", unit="") -> str:
    if v is None:
        return "\u2014"
    return f"{v:{spec}}{unit}"


def ts(epoch: int) -> str:
    return datetime.fromtimestamp(epoch, tz=CENTRAL).strftime("%Y-%m-%d %H:%M %Z")


def short_ts(epoch: int) -> str:
    return datetime.fromtimestamp(epoch, tz=CENTRAL).strftime("%b %-d %H:%M")


def to_slp(station_p: float, air_temp_c: float, elevation_m: float) -> float:
    """Reduce station pressure to sea-level using the hypsometric formula."""
    T_K = air_temp_c + 273.15 + 0.0065 * elevation_m / 2
    return station_p * math.exp(9.80665 * elevation_m / (287.053 * T_K))
