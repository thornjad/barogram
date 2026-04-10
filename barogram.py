#!/usr/bin/env python3
# barogram — personal weather forecast ensemble
# requires Python 3.11+; no Windows support

import argparse
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import config as cfg
import db

CENTRAL = ZoneInfo("America/Chicago")

_COMPASS = [
    "N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE",
    "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW",
]


def _wind_dir(degrees: int | None) -> str:
    if degrees is None:
        return "—"
    return _COMPASS[round(degrees / 22.5) % 16]


def _temp(c: float | None) -> str:
    if c is None:
        return "—"
    return f"{c:.1f}°C ({c * 9/5 + 32:.1f}°F)"


def _val(v, spec=".1f", unit="") -> str:
    if v is None:
        return "—"
    return f"{v:{spec}}{unit}"


def _ts(epoch: int) -> str:
    return datetime.fromtimestamp(epoch, tz=CENTRAL).strftime("%Y-%m-%d %H:%M %Z")


def cmd_conditions(args, conf):
    try:
        conn = db.open_input_db(conf.input_db)
    except FileNotFoundError as e:
        sys.exit(f"error: {e}")
    try:
        db.validate_schema(conn)
    except ValueError as e:
        sys.exit(f"error: {e}")

    tempest = db.latest_tempest_obs(conn)
    nws = db.latest_nws_obs(conn)

    if tempest:
        name = tempest["name"] or tempest["station_id"]
        print(f"{name} ({tempest['station_id']}) — {_ts(tempest['timestamp'])}")
        print(f"  Temperature:   {_temp(tempest['air_temp'])}")
        print(f"  Humidity:      {_val(tempest['relative_humidity'], '.0f', '%')}")
        print(f"  Pressure:      {_val(tempest['station_pressure'], '.1f', ' mb')} (station)")
        gust = tempest["wind_gust"]
        gust_str = f", gusts to {_val(gust, '.1f', ' m/s')}" if gust is not None else ""
        print(f"  Wind:          {_wind_dir(tempest['wind_direction'])} "
              f"{_val(tempest['wind_avg'], '.1f', ' m/s')}{gust_str}")
        print(f"  Precipitation: {_val(tempest['precip_accum_day'], '.1f', ' mm')} today")
        print(f"  UV Index:      {_val(tempest['uv_index'], '.1f')}")
        print(f"  Solar:         {_val(tempest['solar_radiation'], '.0f', ' W/m²')}")
        lc = tempest["lightning_count"]
        print(f"  Lightning:     {lc if lc is not None else 0} strikes")
    else:
        print("no Tempest observations found")

    print()

    if nws:
        name = nws["name"] or nws["station_id"]
        print(f"NWS {nws['station_id']} ({name}) — {_ts(nws['timestamp'])}")
        print(f"  Temperature:   {_temp(nws['air_temp'])}")
        print(f"  Dewpoint:      {_temp(nws['dew_point'])}")
        print(f"  Humidity:      {_val(nws['relative_humidity'], '.0f', '%')}")
        print(f"  Wind:          {_wind_dir(nws['wind_direction'])} "
              f"{_val(nws['wind_speed'], '.1f', ' m/s')}")
        print(f"  Pressure:      {_val(nws['sea_level_pressure'], '.1f', ' mb')}")
        print(f"  Sky:           {nws['sky_cover'] or '—'}")
        print(f"  METAR:         {nws['raw_metar'] or '—'}")
    else:
        print("no NWS observations found")


def main():
    script_dir = Path(__file__).parent
    default_config = script_dir / "barogram.toml"

    parser = argparse.ArgumentParser(
        prog="barogram",
        description="personal weather forecast ensemble",
    )
    parser.add_argument(
        "--config",
        metavar="PATH",
        default=str(default_config),
        help="config file (default: barogram.toml next to this script)",
    )
    subparsers = parser.add_subparsers(dest="command", metavar="command")
    subparsers.add_parser("conditions", help="show latest observed conditions")

    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        sys.exit(0)

    conf = cfg.load(args.config)

    if args.command == "conditions":
        cmd_conditions(args, conf)


if __name__ == "__main__":
    main()
