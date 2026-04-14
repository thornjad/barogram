#!/usr/bin/env python3
# barogram — personal weather forecast ensemble
# requires Python 3.11+; no Windows support

import argparse
import sys
import time
from pathlib import Path

import config as cfg
import dashboard as dash
import db
import fmt
import models.climatological_mean as climatological_mean
import models.climo_deviation as climo_deviation
import models.persistence as persistence
import models.weighted_climatological_mean as weighted_climatological_mean

_MODELS = [persistence, climatological_mean, weighted_climatological_mean, climo_deviation]
import score as scorer


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
        print(f"{name} ({tempest['station_id']}) \u2014 {fmt.ts(tempest['timestamp'])}")
        print(f"  Temperature:   {fmt.temp(tempest['air_temp'])}")
        print(f"  Dewpoint:      {fmt.temp(tempest['dew_point'])}")
        print(f"  Pressure:      {fmt.val(tempest['station_pressure'], '.1f', ' mb')} (station)")
        gust = tempest["wind_gust"]
        gust_str = f", gusts to {fmt.val(gust, '.1f', ' m/s')}" if gust is not None else ""
        print(f"  Wind:          {fmt.wind_dir(tempest['wind_direction'])} "
              f"{fmt.val(tempest['wind_avg'], '.1f', ' m/s')}{gust_str}")
        print(f"  Precipitation: {fmt.val(tempest['precip_accum_day'], '.1f', ' mm')} today")
        print(f"  UV Index:      {fmt.val(tempest['uv_index'], '.1f')}")
        print(f"  Solar:         {fmt.val(tempest['solar_radiation'], '.0f', ' W/m\u00b2')}")
        lc = tempest["lightning_count"]
        print(f"  Lightning:     {lc if lc is not None else 0} strikes")
    else:
        print("no Tempest observations found")

    print()

    if nws:
        name = nws["name"] or nws["station_id"]
        print(f"NWS {nws['station_id']} ({name}) \u2014 {fmt.ts(nws['timestamp'])}")
        print(f"  Temperature:   {fmt.temp(nws['air_temp'])}")
        print(f"  Dewpoint:      {fmt.temp(nws['dew_point'])}")
        print(f"  Wind:          {fmt.wind_dir(nws['wind_direction'])} "
              f"{fmt.val(nws['wind_speed'], '.1f', ' m/s')}")
        print(f"  Pressure:      {fmt.val(nws['sea_level_pressure'], '.1f', ' mb')}")
        print(f"  Sky:           {nws['sky_cover'] or '\u2014'}")
        print(f"  METAR:         {nws['raw_metar'] or '\u2014'}")
    else:
        print("no NWS observations found")


def cmd_forecast(args, conf):
    issued_at = int(time.time())
    migrations_dir = Path(__file__).parent / "migrations"

    try:
        conn_in = db.open_input_db(conf.input_db)
    except FileNotFoundError as e:
        sys.exit(f"error: {e}")
    try:
        db.validate_schema(conn_in)
    except ValueError as e:
        sys.exit(f"error: {e}")

    conn_out = db.open_output_db(conf.output_db)
    db.run_migrations(conn_out, migrations_dir)

    obs = db.latest_tempest_obs(conn_in)
    if obs is None:
        sys.exit("error: no Tempest observations in input database")

    for model in _MODELS:
        if getattr(model, "NEEDS_CONN_IN", False):
            rows = model.run(obs, issued_at, conn_in=conn_in)
        else:
            rows = model.run(obs, issued_at)
        db.insert_forecasts(conn_out, rows)
        print(f"  {model.MODEL_NAME}: {len(rows)} rows")
    print("forecast complete")


def cmd_run(args, conf):
    issued_at = int(time.time())
    output = Path(__file__).parent / "dashboard.html"
    migrations_dir = Path(__file__).parent / "migrations"

    try:
        conn_in = db.open_input_db(conf.input_db)
    except FileNotFoundError as e:
        sys.exit(f"error: {e}")
    try:
        db.validate_schema(conn_in)
    except ValueError as e:
        sys.exit(f"error: {e}")

    conn_out = db.open_output_db(conf.output_db)
    db.run_migrations(conn_out, migrations_dir)

    print("scoring...")
    result = scorer.run(conn_in, conn_out)
    print(f"  scored {result['scored']}, skipped {result['skipped']}")

    obs = db.latest_tempest_obs(conn_in)
    if obs is None:
        sys.exit("error: no Tempest observations in input database")

    print("forecasting...")
    for model in _MODELS:
        if getattr(model, "NEEDS_CONN_IN", False):
            rows = model.run(obs, issued_at, conn_in=conn_in)
        else:
            rows = model.run(obs, issued_at)
        db.insert_forecasts(conn_out, rows)
        print(f"  {model.MODEL_NAME}: {len(rows)} rows")

    print("building dashboard...")
    dash.generate(conn_in, conn_out, output)
    print(f"  wrote {output}")


def cmd_dashboard(args, conf):
    output = Path(__file__).parent / "dashboard.html"
    migrations_dir = Path(__file__).parent / "migrations"

    try:
        conn_in = db.open_input_db(conf.input_db)
    except FileNotFoundError as e:
        sys.exit(f"error: {e}")
    try:
        db.validate_schema(conn_in)
    except ValueError as e:
        sys.exit(f"error: {e}")

    conn_out = db.open_output_db(conf.output_db)
    db.run_migrations(conn_out, migrations_dir)

    try:
        dash.generate(conn_in, conn_out, output)
    except ValueError as e:
        sys.exit(f"error: {e}")
    print(f"wrote {output}")


def cmd_score(args, conf):
    migrations_dir = Path(__file__).parent / "migrations"

    try:
        conn_in = db.open_input_db(conf.input_db)
    except FileNotFoundError as e:
        sys.exit(f"error: {e}")
    try:
        db.validate_schema(conn_in)
    except ValueError as e:
        sys.exit(f"error: {e}")

    conn_out = db.open_output_db(conf.output_db)
    db.run_migrations(conn_out, migrations_dir)

    result = scorer.run(conn_in, conn_out)
    print(f"scored {result['scored']}, skipped {result['skipped']}")


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
    subparsers.add_parser(
        "run", help="score pending forecasts, run models, and rebuild dashboard"
    )
    subparsers.add_parser("conditions", help="show latest observed conditions")
    subparsers.add_parser(
        "forecast", help="run forecast models and write to output database"
    )
    subparsers.add_parser(
        "dashboard", help="generate dashboard.html from latest forecast run"
    )
    subparsers.add_parser(
        "score", help="score past forecasts against observations"
    )

    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        sys.exit(0)

    conf = cfg.load(args.config)

    if args.command == "run":
        cmd_run(args, conf)
    elif args.command == "conditions":
        cmd_conditions(args, conf)
    elif args.command == "forecast":
        cmd_forecast(args, conf)
    elif args.command == "dashboard":
        cmd_dashboard(args, conf)
    elif args.command == "score":
        cmd_score(args, conf)


if __name__ == "__main__":
    main()
