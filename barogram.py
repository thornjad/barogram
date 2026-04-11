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
import models.persistence as persistence
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
        print(f"  Humidity:      {fmt.val(tempest['relative_humidity'], '.0f', '%')}")
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
        print(f"  Humidity:      {fmt.val(nws['relative_humidity'], '.0f', '%')}")
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

    rows = persistence.run(obs, issued_at)
    db.insert_forecasts(conn_out, rows)

    valid_start = fmt.ts(obs["timestamp"] + 6 * 3600)
    valid_end = fmt.ts(obs["timestamp"] + 24 * 3600)
    print(f"persistence: {len(rows)} forecasts issued at {fmt.ts(issued_at)}")
    print(f"  obs:   {fmt.ts(obs['timestamp'])}")
    print(f"  valid: {valid_start} \u2014 {valid_end}")


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

    print(f"dashboard written to {output}")


_SCORE_UNIT = {
    "temperature": "\u00b0C",
    "humidity": "%",
    "pressure": "mb",
    "wind_speed": "m/s",
}
_SCORE_VAR_ORDER = ["temperature", "humidity", "pressure", "wind_speed"]


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
    print(
        f"scored {result['scored']} forecasts, "
        f"skipped {result['skipped']} (no obs within window)"
    )

    summary = db.score_summary(conn_out)
    if not summary:
        return

    # group: (model, type) -> variable -> lead_hours -> (n, avg_mae, avg_bias)
    by_model: dict = {}
    for row in summary:
        m, t, v, l = row["model"], row["type"], row["variable"], row["lead_hours"]
        by_model.setdefault((m, t), {}).setdefault(v, {})[l] = (
            row["n"], row["avg_mae"], row["avg_bias"]
        )

    # base models first, then ensemble
    sorted_models = sorted(
        by_model.keys(), key=lambda k: (0 if k[1] == "base" else 1, k[0])
    )
    for (model, model_type), var_data in sorted_models:
        leads = sorted({l for vd in var_data.values() for l in vd})
        total_n = sum(r["n"] for r in summary if r["model"] == model)
        lead_header = "  ".join(f"+{l}h".ljust(14) for l in leads)
        type_tag = " [ensemble]" if model_type == "ensemble" else ""
        print(f"\n{model}{type_tag} \u2014 MAE / bias ({total_n} scored):")
        print(f"  {'variable':<12}  {lead_header}")
        for var in _SCORE_VAR_ORDER:
            if var not in var_data:
                continue
            unit = _SCORE_UNIT.get(var, "")
            cells = []
            for l in leads:
                if l in var_data[var]:
                    _, mae, bias = var_data[var][l]
                    sign = "+" if bias >= 0 else ""
                    cells.append(f"{mae:.2f} / {sign}{bias:.2f}".ljust(14))
                else:
                    cells.append("\u2014".ljust(14))
            print(f"  {var:<12}  {'  '.join(cells)}  ({unit})")


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

    if args.command == "conditions":
        cmd_conditions(args, conf)
    elif args.command == "forecast":
        cmd_forecast(args, conf)
    elif args.command == "dashboard":
        cmd_dashboard(args, conf)
    elif args.command == "score":
        cmd_score(args, conf)


if __name__ == "__main__":
    main()
