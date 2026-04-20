#!/usr/bin/env python3
# barogram — personal weather forecast ensemble
# requires Python 3.11+; no Windows support

import argparse
import sys
import time
from collections import defaultdict
from pathlib import Path

import config as cfg
import dashboard as dash
import db
import fmt
import sync as _sync
import models.airmass_diurnal as airmass_diurnal
import models.climatological_mean as climatological_mean
import models.climo_deviation as climo_deviation
import models.diurnal_curve as diurnal_curve
import models.ensemble as barogram_ensemble
import models.persistence as persistence
import models.pressure_tendency as pressure_tendency
import models.weighted_climatological_mean as weighted_climatological_mean

_MODELS = [
    persistence,
    climatological_mean,
    weighted_climatological_mean,
    climo_deviation,
    pressure_tendency,
    diurnal_curve,
    airmass_diurnal,
    barogram_ensemble,  # must be last: reads base model rows from current run
]
import score as scorer

_LOCAL_ENV = Path(__file__).parent / "barogram.local.toml"


def _sync_check():
    conf = _sync.load_env(_LOCAL_ENV)
    if conf is None:
        return
    if not _sync.wait_for_idle(conf):
        print("warning: syncthing not idle or unreachable — proceeding anyway")


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
        print(f"  Pressure:      {fmt.val(tempest['station_pressure'], '.1f', ' hPa')} (station)")
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
        print(f"  Pressure:      {fmt.val(nws['sea_level_pressure'], '.1f', ' hPa')}")
        print(f"  Sky:           {nws['sky_cover'] or '\u2014'}")
        print(f"  METAR:         {nws['raw_metar'] or '\u2014'}")
    else:
        print("no NWS observations found")


def cmd_forecast(args, conf):
    _sync_check()
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
        kwargs = {}
        if getattr(model, "NEEDS_CONN_IN", False):
            kwargs["conn_in"] = conn_in
        if getattr(model, "NEEDS_CONN_OUT", False):
            kwargs["conn_out"] = conn_out
        if getattr(model, "NEEDS_WEIGHTS", False):
            kwargs["weights"] = db.load_weights(conn_out, model.MODEL_ID)
        rows = model.run(obs, issued_at, **kwargs)
        db.insert_forecasts(conn_out, rows)
        print(f"  {model.MODEL_NAME}: {len(rows)} rows")
    db.set_metadata(conn_out, "last_forecast", str(issued_at))
    print("forecast complete")


def cmd_run(args, conf):
    _sync_check()
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
        kwargs = {}
        if getattr(model, "NEEDS_CONN_IN", False):
            kwargs["conn_in"] = conn_in
        if getattr(model, "NEEDS_CONN_OUT", False):
            kwargs["conn_out"] = conn_out
        if getattr(model, "NEEDS_WEIGHTS", False):
            kwargs["weights"] = db.load_weights(conn_out, model.MODEL_ID)
        rows = model.run(obs, issued_at, **kwargs)
        db.insert_forecasts(conn_out, rows)
        print(f"  {model.MODEL_NAME}: {len(rows)} rows")
    db.set_metadata(conn_out, "last_forecast", str(issued_at))

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
    _sync_check()
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


def cmd_query(args, conf):
    import json as json_mod

    if args.input:
        try:
            conn = db.open_input_db(conf.input_db)
        except FileNotFoundError as e:
            sys.exit(f"error: {e}")
    else:
        migrations_dir = Path(__file__).parent / "migrations"
        conn = db.open_output_db(conf.output_db)
        db.run_migrations(conn, migrations_dir)

    try:
        cur = conn.execute(args.sql)
    except Exception as e:
        sys.exit(f"error: {e}")

    rows = cur.fetchall()
    if not rows:
        print("(no rows)")
        return

    cols = list(rows[0].keys())

    if args.format == "json":
        print(json_mod.dumps([dict(r) for r in rows], indent=2))
        return

    str_rows = [[str(r[c]) if r[c] is not None else "" for c in cols] for r in rows]
    widths = []
    for i, c in enumerate(cols):
        col_vals = [row[i] for row in str_rows]
        widths.append(max(len(c), max((len(v) for v in col_vals), default=0)))

    def fmt_row(vals):
        return "  ".join(v.ljust(w) for v, w in zip(vals, widths))

    print(fmt_row(cols))
    print("  ".join("-" * w for w in widths))
    for row in str_rows:
        print(fmt_row(row))


def cmd_tune(args, conf):
    _sync_check()
    migrations_dir = Path(__file__).parent / "migrations"
    conn_out = db.open_output_db(conf.output_db)
    db.run_migrations(conn_out, migrations_dir)

    ensemble_model_ids = {m.MODEL_ID for m in _MODELS if getattr(m, "NEEDS_WEIGHTS", False)}

    summary = db.score_summary(conn_out)

    scored = [
        r for r in summary
        if r["model_id"] in ensemble_model_ids
        and r["member_id"] > 0
        and r["n"] >= args.min_runs
        and r["avg_mae"] is not None
        and r["avg_mae"] > 0
    ]

    if not scored:
        print(f"no qualifying data (need >= {args.min_runs} scored rows per cell; "
              f"run 'score' first or lower --min-runs)")
        return

    groups = defaultdict(dict)
    model_names = {}
    for r in scored:
        groups[(r["model_id"], r["variable"], r["lead_hours"])][r["member_id"]] = r["avg_mae"]
        model_names[r["model_id"]] = r["model"]

    all_weights = {model_id: {} for model_id in ensemble_model_ids}
    for (model_id, variable, lead_hours), mae_by_member in groups.items():
        raw = {mid: 1.0 / mae for mid, mae in mae_by_member.items()}
        raw_total = sum(raw.values())
        fractions = {mid: w / raw_total for mid, w in raw.items()}

        n = len(fractions)
        min_w = args.floor / n
        floored = {mid: max(f, min_w) for mid, f in fractions.items()}
        floored_total = sum(floored.values())
        final = {mid: w / floored_total for mid, w in floored.items()}

        for mid, w in final.items():
            all_weights[model_id][(mid, variable, lead_hours)] = w

    for model_id in sorted(all_weights):
        name = model_names.get(model_id, f"model {model_id}")
        if not all_weights[model_id]:
            print(f"\n{name}: no qualifying data")
            continue
        print(f"\n{name}:")
        for (mid, variable, lead_hours), weight in sorted(all_weights[model_id].items()):
            n = len(groups[(model_id, variable, lead_hours)])
            equal_w = 1.0 / n
            print(f"  member={mid:2d}  {variable:12s}  lead={lead_hours:2d}h  "
                  f"weight={weight:.4f}  (equal={equal_w:.4f})")

    if args.dry_run:
        print("\n(dry run — no changes written)")
        return

    now = int(time.time())
    for model_id, weights_dict in all_weights.items():
        if weights_dict:
            db.save_weights(conn_out, model_id, weights_dict, now)
    db.set_metadata(conn_out, "last_tune", str(now))
    total = sum(len(v) for v in all_weights.values())
    print(f"\nwrote {total} weight rows")


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
    p = subparsers.add_parser(
        "query", help="run a SQL query against the output or input database"
    )
    p.add_argument("sql", help="SQL query to execute")
    p.add_argument(
        "--input", action="store_true",
        help="query the input (wxlog) database instead of barogram.db",
    )
    p.add_argument(
        "--format", choices=["table", "json"], default="table",
        help="output format (default: table)",
    )
    p = subparsers.add_parser(
        "tune", help="compute inverse-MAE member weights from scoring history"
    )
    p.add_argument(
        "--min-runs", type=int, default=3, metavar="N",
        help="min scored rows per (member, variable, lead) to include (default: 3)",
    )
    p.add_argument(
        "--floor", type=float, default=0.5, metavar="F",
        help="min weight as a multiple of equal weight share (default: 0.5)",
    )
    p.add_argument(
        "--dry-run", action="store_true",
        help="print weights without writing to database",
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
    elif args.command == "query":
        cmd_query(args, conf)
    elif args.command == "tune":
        cmd_tune(args, conf)


if __name__ == "__main__":
    main()
