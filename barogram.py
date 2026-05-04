#!/usr/bin/env python3
# barogram — personal weather forecast ensemble
# requires Python 3.11+; no Windows support

import argparse
import sys
import time
import traceback
from collections import defaultdict
from pathlib import Path

import config as cfg
import dashboard as dash
import db
import fmt
import sync as _sync
import models.analog as analog
import models.airmass_diurnal as airmass_diurnal
import models.airmass_precip as airmass_precip
import models.bogo as bogo
import models.climatological_mean as climatological_mean
import models.climo_deviation as climo_deviation
import models.diurnal_curve as diurnal_curve
import models.ensemble as barogram_ensemble
import models.nws as nws_model
import models.persistence as persistence
import models.pressure_tendency as pressure_tendency
import models.surface_signs as surface_signs
import models.synoptic_state_machine as synoptic_state_machine
import models.tempest_forecast as tempest_forecast_model
import models.weighted_climatological_mean as weighted_climatological_mean

_MODELS = [
    persistence,
    climatological_mean,
    weighted_climatological_mean,
    climo_deviation,
    pressure_tendency,
    diurnal_curve,
    airmass_diurnal,
    airmass_precip,
    analog,
    surface_signs,
    synoptic_state_machine,
    bogo,
    nws_model,
    tempest_forecast_model,
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

    shared_all_obs = (
        db.tempest_obs_in_range(conn_in, 0, issued_at)
        if any(getattr(m, "NEEDS_ALL_OBS", False) for m in _MODELS)
        else None
    )
    shared_location = (
        db.tempest_station_location(conn_in)
        if any(getattr(m, "NEEDS_LOCATION", False) for m in _MODELS)
        else None
    )

    failed = []
    for model in _MODELS:
        kwargs = {}
        if getattr(model, "NEEDS_CONF", False):
            kwargs["conf"] = conf
        if getattr(model, "NEEDS_CONN_IN", False):
            kwargs["conn_in"] = conn_in
        if getattr(model, "NEEDS_CONN_OUT", False):
            kwargs["conn_out"] = conn_out
        if getattr(model, "NEEDS_WEIGHTS", False):
            kwargs["weights"] = db.load_weights(conn_out, model.MODEL_ID)
        if getattr(model, "NEEDS_ALL_OBS", False):
            kwargs["all_obs"] = shared_all_obs
        if getattr(model, "NEEDS_LOCATION", False):
            kwargs["location"] = shared_location
        try:
            rows = model.run(obs, issued_at, **kwargs)
            db.insert_forecasts(conn_out, rows)
            print(f"  {model.MODEL_NAME}: {len(rows)} rows")
        except Exception as e:
            print(f"  {model.MODEL_NAME}: ERROR — {e}", file=sys.stderr)
            traceback.print_exc(file=sys.stderr)
            failed.append(model.MODEL_NAME)
    db.set_metadata(conn_out, "last_forecast", str(issued_at))
    if failed:
        print(f"forecast complete with errors: {', '.join(failed)}", file=sys.stderr)
        sys.exit(1)
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

    shared_all_obs = (
        db.tempest_obs_in_range(conn_in, 0, issued_at)
        if any(getattr(m, "NEEDS_ALL_OBS", False) for m in _MODELS)
        else None
    )
    shared_location = (
        db.tempest_station_location(conn_in)
        if any(getattr(m, "NEEDS_LOCATION", False) for m in _MODELS)
        else None
    )

    print("forecasting...")
    failed = []
    for model in _MODELS:
        kwargs = {}
        if getattr(model, "NEEDS_CONF", False):
            kwargs["conf"] = conf
        if getattr(model, "NEEDS_CONN_IN", False):
            kwargs["conn_in"] = conn_in
        if getattr(model, "NEEDS_CONN_OUT", False):
            kwargs["conn_out"] = conn_out
        if getattr(model, "NEEDS_WEIGHTS", False):
            kwargs["weights"] = db.load_weights(conn_out, model.MODEL_ID)
        if getattr(model, "NEEDS_ALL_OBS", False):
            kwargs["all_obs"] = shared_all_obs
        if getattr(model, "NEEDS_LOCATION", False):
            kwargs["location"] = shared_location
        try:
            rows = model.run(obs, issued_at, **kwargs)
            db.insert_forecasts(conn_out, rows)
            print(f"  {model.MODEL_NAME}: {len(rows)} rows")
        except Exception as e:
            print(f"  {model.MODEL_NAME}: ERROR — {e}", file=sys.stderr)
            traceback.print_exc(file=sys.stderr)
            failed.append(model.MODEL_NAME)
    db.set_metadata(conn_out, "last_forecast", str(issued_at))
    if failed:
        print(f"forecast errors: {', '.join(failed)}", file=sys.stderr)

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


def _convert_forecast_value(var: str, val: float | None) -> float | None:
    if val is None:
        return None
    if var in ("temperature", "dewpoint"):
        return val * 9 / 5 + 32
    return val


def _convert_error_value(var: str, val: float | None) -> float | None:
    if val is None:
        return None
    if var in ("temperature", "dewpoint"):
        return val * 1.8
    return val


def _print_insights_table(result: dict) -> None:
    print(f"generated:    {fmt.ts(result['generated_at'])}")
    print(f"scored runs (alltime):  {result['n_scored_runs_alltime']}")
    print(f"accuracy window:        {result['accuracy_window_runs']} runs")

    ef = result.get("ensemble_forecast")
    if ef:
        print(f"\nensemble forecast  (issued {fmt.ts(ef['issued_at'])})")
        header = f"  {'lead':>4}  {'temp (F)':>10}  {'dewpt (F)':>10}  {'pres (hPa)':>11}"
        print(header)
        print("  " + "-" * (len(header) - 2))
        for lead in sorted(ef["leads"], key=int):
            lv = ef["leads"][lead]
            t = fmt.val(lv.get("temperature"), ".1f")
            d = fmt.val(lv.get("dewpoint"), ".1f")
            p = fmt.val(lv.get("pressure"), ".1f")
            print(f"  {lead+'h':>4}  {t:>10}  {d:>10}  {p:>11}")

    accuracy = result.get("model_accuracy", {})
    if accuracy:
        print("\nmodel accuracy  (last 10 scored runs, converted units)")
        for model_name, variables in sorted(accuracy.items()):
            print(f"\n  {model_name}")
            print(f"    {'variable':14}  {'6h MAE':>8}  {'12h MAE':>8}  {'18h MAE':>8}  {'24h MAE':>8}"
                  f"  {'6h bias':>8}  {'12h bias':>8}  {'18h bias':>8}  {'24h bias':>8}")
            print("    " + "-" * 102)
            for var, stats in sorted(variables.items()):
                def _f(k):
                    v = stats.get(k)
                    return f"{v:.3f}" if v is not None else "—"
                row = (f"    {var:14}"
                       f"  {_f('mae_6h'):>8}  {_f('mae_12h'):>8}  {_f('mae_18h'):>8}  {_f('mae_24h'):>8}"
                       f"  {_f('bias_6h'):>8}  {_f('bias_12h'):>8}  {_f('bias_18h'):>8}  {_f('bias_24h'):>8}")
                print(row)


def _compute_slp_offset(conf) -> float:
    """Return the station→SLP pressure offset (hPa) from the latest Tempest obs.

    Mirrors dashboard._slp_correction. Returns 0.0 on any failure so callers
    can proceed with station pressure unchanged.
    """
    try:
        conn_in = db.open_input_db(conf.input_db)
    except (FileNotFoundError, Exception):
        return 0.0
    try:
        obs = db.latest_tempest_obs(conn_in)
        if obs is None:
            return 0.0
        sp = obs["station_pressure"]
        if sp is None:
            return 0.0
        try:
            slp_stored = obs["sea_level_pressure"]
            if slp_stored is not None:
                return slp_stored - sp
        except (IndexError, KeyError):
            pass
        elevation_m = db.tempest_station_elevation(conn_in)
        if elevation_m <= 0.0:
            return 0.0
        temp = obs["air_temp"]
        if temp is None:
            return 0.0
        return fmt.to_slp(sp, temp, elevation_m) - sp
    except Exception:
        return 0.0


def cmd_insights(args, conf):
    import json as json_mod

    output_db_path = Path(conf.output_db)
    if not output_db_path.exists():
        print("{}")
        return

    migrations_dir = Path(__file__).parent / "migrations"
    conn_out = db.open_output_db(conf.output_db)
    db.run_migrations(conn_out, migrations_dir)

    count_row = conn_out.execute("select count(*) as n from forecasts").fetchone()
    if count_row["n"] == 0:
        print("{}")
        return

    generated_at = int(time.time())
    n_scored_runs = db.accuracy_run_count(conn_out, 0)
    slp_offset = _compute_slp_offset(conf)

    all_latest = db.latest_forecast_per_model(conn_out)
    ens_rows = [r for r in all_latest
                if r["model"] == "barogram_ensemble" and r["member_id"] == 0]

    ens_issued_at = ens_rows[0]["issued_at"] if ens_rows else None
    leads: dict = {}
    for r in ens_rows:
        lead = str(r["lead_hours"])
        if lead not in leads:
            leads[lead] = {}
        var = r["variable"]
        raw_val = r["value"]
        if var == "pressure" and raw_val is not None:
            raw_val = raw_val + slp_offset
        val = _convert_forecast_value(var, raw_val)
        spread = _convert_error_value(var, r["spread"])
        leads[lead][var] = round(val, 2) if val is not None else None
        if spread is not None:
            leads[lead][f"{var}_spread"] = round(spread, 2)

    ensemble_forecast = {"issued_at": ens_issued_at, "leads": leads} if ens_issued_at else None

    _TARGET_MODELS = {"nws", "tempest_forecast", "barogram_ensemble"}
    _ALL_LEADS = [6, 12, 18, 24]
    _ACCURACY_WINDOW = 10
    summary = db.score_summary_last_n_runs(conn_out, _ACCURACY_WINDOW)
    summary = [r for r in summary if r["member_id"] == 0 and r["model"] in _TARGET_MODELS]

    accuracy: dict = {}
    for r in summary:
        model_name = r["model"]
        var = r["variable"]
        lead = r["lead_hours"]
        mae = _convert_error_value(var, r["avg_mae"])
        bias = _convert_error_value(var, r["avg_bias"])
        accuracy.setdefault(model_name, {})
        if var not in accuracy[model_name]:
            accuracy[model_name][var] = {f"mae_{l}h": None for l in _ALL_LEADS}
            accuracy[model_name][var].update({f"bias_{l}h": None for l in _ALL_LEADS})
        accuracy[model_name][var][f"mae_{lead}h"] = round(mae, 3) if mae is not None else None
        accuracy[model_name][var][f"bias_{lead}h"] = round(bias, 3) if bias is not None else None

    result: dict = {
        "generated_at": generated_at,
        "n_scored_runs_alltime": n_scored_runs,
        "accuracy_window_runs": _ACCURACY_WINDOW,
    }
    if ensemble_forecast:
        result["ensemble_forecast"] = ensemble_forecast
    result["model_accuracy"] = accuracy

    if args.format == "json":
        print(json_mod.dumps(result, indent=2))
    else:
        _print_insights_table(result)


def cmd_tune(args, conf):
    _sync_check()
    migrations_dir = Path(__file__).parent / "migrations"
    conn_out = db.open_output_db(conf.output_db)
    db.run_migrations(conn_out, migrations_dir)

    _SECTOR_LABELS = {0: "night 00-05", 1: "morning 06-11", 2: "afternoon 12-17", 3: "evening 18-23"}

    ensemble_model_ids = {m.MODEL_ID for m in _MODELS if getattr(m, "NEEDS_WEIGHTS", False)}

    summary = db.score_summary(conn_out)
    sector_summary = db.score_summary_by_sector(conn_out)

    # pooled MAE across all sectors; only qualifying cells
    pooled_mae = {
        (r["model_id"], r["member_id"], r["variable"], r["lead_hours"]): r["avg_mae"]
        for r in summary
        if r["model_id"] in ensemble_model_ids
        and r["member_id"] > 0
        and r["n"] >= args.min_runs
        and r["avg_mae"] is not None
        and r["avg_mae"] > 0
    }
    model_names = {r["model_id"]: r["model"] for r in summary if r["model_id"] in ensemble_model_ids}

    if not pooled_mae:
        print(f"no qualifying data (need >= {args.min_runs} scored rows per cell; "
              f"run 'score' first or lower --min-runs)")
        return

    # sector MAE: (model_id, member_id, variable, lead_hours, sector) -> (avg_mae, n)
    sector_mae = {}
    for r in sector_summary:
        if r["model_id"] not in ensemble_model_ids or r["member_id"] <= 0:
            continue
        sector_mae[(r["model_id"], r["member_id"], r["variable"],
                    r["lead_hours"], r["sector"])] = (r["avg_mae"], r["n"])

    # group pooled data by (model_id, variable, lead_hours) -> {member_id: mae}
    pooled_groups: dict = defaultdict(dict)
    for (model_id, member_id, variable, lead_hours), mae in pooled_mae.items():
        pooled_groups[(model_id, variable, lead_hours)][member_id] = mae

    all_weights = {model_id: {} for model_id in ensemble_model_ids}
    for (model_id, variable, lead_hours), pooled_by_member in pooled_groups.items():
        for sector in range(4):
            blended = {}
            for mid, p_mae in pooled_by_member.items():
                s_data = sector_mae.get((model_id, mid, variable, lead_hours, sector))
                if s_data and s_data[1] >= args.min_runs and s_data[0] > 0:
                    blended[mid] = (1 - args.pool_alpha) * s_data[0] + args.pool_alpha * p_mae
                else:
                    blended[mid] = p_mae
            raw = {mid: 1.0 / mae for mid, mae in blended.items()}
            raw_total = sum(raw.values())
            fractions = {mid: w / raw_total for mid, w in raw.items()}
            n_members = len(fractions)
            min_w = args.floor / n_members
            floored = {mid: max(f, min_w) for mid, f in fractions.items()}
            if bogo.MODEL_ID in floored:
                floored[bogo.MODEL_ID] = min_w  # bogo always gets minimum weight
            floored_total = sum(floored.values())
            final = {mid: w / floored_total for mid, w in floored.items()}
            for mid, w in final.items():
                all_weights[model_id][(mid, variable, lead_hours, sector)] = w

    for model_id in sorted(all_weights):
        name = model_names.get(model_id, f"model {model_id}")
        if not all_weights[model_id]:
            print(f"\n{name}: no qualifying data")
            continue
        print(f"\npool_alpha={args.pool_alpha:.2f}")
        print(f"{name}:")
        cur_sector = None
        for key in sorted(all_weights[model_id], key=lambda k: (k[3], k[2], k[0], k[1])):
            mid, variable, lead_hours, sector = key
            weight = all_weights[model_id][key]
            if sector != cur_sector:
                print(f"  sector={sector} ({_SECTOR_LABELS[sector]}):")
                cur_sector = sector
            group_size = len(pooled_groups.get((model_id, variable, lead_hours), {}))
            equal_w = 1.0 / group_size if group_size else 1.0
            print(f"    member={mid:2d}  {variable:12s}  lead={lead_hours:2d}h  "
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
        "insights", help="emit forecast and accuracy summary as JSON"
    )
    p.add_argument(
        "--format", choices=["json", "table"], default="json",
        help="output format (default: json)",
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
        "--pool-alpha", type=float, default=0.10, metavar="A",
        help="pooled-weight blend fraction (0–1); permanent floor mixing sector and "
             "all-data weights (default: 0.10)",
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
    elif args.command == "insights":
        cmd_insights(args, conf)
    elif args.command == "tune":
        cmd_tune(args, conf)


if __name__ == "__main__":
    main()
