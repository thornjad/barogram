import json
import re
import sqlite3
import time
import urllib.request
from datetime import date, datetime, timezone
from pathlib import Path

import db
import fmt
import models.pressure_tendency as pressure_tendency

VARIABLES = ["temperature", "dewpoint", "pressure", "precip_prob"]

_VARIABLE_LABEL = {
    "temperature": "Temperature",
    "dewpoint": "Dew Point",
    "pressure": "Pressure",
    "precip_prob": "Precip Prob",
}

_UNIT = {
    "temperature": "\u00b0F",
    "dewpoint": "\u00b0F",
    "pressure": "hPa",
    "precip_prob": "%",
}

_FMT = {
    "temperature": ".1f",
    "dewpoint": ".1f",
    "pressure": ".1f",
    "precip_prob": ".0f",
}


def _to_f(c):
    return None if c is None else c * 9 / 5 + 32


def _to_pct(v):
    return None if v is None else v * 100


def _to_mph(ms):
    return None if ms is None else ms * 2.23694


def _diff_to_f(v):
    return None if v is None else v * 1.8


def _to_in(mm):
    return None if mm is None else mm / 25.4



def _slp_correction(obs, elevation_m: float = 0.0) -> float:
    """Derive the station→SLP pressure offset (hPa) from the latest tempest obs.

    Prefers the stored sea_level_pressure if available; otherwise computes
    from the barometric formula using station pressure, temperature, and
    the configured station elevation.
    """
    if obs is None:
        return 0.0
    sp = obs["station_pressure"]
    if sp is None:
        return 0.0
    slp_stored = obs["sea_level_pressure"]
    if slp_stored is not None:
        return slp_stored - sp
    if elevation_m <= 0.0:
        return 0.0
    temp = obs["air_temp"]
    if temp is None:
        return 0.0
    return fmt.to_slp(sp, temp, elevation_m) - sp


def _fetch_nws_forecast(lat: float, lon: float) -> dict[int, dict]:
    """Fetch NWS hourly forecasts keyed by unix timestamp (SI units). Returns {} on failure."""
    try:
        req = urllib.request.Request(
            f"https://api.weather.gov/points/{lat:.4f},{lon:.4f}",
            headers={"User-Agent": "barogram/1.0"},
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            points = json.loads(resp.read())
        hourly_url = points["properties"]["forecastHourly"]

        req = urllib.request.Request(hourly_url, headers={"User-Agent": "barogram/1.0"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            hourly = json.loads(resp.read())

        result: dict[int, dict] = {}
        for period in hourly["properties"]["periods"]:
            ts = int(datetime.fromisoformat(period["startTime"]).timestamp())
            temp = period.get("temperature")
            if temp is None:
                continue
            unit = period.get("temperatureUnit", "F")
            temp_c = (temp - 32) * 5 / 9 if unit == "F" else float(temp)
            dew_c = (period.get("dewpoint") or {}).get("value")  # already °C
            pop = (period.get("probabilityOfPrecipitation") or {}).get("value")
            entry: dict = {"temperature": temp_c, "dewpoint": dew_c}
            if pop is not None:
                entry["precip_prob"] = pop / 100.0
            result[ts] = entry
        return result
    except Exception:
        return {}


_CSS = """\
* { box-sizing: border-box; margin: 0; padding: 0; }
body {
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    font-size: 14px;
    line-height: 1.5;
    color: #1a1a1a;
    background: #f5f5f5;
    padding: 24px 16px;
}
.container { max-width: 960px; margin: 0 auto; }
header {
    position: sticky;
    top: 0;
    z-index: 100;
    background: #f5f5f5;
    display: flex;
    flex-direction: column;
    gap: 8px;
    margin-bottom: 24px;
    padding: 12px 0;
    border-bottom: 2px solid #1a1a1a;
}
.header-top {
    display: flex;
    justify-content: space-between;
    align-items: flex-end;
}
header h1 { font-size: 22px; letter-spacing: -0.5px; }
.generated { font-size: 12px; color: #666; display: flex; flex-direction: column; align-items: flex-end; gap: 2px; }
.stale-banner {
    background: #fff3cd;
    border: 1px solid #ffc107;
    border-left: 4px solid #e6a800;
    padding: 10px 14px;
    margin-bottom: 20px;
    font-size: 13px;
    border-radius: 3px;
}
.stale-banner code { background: #ffeaa0; padding: 1px 4px; border-radius: 2px; font-size: 12px; }
h2 { font-size: 15px; font-weight: 600; margin-bottom: 12px; }
h3 { font-size: 13px; font-weight: 600; margin-bottom: 4px; }
.conditions-grid {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 16px;
}
.card {
    background: #fff;
    border: 1px solid #ddd;
    border-radius: 4px;
    padding: 14px 16px;
}
.station-id { font-weight: 400; color: #666; }
.obs-time { font-size: 12px; color: #666; margin-bottom: 8px; }
.obs-table { width: 100%; border-collapse: collapse; }
.obs-table th {
    text-align: left;
    font-weight: 500;
    color: #555;
    padding: 2px 12px 2px 0;
    white-space: nowrap;
    width: 1%;
}
.obs-table td { padding: 2px 0; }
.run-meta { font-size: 13px; color: #444; background: #fff; border: 1px solid #ddd; border-radius: 4px; padding: 12px 16px; }
.run-meta strong { font-weight: 600; }
table.forecast-table {
    width: 100%;
    border-collapse: collapse;
    background: #fff;
    border: 1px solid #ddd;
    border-radius: 4px;
}
table.forecast-table th,
table.forecast-table td {
    padding: 8px 12px;
    text-align: right;
    border-bottom: 1px solid #eee;
}
table.forecast-table th { text-align: left; font-weight: 500; color: #555; }
table.forecast-table thead th { background: #f9f9f9; font-weight: 600; color: #1a1a1a; }
table.forecast-table thead th:not(:first-child) { text-align: right; }
table.forecast-table tbody tr:last-child td,
table.forecast-table tbody tr:last-child th { border-bottom: none; }
.charts-grid {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 16px;
}
.mae-charts-grid {
    display: flex;
    flex-direction: column;
    gap: 16px;
}
.mae-filter-bar { display: flex; gap: 6px; margin-bottom: 10px; flex-wrap: wrap; align-items: center; }
.mae-filter-btn, .fcst-filter-btn,
.bias-filter-btn, .lead-skill-filter-btn, .heatmap-filter-btn,
.diurnal-filter-btn, .error-dist-var-btn, .error-dist-lead-btn,
.trajectory-filter-btn, .acc-filter-btn, .acc-window-btn { padding: 4px 12px; font-size: 12px; font-family: inherit; background: #fff; border: 1px solid #ccc; border-radius: 3px; cursor: pointer; color: #444; }
.mae-filter-btn:hover, .fcst-filter-btn:hover,
.bias-filter-btn:hover, .lead-skill-filter-btn:hover, .heatmap-filter-btn:hover,
.diurnal-filter-btn:hover, .error-dist-var-btn:hover, .error-dist-lead-btn:hover,
.trajectory-filter-btn:hover, .acc-filter-btn:hover, .acc-window-btn:hover { background: #f0f0f0; }
.mae-filter-btn.active, .fcst-filter-btn.active,
.bias-filter-btn.active, .lead-skill-filter-btn.active, .heatmap-filter-btn.active,
.diurnal-filter-btn.active, .error-dist-var-btn.active, .error-dist-lead-btn.active,
.trajectory-filter-btn.active, .acc-filter-btn.active, .acc-window-btn.active { background: #1a1a1a; color: #fff; border-color: #1a1a1a; }
.mae-raw-btn { margin-left: auto; padding: 4px 12px; font-size: 12px; font-family: inherit; background: #fff; border: 1px solid #ccc; border-radius: 3px; cursor: pointer; color: #666; }
.mae-raw-btn:hover { background: #f0f0f0; }
.mae-raw-btn.active { background: #555; color: #fff; border-color: #555; }
.chart-container {
    background: #fff;
    border: 1px solid #ddd;
    border-radius: 4px;
}
.muted { color: #888; font-style: italic; font-size: 13px; }
.obs-subhead { margin-top: 20px; margin-bottom: 6px; }
.collapsible-section { border: none; }
.collapsible-section > summary { cursor: pointer; list-style: none; }
.collapsible-section > summary::-webkit-details-marker { display: none; }
.collapsible-section > summary::before { content: "▶ "; font-size: 11px; color: #888; }
.collapsible-section[open] > summary::before { content: "▼ "; }
.acc-cell { text-align: center; min-width: 58px; }
.acc-excellent { color: #0a5c0a; font-weight: 700; }
.acc-high { color: #1a6b1a; font-weight: 600; }
.acc-mid { color: #5a7a00; }
.acc-ok { color: #555; }
.acc-low { color: #8b4400; }
.acc-poor { color: #8b2020; }
.acc-lead-table th.model-name-cell { text-align: left; font-weight: 500; padding-right: 16px; }
.acc-lead-table .baseline-row th.model-name-cell { color: #bbb; }
.acc-overall-table td { text-align: center; font-size: 15px; font-weight: 600; }
.obs-history-table {
    min-width: 100%;
    width: max-content;
    border-collapse: collapse;
    background: #fff;
    border: 1px solid #ddd;
    border-radius: 4px;
    font-size: 13px;
}
.obs-history-table th,
.obs-history-table td {
    padding: 6px 10px;
    text-align: left;
    border-bottom: 1px solid #eee;
    white-space: nowrap;
}
.obs-history-table thead th { background: #f9f9f9; font-weight: 600; color: #1a1a1a; }
.obs-history-table tbody tr:last-child td { border-bottom: none; }
.table-scroll { overflow-x: auto; margin-bottom: 8px; }
.more-btn {
    padding: 5px 14px;
    font-size: 13px;
    font-family: inherit;
    background: #fff;
    border: 1px solid #ccc;
    border-radius: 3px;
    cursor: pointer;
    color: #333;
}
.more-btn:hover { background: #f0f0f0; }
.verification-windows {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(420px, 1fr));
    gap: 16px;
    margin-bottom: 20px;
}
.verification-primary { margin-top: 16px; margin-bottom: 20px; }
.score-table {
    width: 100%;
    border-collapse: collapse;
    background: #fff;
    border: 1px solid #ddd;
    border-radius: 4px;
    font-size: 13px;
}
.score-table th,
.score-table td {
    padding: 6px 10px;
    text-align: right;
    border-bottom: 1px solid #eee;
}
.score-table th { text-align: left; font-weight: 500; color: #555; }
.score-table thead th { background: #f9f9f9; font-weight: 600; color: #1a1a1a; }
.score-table tbody tr:last-child td,
.score-table tbody tr:last-child th { border-bottom: none; }
.score-table td small { color: #888; display: block; font-size: 11px; }
.window-label { font-size: 12px; color: #666; margin-bottom: 6px; }
.model-header th { background: #f0f0f0; font-size: 11px; color: #555; padding: 4px 10px; font-weight: 600; letter-spacing: 0.03em; text-transform: uppercase; }
.ensemble-header th { background: #eff4ff; font-size: 11px; color: #3b5bdb; padding: 4px 10px; font-weight: 600; letter-spacing: 0.03em; text-transform: uppercase; }
.external-header th { background: #fff3e0; font-size: 11px; color: #b34400; padding: 4px 10px; font-weight: 600; letter-spacing: 0.03em; text-transform: uppercase; }
.ensemble-row th, .ensemble-row td { background: #f8faff; }
.external-row th, .external-row td { background: #fffbf6; }
.model-runs { display: flex; flex-direction: column; gap: 20px; }
.model-run-card { background: #fff; border: 1px solid #ddd; border-radius: 4px; overflow: hidden; }
.model-run-header { display: flex; align-items: baseline; gap: 10px; padding: 10px 16px; background: #f9f9f9; border-bottom: 1px solid #eee; }
.model-run-header strong { font-size: 14px; }
.base-badge, .ensemble-badge, .baseline-badge, .external-badge, .fun-badge { font-size: 11px; padding: 1px 6px; border-radius: 3px; font-weight: 600; letter-spacing: 0.03em; text-transform: uppercase; }
.base-badge { background: #e8f4e8; color: #2d6a2d; }
.ensemble-badge { background: #eff4ff; color: #3b5bdb; }
.baseline-badge { background: #ece9e0; color: #aaa; }
.external-badge { background: #fff3e0; color: #b34400; }
.fun-badge { background: #d4f0d4; color: #1e6b1e; }
.mae-summary-table .baseline-row th { color: #bbb; }
.baseline-row td { color: #bbb; }
.baseline-row .model-id-cell { color: #888; }
.member-badge { font-size: 11px; padding: 1px 6px; border-radius: 3px; font-weight: 500; background: #f5f0ff; color: #6b3fa0; }
.run-detail { font-size: 12px; color: #666; margin-left: auto; }
.mae-summary-table {
    width: 100%;
    border-collapse: collapse;
    background: #fff;
    border: 1px solid #ddd;
    border-radius: 4px;
    font-size: 13px;
    margin-bottom: 8px;
}
.mae-summary-table th,
.mae-summary-table td {
    padding: 6px 10px;
    text-align: right;
    border-bottom: 1px solid #eee;
}
.mae-summary-table th { text-align: left; font-weight: 500; color: #555; }
.mae-summary-table thead th { background: #f9f9f9; font-weight: 600; color: #1a1a1a; }
.mae-summary-table tbody tr:last-child td,
.mae-summary-table tbody tr:last-child th { border-bottom: none; }
.model-id-cell { color: #888; font-size: 12px; text-align: right; white-space: nowrap; width: 1%; }
.mae-summary-table thead th:not(:nth-child(2)) { text-align: right; }
.score-table thead th:not(:first-child) { text-align: right; }
.mae-better { color: #2a6a2a; font-weight: 600; }
.mae-baseline-val { color: #bbb; }
.mae-worse { color: #8b2020; font-weight: 600; }
.chart-legend-note { font-size: 11px; color: #999; margin: 2px 0 10px; }
.score-details summary {
    cursor: pointer;
    font-size: 12px;
    color: #555;
    padding: 4px 0 8px;
    user-select: none;
}
.score-details summary:hover { color: #1a1a1a; }
.member-detail { margin-top: 6px; }
.member-detail-table {
    width: 100%;
    border-collapse: collapse;
    font-size: 12px;
    background: #fafafa;
    border: 1px solid #e0e0e0;
    border-radius: 3px;
}
.member-detail-table th,
.member-detail-table td {
    padding: 4px 8px;
    text-align: right;
    border-bottom: 1px solid #eee;
}
.member-detail-table th { text-align: left; color: #555; }
.member-detail-table thead th { background: #f0f0f0; font-weight: 600; color: #1a1a1a; }
.member-detail-table tbody tr:last-child td,
.member-detail-table tbody tr:last-child th { border-bottom: none; }
.member-btn {
    padding: 2px 9px;
    font-size: 11px;
    font-family: inherit;
    background: #fff;
    border: 1px solid #ccc;
    border-radius: 3px;
    cursor: pointer;
    color: #444;
}
.member-btn:hover { background: #f0f0f0; }
.member-detail-row td { padding: 6px 10px; }
.mf-btn {
    font-size: 11px;
    padding: 1px 6px;
    border-radius: 3px;
    font-weight: 500;
    background: #f5f0ff;
    color: #6b3fa0;
    border: 1px solid #d4bfff;
    cursor: pointer;
    font-family: inherit;
}
.mf-btn:hover { background: #ede3ff; }
.member-forecast-panel {
    padding: 12px 16px;
    background: #f8f8ff;
    border-top: 1px solid #eee;
}
.weights-section { display: grid; grid-template-columns: repeat(auto-fill, minmax(420px, 1fr)); gap: 16px; margin-top: 12px; align-items: start; }
.weights-model-block { }
.weights-model-block h3 { font-size: 13px; font-weight: 600; margin-bottom: 4px; }
.weight-table {
    border-collapse: collapse;
    font-size: 12px;
    background: #fafafa;
    border: 1px solid #e0e0e0;
    border-radius: 3px;
}
.weight-table th, .weight-table td { padding: 4px 10px; text-align: left; border-bottom: 1px solid #eee; }
.weight-table thead th { background: #f0f0f0; font-weight: 600; color: #1a1a1a; }
.weight-table tbody tr:last-child th,
.weight-table tbody tr:last-child td { border-bottom: none; }
.weight-table td.wt-pct { text-align: right; font-variant-numeric: tabular-nums; min-width: 52px; }
.weight-group-hdr th { background: #f5f5f5; font-size: 11px; color: #888; font-weight: 600;
    letter-spacing: 0.04em; text-transform: uppercase; padding: 3px 10px; }
.learnings-intro { margin-bottom: 14px; color: #555; font-size: 13px; line-height: 1.6; }
.learnings-desc { margin: 8px 0 14px; font-size: 13px; color: #444; line-height: 1.6; padding: 10px 14px; background: #f9f9f9; border-left: 3px solid #ddd; border-radius: 0 3px 3px 0; }
.learnings-hyp-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(460px, 1fr)); gap: 16px; margin-bottom: 14px; }
.no-data { color: #888; font-style: italic; font-size: 13px; margin: 8px 0 16px; }
.filter-label { font-size: 11px; color: #888; white-space: nowrap; align-self: center; }
.filter-sep-left { margin-left: 8px; }
@media (max-width: 768px) {
    body { padding: 12px 10px; }
    .generated { font-size: 11px; }
    .conditions-grid, .charts-grid { grid-template-columns: 1fr; }
    .verification-windows { grid-template-columns: 1fr; }
    .learnings-hyp-grid { grid-template-columns: 1fr; }
    .weights-section { grid-template-columns: 1fr; }
    .section { scroll-margin-top: 70px; }
    .score-table, .mae-summary-table, .obs-history-table { font-size: 12px; }
    .forecast-table th, .forecast-table td { padding: 6px 8px; }
    .fcst-row-refs { gap: 12px; }
    .model-run-header { flex-wrap: wrap; gap: 6px; }
    .mae-raw-btn { margin-left: 0; }
    .jump-nav a { font-size: 11px; padding: 3px 8px; }
    #analysis, #weights, #obs-history { display: none; }
    .model-runs { display: none; }
    .jump-nav a[href="#analysis"],
    .jump-nav a[href="#weights"] { display: none; }
}
.forecast-rows { display: flex; flex-direction: column; gap: 6px; }
.fcst-row {
    display: grid;
    grid-template-columns: 130px 1fr;
    gap: 16px;
    background: #fff;
    border: 1px solid #ddd;
    border-radius: 4px;
    padding: 12px 16px;
    align-items: start;
}
.fcst-row.now-row { border-color: #b0c4de; background: #f5f8fc; }
.fcst-row-main { }
.fcst-row-refs { display: flex; gap: 20px; }
.fcst-label { font-size:11px; font-weight:700; text-transform:uppercase; letter-spacing:.06em; color:#888; margin-bottom:6px; }
.fcst-temp { font-size:26px; font-weight:700; color:#1a1a1a; line-height:1; }
.fcst-temp-spread { font-size:11px; color:#aaa; margin-top:2px; margin-bottom:8px; }
.fcst-details { font-size:12px; color:#555; line-height:1.8; }
.fcst-details .detail-label { color:#999; }
.fcst-no-data { color:#bbb; font-size:13px; }
.fcst-ref { flex: 1; min-width: 0; font-size:11px; color:#999; line-height:1.8; }
.fcst-ref-lbl { font-size:10px; font-weight:700; text-transform:uppercase; letter-spacing:.05em; color:#ccc; display:block; line-height:1.6; }
.fcst-ref .detail-label { color:#bbb; }
.fcst-ref-temp { font-size:14px; }
.fcst-delta { font-size:10px; color:#999; margin-left:3px; }
.jump-nav {
    display: flex;
    gap: 6px;
    flex-wrap: wrap;
}
.jump-nav a {
    font-size: 12px;
    color: #444;
    text-decoration: none;
    padding: 4px 10px;
    border-radius: 3px;
    border: 1px solid #ddd;
    background: #fff;
    white-space: nowrap;
}
.jump-nav a:hover { background: #f0f0f0; color: #1a1a1a; }
.section { margin-bottom: 32px; scroll-margin-top: 115px; }
.section-dig-deeper {
    border-top: 1px solid #ddd;
    padding-top: 14px;
    margin-top: 8px;
    color: #888;
    font-weight: 500;
}
.analysis-section {
    border-top: 3px solid #ddd;
    padding-top: 24px;
    margin-top: 8px;
}
.analysis-section > h2 { color: #555; }
.ap-signal-table {
    width: 100%;
    border-collapse: collapse;
    background: #fff;
    border: 1px solid #ddd;
    border-radius: 4px;
    font-size: 13px;
    margin-top: 10px;
}
.ap-signal-table th, .ap-signal-table td {
    padding: 6px 10px;
    border-bottom: 1px solid #eee;
    text-align: right;
}
.ap-signal-table th { font-weight: 600; background: #f9f9f9; color: #1a1a1a; }
.ap-signal-table th:nth-child(-n+4), .ap-signal-table td:nth-child(-n+4) { text-align: left; }
.ap-signal-table tbody tr:last-child td { border-bottom: none; }
.ap-badge { font-size: 11px; padding: 1px 7px; border-radius: 3px; font-weight: 600; display: inline-block; }
.ap-wet { background: #dbeafe; color: #1e40af; }
.ap-dry { background: #fef3c7; color: #92400e; }
.ap-neutral { background: #f3f4f6; color: #4b5563; }
.ap-none { color: #bbb; font-style: italic; }
"""


def _weights_section_html(rows: list, all_members: list | None = None) -> str:
    from collections import defaultdict

    _SECTOR_LABELS = ["night", "morning", "afternoon", "evening"]
    _SECTOR_HOURS = ["00-05", "06-11", "12-17", "18-23"]

    # build tuned weights: model_id -> member_id -> sector -> avg weight across (var, lead)
    sums: dict = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    model_names: dict = {}
    member_names: dict = {}
    has_sectors: dict = {}
    for r in rows:
        sector = r["sector"] if "sector" in r.keys() else None
        sums[r["model_id"]][r["member_id"]][sector].append(r["weight"])
        model_names[r["model_id"]] = r["model_name"]
        member_names[(r["model_id"], r["member_id"])] = r["member_name"] or str(r["member_id"])
        if sector is not None:
            has_sectors[r["model_id"]] = True

    # avg_weights: model_id -> member_id -> sector -> avg weight
    avg_weights: dict = {
        mid: {
            mem_id: {s: sum(ws) / len(ws) for s, ws in sectors.items()}
            for mem_id, sectors in members.items()
        }
        for mid, members in sums.items()
    }
    tuned_ids = set(avg_weights)

    # add models that have members but no tuned weights yet (equal-weight placeholder)
    if all_members:
        by_model: dict = defaultdict(list)
        for r in all_members:
            by_model[r["model_id"]].append(r)
        for model_id, members in by_model.items():
            if model_id not in tuned_ids:
                n = len(members)
                avg_weights[model_id] = {
                    r["member_id"]: {None: 1.0 / n} for r in members
                }
                model_names[model_id] = members[0]["model_name"]
                for r in members:
                    member_names[(model_id, r["member_id"])] = (
                        r["member_name"] or str(r["member_id"])
                    )

    if not avg_weights:
        return ""

    def _group_label(name: str) -> str:
        if name.startswith("s-"): return "static"
        if name.startswith("d03-"): return "decay k=0.03"
        if name.startswith("d05-"): return "decay k=0.05"
        if name.startswith("d10-"): return "decay k=0.10"
        if name.startswith("sine-"): return "sine"
        if name.startswith("piecewise-"): return "piecewise"
        if name.startswith("asymmetric-"): return "asymmetric"
        if name.startswith("solar"): return "solar"
        return ""

    blocks = []
    for model_id in sorted(avg_weights):
        members_data = avg_weights[model_id]
        sectored = has_sectors.get(model_id, False)
        n = len(members_data)
        equal_w = 1.0 / n
        tuned = model_id in tuned_ids

        if sectored:
            # per-sector columns: compute max weight per sector for coloring
            sector_max = {}
            for sector in range(4):
                vals = [sectors.get(sector, equal_w) for sectors in members_data.values()]
                sector_max[sector] = max(vals)

            header_cells = "".join(
                f'<th class="wt-pct">{_SECTOR_LABELS[s]}<br>'
                f'<span style="font-weight:400;color:#999">{_SECTOR_HOURS[s]}</span></th>'
                for s in range(4)
            )
            table_rows = []
            prev_group = None
            for mem_id in sorted(members_data):
                sectors = members_data[mem_id]
                name = member_names[(model_id, mem_id)]
                group = _group_label(name)
                if group and group != prev_group:
                    table_rows.append(
                        f'<tr class="weight-group-hdr">'
                        f'<th colspan="5">{group}</th></tr>'
                    )
                    prev_group = group
                cells = []
                for sector in range(4):
                    w = sectors.get(sector, equal_w)
                    spread = sector_max[sector] - equal_w
                    if spread > 0 and w > equal_w:
                        opacity = min((w - equal_w) / spread, 1.0) * 0.45
                    else:
                        opacity = 0.0
                    color = f'background:rgba(59,91,219,{opacity:.3f})' if opacity > 0.01 else ''
                    style = f' style="{color}"' if color else ''
                    cells.append(f'<td class="wt-pct"{style}>{w:.1%}</td>')
                table_rows.append(
                    f'<tr>'
                    f'<th><span class="model-id-cell">{mem_id}</span> {name}</th>'
                    f'{"".join(cells)}'
                    f'</tr>'
                )
            thead = f'<thead><tr><th>Member</th>{header_cells}</tr></thead>'
        else:
            # no sector data — single avg weight column (legacy / untuned fallback)
            all_weights_flat = [
                list(sectors.values())[0]
                for sectors in members_data.values()
            ]
            max_w = max(all_weights_flat)
            spread = max_w - equal_w
            table_rows = []
            prev_group = None
            for mem_id in sorted(members_data):
                w = list(members_data[mem_id].values())[0]
                name = member_names[(model_id, mem_id)]
                group = _group_label(name)
                if group and group != prev_group:
                    table_rows.append(
                        f'<tr class="weight-group-hdr"><th colspan="2">{group}</th></tr>'
                    )
                    prev_group = group
                if spread > 0 and w > equal_w:
                    opacity = min((w - equal_w) / spread, 1.0) * 0.45
                else:
                    opacity = 0.0
                color = f'background:rgba(59,91,219,{opacity:.3f})' if opacity > 0.01 else ''
                cell_style = f' style="{color}"' if color else ''
                table_rows.append(
                    f'<tr>'
                    f'<th><span class="model-id-cell">{mem_id}</span> {name}</th>'
                    f'<td class="wt-pct"{cell_style}>{w:.1%}</td>'
                    f'</tr>'
                )
            thead = '<thead><tr><th>Member</th><th>Avg weight</th></tr></thead>'

        untrained_note = (
            '' if tuned
            else ' <span style="color:#aaa;font-style:italic;font-weight:400">(not tuned)</span>'
        )
        block_inner = (
            f'<div class="weights-model-block">'
            f'<h3>{model_names[model_id]}{untrained_note}'
            f' <span class="model-id-cell">(model {model_id})</span></h3>'
            f'<p class="window-label">equal weight: {equal_w:.1%} per member</p>'
            f'<table class="weight-table">'
            f'{thead}'
            f'<tbody>{"".join(table_rows)}</tbody>'
            f'</table>'
            f'</div>'
        )
        blocks.append((model_id, block_inner))

    _ENSEMBLE_ID = 100
    ensemble_blocks = [b for mid, b in blocks if mid == _ENSEMBLE_ID]
    other_blocks = [(mid, b) for mid, b in blocks if mid != _ENSEMBLE_ID]

    ensemble_html = (
        f'<div class="weights-section">{"".join(ensemble_blocks)}</div>'
        if ensemble_blocks else ''
    )
    others_html = "".join(
        f'<details class="collapsible-section" style="margin-top:12px">'
        f'<summary>{model_names[mid]}'
        f' <span class="model-id-cell" style="font-weight:400">(model {mid})</span>'
        f'</summary>'
        f'<div class="weights-section" style="margin-top:8px">{b}</div>'
        f'</details>'
        for mid, b in other_blocks
    )

    return ensemble_html + others_html


def _table_data(rows) -> dict:
    """variable -> lead_hours -> value"""
    data: dict = {}
    for row in rows:
        var = row["variable"]
        if var not in data:
            data[var] = {}
        data[var][row["lead_hours"]] = row["value"]
    return data


def _chart_data(rows) -> dict:
    """variable -> model -> {x: [ISO timestamps], y: [values]}"""
    from datetime import datetime
    data: dict = {}
    for row in rows:
        var = row["variable"]
        model = row["model"]
        if var not in data:
            data[var] = {}
        if model not in data[var]:
            data[var][model] = {"x": [], "y": [], "model_id": row["model_id"]}
        v = row["value"]
        if var == "temperature" or var == "dewpoint":
            v = _to_f(v)
        ts = datetime.fromtimestamp(row["valid_at"], tz=fmt.CENTRAL).strftime("%Y-%m-%d %H:%M:%S")
        data[var][model]["x"].append(ts)
        data[var][model]["y"].append(v)
    return data


def _zambretti_panel_html(z: dict | None) -> str:
    if z is None or z.get("letter") == "\u2014":
        return ""
    cat = z["category"].replace("_", " ")
    rate = z["rate_hpa_per_h"]
    rate_str = f"{rate:+.2f} hPa/h" if rate is not None else "\u2014"
    return (
        f'<div class="card" style="margin-top:12px">'
        f'<h3>Barometer says: {z["description"]}'
        f' <span class="station-id">({z["letter"]})</span></h3>'
        f'<p style="font-size:13px;color:#555;margin-top:4px">'
        f'Tendency: {cat} &mdash; {rate_str}'
        f'</p>'
        f'<p style="font-size:11px;color:#888;margin-top:4px">'
        f'Zambretti algorithm &mdash; sea-level pressure'
        f'</p>'
        f'</div>'
    )


def _conditions_card(label: str, obs, elevation_m: float = 0.0) -> str:
    if obs is None:
        return (
            f'<div class="card"><h3>{label}</h3>'
            f'<p class="muted">no data</p></div>'
        )

    station_id = obs["station_id"]
    name = obs["name"] or station_id
    timestamp = fmt.ts(obs["timestamp"])

    if label == "Tempest":
        name = "Tempest Weather Station"
        station_id = None
        gust = obs["wind_gust"]
        gust_str = f", gusts to {fmt.val(_to_mph(gust), '.1f', ' mph')}" if gust is not None else ""
        lc = obs["lightning_count"]
        sp = obs["station_pressure"]
        slp = obs["sea_level_pressure"]
        if slp is None and sp is not None and elevation_m > 0.0 and obs["air_temp"] is not None:
            slp = fmt.to_slp(sp, obs["air_temp"], elevation_m)
        if slp is not None:
            pres_cell = fmt.val(slp, ".1f", " hPa")
        else:
            pres_cell = fmt.val(sp, ".1f", " hPa") + " (station)"
        rows_html = (
            f'<tr><th>Temperature</th><td>{fmt.temp(obs["air_temp"])}</td></tr>'
            f'<tr><th>Dew Point</th><td>{fmt.temp(obs["dew_point"])}</td></tr>'
            f'<tr><th>Pressure</th><td>{pres_cell}</td></tr>'
            f'<tr><th>Wind</th><td>{fmt.wind_dir(obs["wind_direction"])} {fmt.val(_to_mph(obs["wind_avg"]), ".1f", " mph")}{gust_str}</td></tr>'
            f'<tr><th>Precip today</th><td>{fmt.val(_to_in(obs["precip_accum_day"]), ".2f", " in")}</td></tr>'
            f'<tr><th>UV Index</th><td>{fmt.val(obs["uv_index"], ".1f")}</td></tr>'
            f'<tr><th>Solar</th><td>{fmt.val(obs["solar_radiation"], ".0f", " W/m\u00b2")}</td></tr>'
            f'<tr><th>Lightning</th><td>{lc if lc is not None else 0} strikes (3-min count)</td></tr>'
        )
    else:
        nws_slp = obs["sea_level_pressure"]
        if nws_slp is None:
            try:
                nws_slp = obs["pressure_altimeter"]
            except (IndexError, KeyError):
                pass
        rows_html = (
            f'<tr><th>Temperature</th><td>{fmt.temp(obs["air_temp"])}</td></tr>'
            f'<tr><th>Dew Point</th><td>{fmt.temp(obs["dew_point"])}</td></tr>'
            f'<tr><th>Wind</th><td>{fmt.wind_dir(obs["wind_direction"])} {fmt.val(_to_mph(obs["wind_speed"]), ".1f", " mph")}</td></tr>'
            f'<tr><th>Pressure</th><td>{fmt.val(nws_slp, ".1f", " hPa")}</td></tr>'
            f'<tr><th>Sky</th><td>{obs["sky_cover"] or "\u2014"}</td></tr>'
            f'<tr><th>METAR</th><td>{obs["raw_metar"] or "\u2014"}</td></tr>'
        )

    return (
        f'<div class="card">'
        f'<h3>{label}: {name}'
        + (f' <span class="station-id">({station_id})</span>' if station_id else "")
        + '</h3>'
        f'<p class="obs-time">{timestamp}</p>'
        f'<table class="obs-table"><tbody>{rows_html}</tbody></table>'
        f'</div>'
    )


def _forecast_table_html(table: dict, lead_times: list, slp_offset: float = 0.0) -> str:
    header_cells = "".join(f"<th>+{h}h</th>" for h in lead_times)
    rows = []
    for var in VARIABLES:
        if not table.get(var):
            continue
        label = _VARIABLE_LABEL.get(var, var)
        unit = _UNIT.get(var, "")
        if var == "pressure" and slp_offset != 0.0:
            label = "Station P"
        cells = []
        for h in lead_times:
            v = table.get(var, {}).get(h)
            if var in ("temperature", "dewpoint"):
                v = _to_f(v)
            elif var == "precip_prob":
                v = _to_pct(v)
            fmt_spec = _FMT.get(var, ".1f")
            cells.append(f"<td>{fmt.val(v, fmt_spec, unit)}</td>")
        rows.append(f'<tr><th>{label}</th>{"".join(cells)}</tr>')
        if var == "pressure" and slp_offset != 0.0:
            slp_cells = []
            for h in lead_times:
                v = table.get(var, {}).get(h)
                slp_cells.append(
                    f"<td>{fmt.val(v + slp_offset if v is not None else None, '.1f', ' hPa')}</td>"
                )
            rows.append(f'<tr><th>SLP</th>{"".join(slp_cells)}</tr>')

    return (
        '<table class="forecast-table">'
        f'<thead><tr><th>Variable</th>{header_cells}</tr></thead>'
        f'<tbody>{"".join(rows)}</tbody>'
        '</table>'
    )


def _model_runs_html(
    rows: list,
    lead_times: list,
    member_counts: dict | None = None,
    member_rows: list | None = None,
    slp_offset: float = 0.0,
) -> str:
    by_model: dict = {}
    for row in rows:
        key = (row["model_id"], row["model"], row["type"], row["issued_at"])
        by_model.setdefault(key, []).append(row)

    sorted_keys = sorted(by_model, key=lambda k: k[0])
    cards = []
    for (model_id, model, mtype, issued_at) in sorted_keys:
        model_rows = by_model[(model_id, model, mtype, issued_at)]
        table = _table_data(model_rows)
        if model == "bogo":
            type_badge = '<span class="fun-badge">fun</span>'
        else:
            type_badge = {
                "ensemble": f'<span class="ensemble-badge">ensemble</span>',
                "external": f'<span class="external-badge">external</span>',
            }.get(mtype, "")
        table_html = _forecast_table_html(table, lead_times, slp_offset)
        n_members = (member_counts or {}).get(model_id, 0)
        member_toggle = (
            f'<button class="mf-btn" data-model-id="{model_id}">'
            f'{n_members} members &#x25be;</button>'
            if n_members else ""
        )
        member_panel = (
            f'<div class="member-forecast-panel" id="mfp-{model_id}" style="display:none"></div>'
            if n_members else ""
        )
        cards.append(
            f'<div class="model-run-card">'
            f'<div class="model-run-header">'
            f'<strong><span class="model-id-cell">{model_id}</span> {model}</strong>'
            f'{type_badge}'
            f'{member_toggle}'
            f'<span class="run-detail">issued {fmt.ts(issued_at)} &mdash; {len(model_rows)} rows</span>'
            f'</div>'
            f'{table_html}'
            f'{member_panel}'
            f'</div>'
        )
    return "\n".join(cards)


def _tempest_obs_row(row, elevation_m: float = 0.0) -> str:
    gust = row["wind_gust"]
    wind = fmt.wind_dir(row["wind_direction"]) + " " + fmt.val(_to_mph(row["wind_avg"]), ".1f", " mph")
    if gust is not None:
        wind += f" g{fmt.val(_to_mph(gust), '.1f')}"
    lc = row["lightning_count"]
    sp = row["station_pressure"]
    slp_cell = ""
    if elevation_m > 0.0:
        slp = row["sea_level_pressure"]
        if slp is None and sp is not None and row["air_temp"] is not None:
            slp = fmt.to_slp(sp, row["air_temp"], elevation_m)
        slp_cell = f"<td>{fmt.val(slp, '.1f', ' hPa')}</td>"
    return (
        "<tr>"
        f"<td>{fmt.ts(row['timestamp'])}</td>"
        f"<td>{fmt.temp(row['air_temp'])}</td>"
        f"<td>{fmt.temp(row['dew_point'])}</td>"
        f"<td>{fmt.val(sp, '.1f', ' hPa')}</td>"
        f"{slp_cell}"
        f"<td>{wind}</td>"
        f"<td>{fmt.val(_to_in(row['precip_accum_day']), '.2f', ' in')}</td>"
        f"<td>{lc if lc is not None else 0}</td>"
        "</tr>"
    )


def _nws_obs_row(row) -> str:
    return (
        "<tr>"
        f"<td>{fmt.ts(row['timestamp'])}</td>"
        f"<td>{fmt.temp(row['air_temp'])}</td>"
        f"<td>{fmt.temp(row['dew_point'])}</td>"
        f"<td>{fmt.wind_dir(row['wind_direction'])} {fmt.val(_to_mph(row['wind_speed']), '.1f', ' mph')}</td>"
        f"<td>{fmt.val(row['sea_level_pressure'], '.1f', ' hPa')}</td>"
        f"<td>{row['sky_cover'] or '\u2014'}</td>"
        "</tr>"
    )


def _obs_history_section(tempest_obs: list, nws_obs: list, elevation_m: float = 0.0) -> str:
    def station_heading(label: str, obs_list: list) -> str:
        if not obs_list:
            return label
        r = obs_list[0]
        if label == "Tempest":
            return 'Tempest Weather Station'
        name = r["name"] or r["station_id"]
        sid = r["station_id"]
        return f'{label}: {name} <span class="station-id">({sid})</span>'

    def table_block(label: str, obs_list: list, tbody_id: str, btn_id: str, headers: list) -> str:
        heading = station_heading(label, obs_list)
        header_html = "".join(f"<th>{h}</th>" for h in headers)
        empty = (
            f'<tr><td colspan="{len(headers)}" class="muted">no data</td></tr>'
            if not obs_list else ""
        )
        return (
            f'<h3 class="obs-subhead">{heading}</h3>'
            f'<div class="table-scroll">'
            f'<table class="obs-history-table">'
            f'<thead><tr>{header_html}</tr></thead>'
            f'<tbody id="{tbody_id}">{empty}</tbody>'
            f'</table>'
            f'</div>'
            f'<button class="more-btn" id="{btn_id}">Load more</button>'
        )

    tempest_headers = (
        ["Time", "Temperature", "Dew Point", "Station P", "SLP", "Wind", "Precip (day)", "Lightning"]
        if elevation_m > 0.0 else
        ["Time", "Temperature", "Dew Point", "Pressure", "Wind", "Precip (day)", "Lightning"]
    )
    tempest_block = table_block(
        "Tempest", tempest_obs, "tempest-obs-tbody", "tempest-more-btn",
        tempest_headers,
    )
    nws_block = table_block(
        "NWS", nws_obs, "nws-obs-tbody", "nws-more-btn",
        ["Time", "Temperature", "Dew Point", "Wind", "Pressure", "Sky"],
    )

    return (
        '<section class="section" id="obs-history">'
        '<h2>Observation History</h2>'
        + tempest_block
        + nws_block
        + '</section>'
    )


def _obs_history_js(tempest_rows: list, nws_rows: list) -> str:
    t_json = json.dumps(tempest_rows)
    n_json = json.dumps(nws_rows)
    return f"""\
const tempestHistory = {t_json};
const nwsHistory = {n_json};

function makeLoader(rows, tbodyId, btnId) {{
    let n = 10;
    const tbody = document.getElementById(tbodyId);
    const btn = document.getElementById(btnId);
    function render() {{
        tbody.innerHTML = rows.slice(0, n).join('');
        if (n >= rows.length) btn.style.display = 'none';
    }}
    render();
    btn.addEventListener('click', function() {{
        n = Math.min(n + 10, rows.length);
        render();
    }});
}}

makeLoader(tempestHistory, 'tempest-obs-tbody', 'tempest-more-btn');
makeLoader(nwsHistory, 'nws-obs-tbody', 'nws-more-btn');
"""


def _compute_model_summary(rows: list) -> dict:
    """Returns model_name -> {model_id, type, avg_mae, mae_24h, n} from member_id=0 rows."""
    accum: dict = {}
    for row in rows:
        name = row["model"]
        if name not in accum:
            accum[name] = {
                "model_id": row["model_id"],
                "type": row["type"],
                "sum_mae": 0.0,
                "sum_n": 0,
                "sum_24": 0.0,
                "n_24": 0,
            }
        a = accum[name]
        n = row["n"]
        mae = row["avg_mae"]
        if mae is not None and n:
            a["sum_mae"] += mae * n
            a["sum_n"] += n
            if row["lead_hours"] == 24:
                a["sum_24"] += mae * n
                a["n_24"] += n
    result = {}
    for name, a in accum.items():
        result[name] = {
            "model_id": a["model_id"],
            "type": a["type"],
            "avg_mae": a["sum_mae"] / a["sum_n"] if a["sum_n"] else None,
            "mae_24h": a["sum_24"] / a["n_24"] if a["n_24"] else None,
            "n": a["sum_n"],
        }
    return result


def _mae_color_class(value: float | None, baseline: float | None) -> str:
    if value is None or baseline is None or baseline == 0:
        return ""
    ratio = value / baseline
    if ratio <= 0.90:
        return ' class="mae-better"'
    if ratio >= 1.10:
        return ' class="mae-worse"'
    return ""


def _score_summary_table(
    summary_rows: list,
    window_label: str,
    member_models: set | None = None,
    all_models: dict | None = None,
) -> str:
    model_summary = _compute_model_summary(summary_rows)

    # merge in any models that have runs but no scored data yet
    if all_models:
        for name, meta in all_models.items():
            if name not in model_summary:
                model_summary[name] = {
                    "model_id": meta["model_id"],
                    "type": meta["type"],
                    "avg_mae": None,
                    "mae_24h": None,
                    "n": 0,
                }

    if not model_summary:
        return f'<div><p class="window-label">{window_label}</p><p class="muted">no scored forecasts</p></div>'

    climo = model_summary.get("climatological_mean", {})
    c_avg = climo.get("avg_mae")
    c_24h = climo.get("mae_24h")
    total = sum(row["n"] for row in summary_rows)
    sorted_names = sorted(model_summary.keys(), key=lambda k: model_summary[k]["model_id"])

    pers_m = model_summary.get("persistence", {})
    pers_avg_ratio = pers_m["avg_mae"] / c_avg if (pers_m.get("avg_mae") is not None and c_avg) else None
    pers_24h_ratio = pers_m["mae_24h"] / c_24h if (pers_m.get("mae_24h") is not None and c_24h) else None

    # level 1: at-a-glance summary rows
    summary_tbody = []
    for name in sorted_names:
        m = model_summary[name]
        if name == "climatological_mean":
            badge = '<span class="baseline-badge">baseline</span>'
        elif name == "persistence":
            badge = ""
        elif m["type"] == "ensemble":
            badge = '<span class="ensemble-badge">ensemble</span>'
        elif m["type"] == "external":
            badge = '<span class="external-badge">external</span>'
        elif name == "bogo":
            badge = '<span class="fun-badge">fun</span>'
        else:
            badge = ""

        is_climo = name == "climatological_mean"
        is_pers = name == "persistence"
        if is_climo:
            avg_ratio = 1.0 if m["avg_mae"] is not None else None
            h24_ratio = 1.0 if m["mae_24h"] is not None else None
        else:
            avg_ratio = m["avg_mae"] / c_avg if (m["avg_mae"] is not None and c_avg) else None
            h24_ratio = m["mae_24h"] / c_24h if (m["mae_24h"] is not None and c_24h) else None

        def _worse_than_pers(ratio, pers_ratio):
            return (ratio is not None and pers_ratio is not None and ratio > pers_ratio)

        if not is_climo and not is_pers and _worse_than_pers(avg_ratio, pers_avg_ratio):
            avg_cls = ' class="mae-worse-pers"'
        else:
            avg_cls = _mae_color_class(avg_ratio, 1.0) if not is_climo and avg_ratio is not None else ""
        if not is_climo and not is_pers and _worse_than_pers(h24_ratio, pers_24h_ratio):
            h24_cls = ' class="mae-worse-pers"'
        else:
            h24_cls = _mae_color_class(h24_ratio, 1.0) if not is_climo and h24_ratio is not None else ""
        def _cell(ratio, raw, cls, is_baseline=False):
            if ratio is None:
                return "\u2014"
            raw_str = f"{raw:.2f}" if raw is not None else "\u2014"
            effective_cls = ' class="mae-baseline-val"' if is_baseline else cls
            return f'<span{effective_cls} data-raw="{raw_str}" data-ratio="{ratio:.2f}">{ratio:.2f}</span>'
        avg_str = _cell(avg_ratio, m["avg_mae"], avg_cls, is_climo)
        h24_str = _cell(h24_ratio, m["mae_24h"], h24_cls, is_climo)

        has_members = bool(member_models) and name in member_models
        safe = name.replace("_", "-").replace(" ", "-")
        members_cell = (
            f'<td><button class="member-btn" data-model="{name}">members</button></td>'
            if has_members else "<td></td>"
        )
        member_detail_row = (
            f'<tr class="member-detail-row" id="mdr-{safe}" style="display:none">'
            f'<td colspan="5"><div class="member-detail" id="md-{safe}"></div></td>'
            f'</tr>'
            if has_members else ""
        )
        row_cls = ' class="baseline-row"' if name in ('climatological_mean', 'persistence') else ''
        summary_tbody.append(
            f'<tr{row_cls}>'
            f'<td class="model-id-cell">{m["model_id"]}</td>'
            f'<th>{name} {badge}</th>'
            f'<td>{avg_str}</td>'
            f'<td>{h24_str}</td>'
            f'{members_cell}'
            f'</tr>'
            f'{member_detail_row}'
        )

    summary_table = (
        '<table class="mae-summary-table">'
        '<thead><tr><th>ID</th><th>Model</th><th class="col-avg-hdr">Avg vs climo</th><th class="col-24h-hdr">+24h vs climo</th><th></th></tr></thead>'
        f'<tbody>{"".join(summary_tbody)}</tbody>'
        '</table>'
    )

    # level 2: variable × lead breakdown wrapped in <details>
    by_model: dict = {}
    for row in summary_rows:
        key = (row["model_id"], row["model"], row["type"])
        by_model.setdefault(key, {}).setdefault(row["variable"], {})[row["lead_hours"]] = (
            row["avg_mae"], row["avg_bias"]
        )

    leads = sorted({row["lead_hours"] for row in summary_rows})
    header_cells = "".join(f"<th>+{h}h</th>" for h in leads)
    sorted_keys = sorted(by_model.keys(), key=lambda k: k[0])
    detail_rows = []
    for model_id, model_name, model_type in sorted_keys:
        hdr_class = {"ensemble": "ensemble-header", "external": "external-header"}.get(
            model_type, "model-header"
        )
        detail_rows.append(
            f'<tr class="{hdr_class}"><th colspan="{len(leads) + 1}">{model_id} — {model_name}</th></tr>'
        )
        var_data = by_model[(model_id, model_name, model_type)]
        row_class = {
            "ensemble": ' class="ensemble-row"',
            "external": ' class="external-row"',
        }.get(model_type, "")
        for var in VARIABLES:
            if var not in var_data:
                continue
            var_label = _VARIABLE_LABEL.get(var, var)
            unit = _UNIT.get(var, "")
            cells = []
            for l in leads:
                if l in var_data[var]:
                    mae, bias = var_data[var][l]
                    if var in ("temperature", "dewpoint"):
                        mae = _diff_to_f(mae)
                        bias = _diff_to_f(bias)
                    sign = "+" if (bias or 0) >= 0 else ""
                    cells.append(f"<td>{mae:.2f}<small>{sign}{bias:.2f}</small></td>")
                else:
                    cells.append("<td>\u2014</td>")
            detail_rows.append(
                f'<tr{row_class}><th>{var_label} ({unit})</th>{"".join(cells)}</tr>'
            )

    detail_table = (
        '<table class="score-table">'
        f'<thead><tr><th>MAE / bias</th>{header_cells}</tr></thead>'
        f'<tbody>{"".join(detail_rows)}</tbody>'
        '</table>'
    )

    return (
        f'<div>'
        f'<p class="window-label">{window_label} \u2014 {total} scored</p>'
        f'<div class="table-scroll">{summary_table}</div>'
        f'<details class="score-details">'
        f'<summary>Variable breakdown</summary>'
        f'<div class="table-scroll">{detail_table}</div>'
        f'</details>'
        f'</div>'
    )


def _rolling_mean(values: list, window: int = 10) -> list:
    """Trailing rolling mean of `window` points; None values are skipped."""
    out = []
    for i in range(len(values)):
        chunk = [x for x in values[max(0, i - window + 1): i + 1] if x is not None]
        out.append(sum(chunk) / len(chunk) if chunk else None)
    return out


def _mae_timeseries_data(timeseries_rows: list) -> dict:
    """lead (str) -> model -> {is_baseline, is_persistence, is_ensemble, model_id,
                               series: {var|avg -> {x, y_raw, y_ratio, y_ratio_rolling}}}

    y_ratio = MAE / climatological_mean_MAE for the same (var, lead, issued_at).
    climatological_mean always has y_ratio = 1.0; persistence shows its actual
    ratio vs climo. Average series ratios are the mean ratio across variables
    (dimensionless, comparable across vars).
    """
    raw: dict = {}
    model_meta: dict = {}
    for row in timeseries_rows:
        lead = row["lead_hours"]
        model_meta[row["model"]] = {"is_ensemble": row["type"] == "ensemble", "model_id": row["model_id"]}
        raw.setdefault(lead, {}).setdefault(row["model"], {}).setdefault(
            row["variable"], {}
        )[row["issued_at"]] = row["avg_mae"]

    result: dict = {}
    for lead in sorted(raw):
        # climo MAE for this lead: var -> issued_at -> mae
        c_ts: dict = raw[lead].get("climatological_mean", {})
        result[str(lead)] = {}
        for model, vars_ in raw[lead].items():
            is_baseline = model == "climatological_mean"
            is_persistence = model == "persistence"
            is_ensemble = model_meta[model]["is_ensemble"]
            series: dict = {}
            for var, ts in vars_.items():
                c_var = c_ts.get(var, {})
                x, y_raw, y_ratio = [], [], []
                for issued in sorted(ts):
                    mae = ts[issued]
                    mae_display = (
                        _diff_to_f(mae) if var in ("temperature", "dewpoint")
                        else mae
                    )
                    c = c_var.get(issued)
                    ratio = 1.0 if is_baseline else (mae / c if c else None)
                    x.append(fmt.short_ts(issued))
                    y_raw.append(mae_display)
                    y_ratio.append(ratio)
                series[var] = {
                    "x": x, "y_raw": y_raw, "y_ratio": y_ratio,
                    "y_ratio_rolling": _rolling_mean(y_ratio),
                    "y_raw_rolling": _rolling_mean(y_raw),
                }
            # average series
            all_issued = sorted(set().union(*[set(ts) for ts in vars_.values()]))
            ax, ay_raw, ay_ratio = [], [], []
            for issued in all_issued:
                ratios, raws = [], []
                for var, ts in vars_.items():
                    if issued not in ts:
                        continue
                    mae = ts[issued]
                    c_var = c_ts.get(var, {})
                    c = c_var.get(issued)
                    if is_baseline or c:
                        ratios.append(1.0 if is_baseline else mae / c)
                    raws.append(mae)
                if ratios:
                    ax.append(fmt.short_ts(issued))
                    ay_ratio.append(sum(ratios) / len(ratios))
                    ay_raw.append(sum(raws) / len(raws) if raws else None)
            series["avg"] = {
                "x": ax, "y_raw": ay_raw, "y_ratio": ay_ratio,
                "y_ratio_rolling": _rolling_mean(ay_ratio),
                "y_raw_rolling": _rolling_mean(ay_raw),
            }
            result[str(lead)][model] = {
                "is_baseline": is_baseline,
                "is_persistence": is_persistence,
                "is_ensemble": is_ensemble,
                "model_id": model_meta[model]["model_id"],
                "series": series,
            }
    return result


def _bias_timeseries_data(rows: list) -> dict:
    """lead (str) -> model -> {is_persistence, is_ensemble, model_id, series: {var -> {x, y}}}"""
    raw: dict = {}
    model_meta: dict = {}
    for row in rows:
        lead = row["lead_hours"]
        model_meta[row["model"]] = {
            "is_ensemble": row["type"] == "ensemble",
            "model_id": row["model_id"],
        }
        raw.setdefault(lead, {}).setdefault(row["model"], {}).setdefault(
            row["variable"], {}
        )[row["issued_at"]] = row["avg_bias"]

    result: dict = {}
    for lead in sorted(raw):
        result[str(lead)] = {}
        for model, vars_ in raw[lead].items():
            is_persistence = model == "persistence"
            is_ensemble = model_meta[model]["is_ensemble"]
            series: dict = {}
            for var, ts in vars_.items():
                x, y = [], []
                for issued in sorted(ts):
                    bias = ts[issued]
                    bias_display = (
                        _diff_to_f(bias) if var in ("temperature", "dewpoint")
                        else bias
                    )
                    x.append(fmt.short_ts(issued))
                    y.append(bias_display)
                series[var] = {"x": x, "y": y}
            result[str(lead)][model] = {
                "is_persistence": is_persistence,
                "is_ensemble": is_ensemble,
                "model_id": model_meta[model]["model_id"],
                "series": series,
            }
    return result


def _lead_skill_data(summary_rows: list) -> dict:
    """variable -> model -> {model_id, is_persistence, is_ensemble, points: {lead -> avg_mae}}"""
    result: dict = {}
    for row in summary_rows:
        var = row["variable"]
        model = row["model"]
        if var not in result:
            result[var] = {}
        if model not in result[var]:
            result[var][model] = {
                "model_id": row["model_id"],
                "is_persistence": model == "persistence",
                "is_ensemble": row["type"] == "ensemble",
                "is_external": row["type"] == "external",
                "points": {},
            }
        mae = row["avg_mae"]
        if mae is not None:
            if var in ("temperature", "dewpoint"):
                mae = _diff_to_f(mae)
        result[var][model]["points"][row["lead_hours"]] = mae
    return result


def _heatmap_data(summary_rows: list) -> dict:
    """variable -> {models, model_ids, leads, z}"""
    lookup: dict = {}
    models_seen: dict = {}
    leads_seen: set = set()

    for row in summary_rows:
        var = row["variable"]
        model = row["model"]
        lead = row["lead_hours"]
        if model not in models_seen:
            models_seen[model] = row["model_id"]
        leads_seen.add(lead)
        mae = row["avg_mae"]
        if mae is not None:
            if var in ("temperature", "dewpoint"):
                mae = _diff_to_f(mae)
        lookup[(var, model, lead)] = mae

    sorted_models = sorted(models_seen.keys(), key=lambda m: models_seen[m])
    sorted_leads = sorted(leads_seen)

    result: dict = {}
    for var in VARIABLES:
        if not any((var, m, l) in lookup for m in sorted_models for l in sorted_leads):
            continue
        z = [
            [lookup.get((var, model, lead)) for lead in sorted_leads]
            for model in sorted_models
        ]
        result[var] = {
            "models": sorted_models,
            "model_ids": [models_seen[m] for m in sorted_models],
            "leads": sorted_leads,
            "z": z,
        }
    return result


def _diurnal_data(rows: list) -> dict:
    """variable -> model -> {model_id, is_persistence, is_ensemble, hours, bias, mae}"""
    raw: dict = {}
    model_meta: dict = {}
    for row in rows:
        var = row["variable"]
        model = row["model"]
        if model not in model_meta:
            model_meta[model] = {
                "model_id": row["model_id"],
                "is_persistence": model == "persistence",
                "is_ensemble": row["type"] == "ensemble",
                "is_external": row["type"] == "external",
            }
        raw.setdefault(var, {}).setdefault(model, {})[row["hour"]] = (
            row["avg_bias"], row["avg_mae"]
        )

    result: dict = {}
    for var, models in raw.items():
        result[var] = {}
        for model, hours_data in models.items():
            hours = sorted(hours_data.keys())
            bias_list, mae_list = [], []
            for h in hours:
                b, m = hours_data[h]
                if var in ("temperature", "dewpoint"):
                    b = _diff_to_f(b)
                    m = _diff_to_f(m)
                bias_list.append(b)
                mae_list.append(m)
            meta = model_meta[model]
            result[var][model] = {
                "model_id": meta["model_id"],
                "is_persistence": meta["is_persistence"],
                "is_ensemble": meta["is_ensemble"],
                "hours": hours,
                "bias": bias_list,
                "mae": mae_list,
            }
    return result


def _error_dist_data(rows: list) -> dict:
    """variable -> lead_str -> model -> {model_id, is_persistence, is_ensemble, errors}"""
    raw: dict = {}
    model_meta: dict = {}
    for row in rows:
        var = row["variable"]
        lead = str(row["lead_hours"])
        model = row["model"]
        if model not in model_meta:
            model_meta[model] = {
                "model_id": row["model_id"],
                "is_persistence": model == "persistence",
                "is_ensemble": row["type"] == "ensemble",
            }
        err = row["error"]
        if err is not None:
            if var in ("temperature", "dewpoint"):
                err = _diff_to_f(err)
        raw.setdefault(var, {}).setdefault(lead, {}).setdefault(model, []).append(err)

    result: dict = {}
    for var, leads in raw.items():
        result[var] = {}
        for lead, models in leads.items():
            result[var][lead] = {}
            for model, errors in models.items():
                meta = model_meta[model]
                result[var][lead][model] = {
                    "model_id": meta["model_id"],
                    "is_persistence": meta["is_persistence"],
                    "is_ensemble": meta["is_ensemble"],
                    "errors": [e for e in errors if e is not None],
                }
    return result


def _mae_timeseries_js(timeseries_data: dict) -> str:
    data_json = json.dumps(timeseries_data)
    filter_labels_json = json.dumps({
        "avg": "Average MAE",
        "temperature": "Temperature MAE (°F)",
        "dewpoint": "Dew Point MAE (°F)",
        "pressure": "Pressure MAE (hPa)",
    })
    return f"""const maeLeadData = {data_json};
const maeFilterLabels = {filter_labels_json};
const maeLeads = Object.keys(maeLeadData).map(Number).sort(function(a,b){{return a-b;}});

const MAE_PALETTE = ['#1f77b4','#ff7f0e','#2ca02c','#d62728','#9467bd','#8c564b','#e377c2'];
const maeAllModels = [...new Set(
    Object.values(maeLeadData).flatMap(function(d){{return Object.keys(d);}})
)].sort();
const maeModelColors = {{}};
maeAllModels.forEach(function(m, i) {{ maeModelColors[m] = MAE_PALETTE[i % MAE_PALETTE.length]; }});
if (maeAllModels.includes('bogo')) maeModelColors['bogo'] = '#b0d8b0';

let maeActiveVar = 'avg';
let verifMode = 'ratio';
let smoothMode = true;

function drawMaeCharts() {{
    maeLeads.forEach(function(lead) {{
        const leadData = maeLeadData[String(lead)] || {{}};
        const traces = Object.entries(leadData).map(function([model, info]) {{
            const s = (info.series || {{}})[maeActiveVar] || {{}};
            const isBaseline = info.is_baseline;
            const isPersistence = info.is_persistence;
            const isRef = isBaseline || isPersistence;
            const isEns = info.is_ensemble;
            const color = isRef ? '#aaaaaa' : maeModelColors[model];
            const dash = isBaseline ? 'longdash' : (isPersistence ? 'dot' : (isEns ? 'dash' : 'solid'));
            // smooth mode: use rolling avg as primary for non-reference models
            let y;
            if (verifMode === 'ratio') {{
                y = (smoothMode && !isRef) ? (s.y_ratio_rolling || []) : (s.y_ratio || []);
            }} else {{
                y = (smoothMode && !isRef) ? (s.y_raw_rolling || []) : (s.y_raw || []);
            }}
            return {{
                type: 'scatter',
                mode: isRef ? 'lines' : (smoothMode ? 'lines' : 'lines+markers'),
                name: String(info.model_id),
                x: s.x || [],
                y: y,
                line: {{ width: isRef ? 1.5 : 2, dash: dash, color: color }},
                marker: {{ size: 5, color: color }}
            }};
        }});
        // per-run mode: show rolling avg as secondary overlay in ratio mode
        if (!smoothMode && verifMode === 'ratio') {{
            Object.entries(leadData).forEach(function([model, info]) {{
                if (info.is_baseline || info.is_persistence) return;
                const s = (info.series || {{}})[maeActiveVar] || {{}};
                const yr = s.y_ratio_rolling || [];
                if (!yr.length) return;
                const color = maeModelColors[model];
                traces.push({{
                    type: 'scatter', mode: 'lines',
                    name: String(info.model_id) + ' (10-run avg)',
                    x: s.x || [], y: yr,
                    line: {{ width: 1.5, dash: 'dashdot', color: color }},
                    showlegend: false
                }});
            }});
        }}
        const isRatio = verifMode === 'ratio';
        const varLabel = isRatio
            ? (maeActiveVar === 'avg' ? 'average skill vs climo' : maeFilterLabels[maeActiveVar].replace(' MAE', ' skill vs climo'))
            : (maeFilterLabels[maeActiveVar] || maeActiveVar);
        const title = '+' + lead + 'h — ' + varLabel;
        const yRange = isRatio ? {{ rangemode: 'tozero' }} : {{ rangemode: 'tozero' }};
        const shapes = isRatio ? [{{
            type: 'line', xref: 'paper', x0: 0, x1: 1, yref: 'y', y0: 1, y1: 1,
            line: {{ color: '#dddddd', width: 1, dash: 'dot' }}
        }}] : [];
        Plotly.react('mae-chart-' + lead, traces, {{
            title: {{ text: title, font: {{ size: 13, family: '-apple-system, sans-serif' }} }},
            margin: {{ t: 40, b: 100, l: 50, r: 16 }},
            xaxis: {{ tickangle: 0, tickfont: {{ size: 10 }}, nticks: 5 }},
            yaxis: Object.assign({{ tickfont: {{ size: 11 }} }}, yRange),
            height: 380,
            showlegend: true,
            legend: {{ orientation: 'h', x: 0, y: -0.18, xanchor: 'left', yanchor: 'top', font: {{ size: 10 }} }},
            shapes: shapes,
            paper_bgcolor: 'white',
            plot_bgcolor: '#fafafa'
        }}, {{responsive: true}});
    }});
}}

function updateTableMode() {{
    const isRatio = verifMode === 'ratio';
    document.querySelectorAll('[data-raw][data-ratio]').forEach(function(el) {{
        el.textContent = isRatio ? el.dataset.ratio : el.dataset.raw;
    }});
    document.querySelectorAll('.col-avg-hdr').forEach(function(el) {{
        el.textContent = isRatio ? 'Avg vs climo' : 'Avg MAE';
    }});
    document.querySelectorAll('.col-24h-hdr').forEach(function(el) {{
        el.textContent = isRatio ? '+24h vs climo' : '+24h MAE';
    }});
}}

document.querySelectorAll('.mae-filter-btn').forEach(function(btn) {{
    btn.addEventListener('click', function() {{
        document.querySelectorAll('.mae-filter-btn').forEach(function(b) {{ b.classList.remove('active'); }});
        btn.classList.add('active');
        maeActiveVar = btn.dataset.var;
        drawMaeCharts();
    }});
}});

document.getElementById('raw-toggle').addEventListener('click', function() {{
    verifMode = verifMode === 'ratio' ? 'raw' : 'ratio';
    this.classList.toggle('active');
    this.textContent = verifMode === 'ratio' ? 'Raw values' : 'Skill ratio';
    drawMaeCharts();
    updateTableMode();
}});

document.getElementById('smooth-toggle').addEventListener('click', function() {{
    smoothMode = !smoothMode;
    this.classList.toggle('active');
    this.textContent = smoothMode ? 'Per-run detail' : 'Smooth';
    drawMaeCharts();
}});

drawMaeCharts();
"""


def _member_forecast_js(member_rows: list, lead_times: list) -> str:
    if not member_rows:
        return ""

    data: dict = {}
    for row in member_rows:
        mid = row["model_id"]
        memid = row["member_id"]
        data.setdefault(mid, {})
        if memid not in data[mid]:
            data[mid][memid] = {"name": row["member_name"] or str(memid), "vars": {}}
        entry = data[mid][memid]
        if row["member_name"] and not entry["name"]:
            entry["name"] = row["member_name"]
        v = row["value"]
        if row["variable"] in ("temperature", "dewpoint"):
            v = _to_f(v)
        entry["vars"].setdefault(row["variable"], {})[row["lead_hours"]] = v

    data_json = json.dumps(data)
    vars_json = json.dumps(VARIABLES)
    var_labels_json = json.dumps(_VARIABLE_LABEL)
    units_json = json.dumps(_UNIT)
    leads_json = json.dumps(lead_times)

    return f"""\
const memberFcstData = {data_json};
const memberFcstVars = {vars_json};
const memberFcstVarLabels = {var_labels_json};
const memberFcstUnits = {units_json};
const memberFcstLeads = {leads_json};

function buildMemberForecastTables(modelId) {{
    const members = memberFcstData[modelId] || {{}};
    const parts = [];
    Object.entries(members).sort(function(a, b) {{ return +a[0] - +b[0]; }}).forEach(function([mid, m]) {{
        const headerCells = memberFcstLeads.map(function(h) {{
            return '<th>+' + h + 'h</th>';
        }}).join('');
        const bodyRows = memberFcstVars.map(function(v) {{
            const varData = (m.vars || {{}})[v] || {{}};
            const cells = memberFcstLeads.map(function(h) {{
                const val = varData[h];
                if (val === null || val === undefined) return '<td>\u2014</td>';
                return '<td>' + val.toFixed(1) + (memberFcstUnits[v] || '') + '</td>';
            }}).join('');
            return '<tr><th>' + (memberFcstVarLabels[v] || v) + '</th>' + cells + '</tr>';
        }}).join('');
        parts.push(
            '<div style="margin-bottom:10px">'
            + '<div style="font-size:11px;font-weight:600;color:#6b3fa0;text-transform:uppercase;'
            + 'letter-spacing:0.03em;margin-bottom:4px">' + m.name + '</div>'
            + '<table class="forecast-table" style="font-size:12px">'
            + '<thead><tr><th>Variable</th>' + headerCells + '</tr></thead>'
            + '<tbody>' + bodyRows + '</tbody>'
            + '</table>'
            + '</div>'
        );
    }});
    return parts.join('') || '<p class="muted">no member data</p>';
}}

document.querySelectorAll('.mf-btn').forEach(function(btn) {{
    btn.addEventListener('click', function() {{
        const modelId = btn.dataset.modelId;
        const panel = document.getElementById('mfp-' + modelId);
        if (!panel) return;
        if (panel.style.display !== 'none') {{
            panel.style.display = 'none';
            btn.innerHTML = btn.innerHTML.replace('\u25b4', '\u25be');
            return;
        }}
        panel.innerHTML = buildMemberForecastTables(+modelId);
        panel.style.display = '';
        btn.innerHTML = btn.innerHTML.replace('\u25be', '\u25b4');
    }});
}});
"""


def _member_detail_js(member_rows: list) -> str:
    if not member_rows:
        return "const memberData = {};"

    data: dict = {}
    for row in member_rows:
        mae = row["avg_mae"]
        if row["variable"] in ("temperature", "dewpoint"):
            mae = _diff_to_f(mae)
        data.setdefault(row["model"], []).append({
            "member_id": row["member_id"],
            "member_name": row["member_name"],
            "variable": row["variable"],
            "lead_hours": row["lead_hours"],
            "avg_mae": mae,
            "n": row["n"],
        })

    data_json = json.dumps(data)
    vars_json = json.dumps(VARIABLES)
    var_labels_json = json.dumps(_VARIABLE_LABEL)
    units_json = json.dumps(_UNIT)

    return f"""\
const memberData = {data_json};
const memberVarLabels = {var_labels_json};
const memberUnits = {units_json};
const memberVariables = {vars_json};

function buildMemberTable(rows) {{
    const members = {{}};
    rows.forEach(function(r) {{
        const key = r.member_id + ':' + (r.member_name || r.member_id);
        if (!members[key]) members[key] = {{}};
        if (!members[key][r.variable]) members[key][r.variable] = {{sum: 0, n: 0}};
        if (r.avg_mae !== null) {{
            members[key][r.variable].sum += r.avg_mae * r.n;
            members[key][r.variable].n += r.n;
        }}
    }});
    const varCols = memberVariables.filter(function(v) {{
        return rows.some(function(r) {{ return r.variable === v; }});
    }});
    const headerCells = varCols.map(function(v) {{
        const unit = memberUnits[v] ? ' (' + memberUnits[v] + ')' : '';
        return '<th>Avg MAE' + unit + '</th>';
    }}).join('');
    const bodyRows = Object.entries(members).map(function([key, varData]) {{
        const label = key.split(':')[1];
        const cells = varCols.map(function(v) {{
            const d = varData[v];
            if (!d || d.n === 0) return '<td>\u2014</td>';
            return '<td>' + (d.sum / d.n).toFixed(2) + '</td>';
        }}).join('');
        return '<tr><th>' + label + '</th>' + cells + '</tr>';
    }}).join('');
    return '<table class="member-detail-table"><thead><tr><th>Member</th>' + headerCells + '</tr></thead><tbody>' + bodyRows + '</tbody></table>';
}}

document.querySelectorAll('.member-btn').forEach(function(btn) {{
    btn.addEventListener('click', function() {{
        const model = btn.dataset.model;
        const safe = model.replace(/_/g, '-').replace(/ /g, '-');
        const row = document.getElementById('mdr-' + safe);
        const container = document.getElementById('md-' + safe);
        if (!row) return;
        if (row.style.display !== 'none') {{
            row.style.display = 'none';
            container.innerHTML = '';
            return;
        }}
        const rows = memberData[model] || [];
        container.innerHTML = rows.length ? buildMemberTable(rows) : '<p class=\\"muted\\">no member data</p>';
        row.style.display = '';
    }});
}});
"""


def _chart_js(chart_data_dict: dict) -> str:
    data_json = json.dumps(chart_data_dict)
    var_labels_json = json.dumps({
        "temperature": "Temperature (\u00b0F)",
        "dewpoint": "Dew Point (\u00b0F)",
        "pressure": "Pressure (hPa)",
    })
    vars_json = json.dumps(VARIABLES)
    return f"""\
const fcstData = {data_json};
const fcstVarLabels = {var_labels_json};
const fcstVariables = {vars_json};
const FCST_PALETTE = ['#1f77b4','#ff7f0e','#2ca02c','#d62728','#9467bd','#8c564b','#e377c2'];

const fcstAllModels = [...new Set(
    Object.values(fcstData).flatMap(function(d) {{ return Object.keys(d); }})
)].sort();
const fcstModelColors = {{}};
fcstAllModels.filter(function(m) {{ return m !== 'persistence'; }}).forEach(function(m, i) {{
    fcstModelColors[m] = FCST_PALETTE[i % FCST_PALETTE.length];
}});
if (fcstAllModels.includes('persistence')) fcstModelColors['persistence'] = '#aaaaaa';
if (fcstAllModels.includes('bogo')) fcstModelColors['bogo'] = '#b0d8b0';

let fcstActiveVar = fcstVariables[0];

function drawFcstChart() {{
    const isMobile = window.innerWidth < 768;
    const mobileModels = new Set(['persistence', 'barogram_ensemble', 'nws', 'tempest_forecast']);
    const varData = fcstData[fcstActiveVar] || {{}};
    const entries = Object.entries(varData).filter(function([model]) {{
        return !isMobile || mobileModels.has(model);
    }});
    const traces = entries.map(function([model, d]) {{
        const isPersistence = model === 'persistence';
        const color = fcstModelColors[model] || '#888888';
        return {{
            type: 'scatter',
            mode: 'lines+markers',
            name: String(d.model_id),
            x: d.x,
            y: d.y,
            line: {{ width: 2, dash: isPersistence ? 'dot' : 'solid', color: color }},
            marker: {{ size: isPersistence ? 5 : 6, color: color }}
        }};
    }});
    Plotly.react('chart-forecast', traces, {{
        title: {{ text: fcstVarLabels[fcstActiveVar], font: {{ size: 13, family: '-apple-system, sans-serif' }} }},
        margin: {{ t: 40, b: isMobile ? 120 : 100, l: 50, r: 16 }},
        xaxis: {{ type: 'date', tickformat: '%b %e %H:%M', tickangle: 0, tickfont: {{ size: 10 }}, nticks: 5 }},
        yaxis: {{ tickfont: {{ size: 11 }} }},
        height: isMobile ? 360 : 420,
        showlegend: true,
        legend: {{ orientation: 'h', x: 0, y: -0.18, xanchor: 'left', yanchor: 'top', font: {{ size: 10 }} }},
        paper_bgcolor: 'white',
        plot_bgcolor: '#fafafa'
    }}, {{responsive: true}});
}}

document.querySelectorAll('.fcst-filter-btn').forEach(function(btn) {{
    btn.addEventListener('click', function() {{
        document.querySelectorAll('.fcst-filter-btn').forEach(function(b) {{ b.classList.remove('active'); }});
        btn.classList.add('active');
        fcstActiveVar = btn.dataset.var;
        drawFcstChart();
    }});
}});

drawFcstChart();
"""


def _bias_timeseries_js(bias_data: dict) -> str:
    data_json = json.dumps(bias_data)
    filter_labels_json = json.dumps({
        "temperature": "Temperature Bias (\u00b0F)",
        "dewpoint": "Dew Point Bias (\u00b0F)",
        "pressure": "Pressure Bias (hPa)",
    })
    return f"""const biasLeadData = {data_json};
const biasFilterLabels = {filter_labels_json};
const biasLeads = Object.keys(biasLeadData).map(Number).sort(function(a,b){{return a-b;}});

const BIAS_PALETTE = ['#1f77b4','#ff7f0e','#2ca02c','#d62728','#9467bd','#8c564b','#e377c2'];
const biasAllModels = [...new Set(
    Object.values(biasLeadData).flatMap(function(d){{return Object.keys(d);}})
)].sort();
const biasModelColors = {{}};
biasAllModels.forEach(function(m, i) {{ biasModelColors[m] = BIAS_PALETTE[i % BIAS_PALETTE.length]; }});
if (biasAllModels.includes('bogo')) biasModelColors['bogo'] = '#b0d8b0';

let biasActiveVar = 'temperature';

function drawBiasCharts() {{
    biasLeads.forEach(function(lead) {{
        const leadData = biasLeadData[String(lead)] || {{}};
        const traces = Object.entries(leadData).map(function([model, info]) {{
            const s = (info.series || {{}})[biasActiveVar] || {{}};
            const isPersistence = info.is_persistence;
            const isEns = info.is_ensemble;
            const color = isPersistence ? '#aaaaaa' : biasModelColors[model];
            return {{
                type: 'scatter',
                mode: 'lines+markers',
                name: String(info.model_id),
                x: s.x || [],
                y: s.y || [],
                line: {{
                    width: 2,
                    dash: isPersistence ? 'dot' : (isEns ? 'dash' : 'solid'),
                    color: color
                }},
                marker: {{ size: isPersistence ? 5 : 6, color: color }}
            }};
        }});
        const shapes = [{{
            type: 'line', xref: 'paper', x0: 0, x1: 1, yref: 'y', y0: 0, y1: 0,
            line: {{ color: '#dddddd', width: 1, dash: 'dot' }}
        }}];
        Plotly.react('bias-chart-' + lead, traces, {{
            title: {{ text: '+' + lead + 'h \u2014 ' + (biasFilterLabels[biasActiveVar] || biasActiveVar),
                      font: {{ size: 13, family: '-apple-system, sans-serif' }} }},
            margin: {{ t: 40, b: 60, l: 50, r: 16 }},
            xaxis: {{ tickangle: 0, tickfont: {{ size: 10 }}, nticks: 5 }},
            yaxis: {{ tickfont: {{ size: 11 }} }},
            height: 380,
            showlegend: false,
            shapes: shapes,
            paper_bgcolor: 'white',
            plot_bgcolor: '#fafafa'
        }}, {{responsive: true}});
    }});
}}

document.querySelectorAll('.bias-filter-btn').forEach(function(btn) {{
    btn.addEventListener('click', function() {{
        document.querySelectorAll('.bias-filter-btn').forEach(function(b) {{ b.classList.remove('active'); }});
        btn.classList.add('active');
        biasActiveVar = btn.dataset.var;
        drawBiasCharts();
    }});
}});

drawBiasCharts();
"""


def _lead_skill_js(skill_data: dict) -> str:
    data_json = json.dumps(skill_data)
    filter_labels_json = json.dumps({
        "temperature": "Temperature MAE (\u00b0F)",
        "dewpoint": "Dew Point MAE (\u00b0F)",
        "pressure": "Pressure MAE (hPa)",
    })
    return f"""const leadSkillData = {data_json};
const leadSkillFilterLabels = {filter_labels_json};

const LEAD_SKILL_PALETTE = ['#1f77b4','#ff7f0e','#2ca02c','#d62728','#9467bd','#8c564b','#e377c2'];
const leadSkillAllModels = [...new Set(
    Object.values(leadSkillData).flatMap(function(d){{return Object.keys(d);}})
)].sort();
const leadSkillModelColors = {{}};
leadSkillAllModels.filter(function(m){{return m !== 'persistence';}}).forEach(function(m, i) {{
    leadSkillModelColors[m] = LEAD_SKILL_PALETTE[i % LEAD_SKILL_PALETTE.length];
}});
if (leadSkillAllModels.includes('persistence')) leadSkillModelColors['persistence'] = '#aaaaaa';
if (leadSkillAllModels.includes('bogo')) leadSkillModelColors['bogo'] = '#b0d8b0';

let leadSkillActiveVar = 'temperature';

function drawLeadSkillChart() {{
    const varData = leadSkillData[leadSkillActiveVar] || {{}};
    const allLeads = [...new Set(
        Object.values(varData).flatMap(function(m){{return Object.keys(m.points).map(Number);}})
    )].sort(function(a,b){{return a-b;}});
    const traces = Object.entries(varData).map(function([model, info]) {{
        const isPersistence = info.is_persistence;
        const isEns = info.is_ensemble;
        const color = leadSkillModelColors[model] || '#888888';
        const y = allLeads.map(function(l) {{
            const v = info.points[l];
            return (v !== undefined && v !== null) ? v : null;
        }});
        return {{
            type: 'scatter',
            mode: 'lines+markers',
            name: String(info.model_id),
            x: allLeads,
            y: y,
            line: {{
                width: 2,
                dash: isPersistence ? 'dot' : (isEns ? 'dash' : 'solid'),
                color: color
            }},
            marker: {{ size: isPersistence ? 5 : 6, color: color }},
            connectgaps: false
        }};
    }});
    Plotly.react('lead-skill-chart', traces, {{
        title: {{ text: leadSkillFilterLabels[leadSkillActiveVar] || leadSkillActiveVar,
                  font: {{ size: 13, family: '-apple-system, sans-serif' }} }},
        margin: {{ t: 40, b: 60, l: 50, r: 16 }},
        xaxis: {{ title: 'Lead hours', tickvals: allLeads, tickfont: {{ size: 11 }} }},
        yaxis: {{ title: 'Avg MAE', rangemode: 'tozero', tickfont: {{ size: 11 }} }},
        height: 380,
        showlegend: false,
        paper_bgcolor: 'white',
        plot_bgcolor: '#fafafa'
    }}, {{responsive: true}});
}}

document.querySelectorAll('.lead-skill-filter-btn').forEach(function(btn) {{
    btn.addEventListener('click', function() {{
        document.querySelectorAll('.lead-skill-filter-btn').forEach(function(b) {{ b.classList.remove('active'); }});
        btn.classList.add('active');
        leadSkillActiveVar = btn.dataset.var;
        drawLeadSkillChart();
    }});
}});

drawLeadSkillChart();
"""


def _heatmap_js(heatmap_data: dict) -> str:
    data_json = json.dumps(heatmap_data)
    return f"""const heatmapData = {data_json};

let heatmapActiveVar = 'temperature';

function drawHeatmapChart() {{
    const d = heatmapData[heatmapActiveVar] || {{}};
    const models = d.models || [];
    const leads = d.leads || [];
    const z = d.z || [];
    const annotations = [];
    models.forEach(function(model, i) {{
        leads.forEach(function(lead, j) {{
            const val = (z[i] || [])[j];
            if (val !== null && val !== undefined) {{
                annotations.push({{
                    x: lead, y: model,
                    text: val.toFixed(2),
                    showarrow: false,
                    font: {{ size: 11, color: '#333' }}
                }});
            }}
        }});
    }});
    Plotly.react('heatmap-chart', [{{
        type: 'heatmap',
        x: leads,
        y: models,
        z: z,
        colorscale: 'RdYlGn',
        reversescale: false,
        showscale: true,
        hovertemplate: '%{{y}}<br>+%{{x}}h<br>MAE: %{{z:.2f}}<extra></extra>'
    }}], {{
        title: {{ text: 'Score Heatmap \u2014 ' + heatmapActiveVar.replace('_', ' '),
                  font: {{ size: 13, family: '-apple-system, sans-serif' }} }},
        margin: {{ t: 40, b: 60, l: 180, r: 16 }},
        xaxis: {{ title: 'Lead hours', tickvals: leads, tickfont: {{ size: 11 }} }},
        yaxis: {{ tickfont: {{ size: 11 }} }},
        height: 300,
        showlegend: false,
        annotations: annotations,
        paper_bgcolor: 'white',
        plot_bgcolor: '#fafafa'
    }}, {{responsive: true}});
}}

document.querySelectorAll('.heatmap-filter-btn').forEach(function(btn) {{
    btn.addEventListener('click', function() {{
        document.querySelectorAll('.heatmap-filter-btn').forEach(function(b) {{ b.classList.remove('active'); }});
        btn.classList.add('active');
        heatmapActiveVar = btn.dataset.var;
        drawHeatmapChart();
    }});
}});

drawHeatmapChart();
"""


def _diurnal_js(diurnal_data: dict) -> str:
    data_json = json.dumps(diurnal_data)
    filter_labels_json = json.dumps({
        "temperature": "Temperature (\u00b0F)",
        "dewpoint": "Dew Point (\u00b0F)",
        "pressure": "Pressure (hPa)",
    })
    return f"""const diurnalData = {data_json};
const diurnalFilterLabels = {filter_labels_json};

const DIURNAL_PALETTE = ['#1f77b4','#ff7f0e','#2ca02c','#d62728','#9467bd','#8c564b','#e377c2'];
const diurnalAllModels = [...new Set(
    Object.values(diurnalData).flatMap(function(d){{return Object.keys(d);}})
)].sort();
const diurnalModelColors = {{}};
diurnalAllModels.filter(function(m){{return m !== 'persistence';}}).forEach(function(m, i) {{
    diurnalModelColors[m] = DIURNAL_PALETTE[i % DIURNAL_PALETTE.length];
}});
if (diurnalAllModels.includes('persistence')) diurnalModelColors['persistence'] = '#aaaaaa';
if (diurnalAllModels.includes('bogo')) diurnalModelColors['bogo'] = '#b0d8b0';

let diurnalActiveVar = 'temperature';
let diurnalMode = 'bias';

function drawDiurnalChart() {{
    const varData = diurnalData[diurnalActiveVar] || {{}};
    const traces = Object.entries(varData).map(function([model, info]) {{
        const isPersistence = info.is_persistence;
        const isEns = info.is_ensemble;
        const color = diurnalModelColors[model] || '#888888';
        const y = diurnalMode === 'bias' ? info.bias : info.mae;
        return {{
            type: 'scatter',
            mode: 'lines+markers',
            name: String(info.model_id),
            x: info.hours,
            y: y,
            line: {{
                width: 2,
                dash: isPersistence ? 'dot' : (isEns ? 'dash' : 'solid'),
                color: color
            }},
            marker: {{ size: isPersistence ? 5 : 6, color: color }}
        }};
    }});
    const shapes = diurnalMode === 'bias' ? [{{
        type: 'line', xref: 'paper', x0: 0, x1: 1, yref: 'y', y0: 0, y1: 0,
        line: {{ color: '#dddddd', width: 1, dash: 'dot' }}
    }}] : [];
    const modeLabel = diurnalMode === 'bias' ? 'Bias' : 'MAE';
    Plotly.react('diurnal-chart', traces, {{
        title: {{ text: 'Diurnal ' + modeLabel + ' \u2014 ' + (diurnalFilterLabels[diurnalActiveVar] || diurnalActiveVar),
                  font: {{ size: 13, family: '-apple-system, sans-serif' }} }},
        margin: {{ t: 40, b: 60, l: 50, r: 16 }},
        xaxis: {{ title: 'Hour (local)', range: [-0.5, 23.5], tickfont: {{ size: 11 }} }},
        yaxis: {{ tickfont: {{ size: 11 }} }},
        height: 380,
        showlegend: false,
        shapes: shapes,
        paper_bgcolor: 'white',
        plot_bgcolor: '#fafafa'
    }}, {{responsive: true}});
}}

document.querySelectorAll('.diurnal-filter-btn').forEach(function(btn) {{
    btn.addEventListener('click', function() {{
        document.querySelectorAll('.diurnal-filter-btn').forEach(function(b) {{ b.classList.remove('active'); }});
        btn.classList.add('active');
        diurnalActiveVar = btn.dataset.var;
        drawDiurnalChart();
    }});
}});

document.getElementById('diurnal-mode-btn').addEventListener('click', function() {{
    diurnalMode = diurnalMode === 'bias' ? 'mae' : 'bias';
    this.classList.toggle('active');
    this.textContent = diurnalMode === 'bias' ? 'Show MAE' : 'Show Bias';
    drawDiurnalChart();
}});

drawDiurnalChart();
"""


def _error_dist_js(dist_data: dict) -> str:
    data_json = json.dumps(dist_data)
    return f"""const errorDistData = {data_json};

const ERROR_DIST_PALETTE = ['#1f77b4','#ff7f0e','#2ca02c','#d62728','#9467bd','#8c564b','#e377c2'];
const errorDistAllModels = [...new Set(
    Object.values(errorDistData).flatMap(function(byLead){{
        return Object.values(byLead).flatMap(function(d){{return Object.keys(d);}});
    }})
)].sort();
const errorDistModelColors = {{}};
errorDistAllModels.filter(function(m){{return m !== 'persistence';}}).forEach(function(m, i) {{
    errorDistModelColors[m] = ERROR_DIST_PALETTE[i % ERROR_DIST_PALETTE.length];
}});
if (errorDistAllModels.includes('persistence')) errorDistModelColors['persistence'] = '#aaaaaa';
if (errorDistAllModels.includes('bogo')) errorDistModelColors['bogo'] = '#b0d8b0';

let errorDistActiveVar = 'temperature';
let errorDistActiveLead = '6';

function drawErrorDistChart() {{
    const varData = (errorDistData[errorDistActiveVar] || {{}})[errorDistActiveLead] || {{}};
    const traces = Object.entries(varData).map(function([model, info]) {{
        const color = errorDistModelColors[model] || '#888888';
        return {{
            type: 'histogram',
            name: String(info.model_id),
            x: info.errors,
            opacity: 0.55,
            autobinx: true,
            marker: {{ color: color, line: {{ color: color, width: 1 }} }}
        }};
    }});
    const shapes = [{{
        type: 'line', xref: 'x', x0: 0, x1: 0, yref: 'paper', y0: 0, y1: 1,
        line: {{ color: '#333', width: 1.5, dash: 'dot' }}
    }}];
    const unitLabels = {{
        temperature: '\u00b0F', dewpoint: '\u00b0F', pressure: 'hPa'
    }};
    Plotly.react('error-dist-chart', traces, {{
        barmode: 'overlay',
        title: {{ text: 'Error Distribution \u2014 ' + errorDistActiveVar.replace('_', ' ') + ' +' + errorDistActiveLead + 'h',
                  font: {{ size: 13, family: '-apple-system, sans-serif' }} }},
        margin: {{ t: 40, b: 60, l: 50, r: 16 }},
        xaxis: {{ title: 'Error (forecast \u2212 observed) (' + (unitLabels[errorDistActiveVar] || '') + ')',
                  tickfont: {{ size: 11 }} }},
        yaxis: {{ title: 'Count', tickfont: {{ size: 11 }} }},
        height: 380,
        showlegend: false,
        shapes: shapes,
        paper_bgcolor: 'white',
        plot_bgcolor: '#fafafa'
    }}, {{responsive: true}});
}}

document.querySelectorAll('.error-dist-var-btn').forEach(function(btn) {{
    btn.addEventListener('click', function() {{
        document.querySelectorAll('.error-dist-var-btn').forEach(function(b) {{ b.classList.remove('active'); }});
        btn.classList.add('active');
        errorDistActiveVar = btn.dataset.var;
        drawErrorDistChart();
    }});
}});

document.querySelectorAll('.error-dist-lead-btn').forEach(function(btn) {{
    btn.addEventListener('click', function() {{
        document.querySelectorAll('.error-dist-lead-btn').forEach(function(b) {{ b.classList.remove('active'); }});
        btn.classList.add('active');
        errorDistActiveLead = btn.dataset.lead;
        drawErrorDistChart();
    }});
}});

drawErrorDistChart();
"""


def _ensemble_forecast_section(
    mean_rows: list, tempest, elevation_m: float = 0.0, nws_forecast: dict | None = None
) -> str:
    """Render the barogram_ensemble forecast section HTML.

    Returns a muted placeholder if no ensemble rows exist yet.
    """
    ens_rows = [
        r for r in mean_rows
        if r["model"] == "barogram_ensemble" and r["member_id"] == 0
    ]
    # {lead_hours: {variable: value}} for tempest_forecast reference
    tempest_fcst_by_lead: dict[int, dict[str, float | None]] = {}
    for row in mean_rows:
        if row["model"] == "tempest_forecast" and row["member_id"] == 0:
            tempest_fcst_by_lead.setdefault(row["lead_hours"], {})[row["variable"]] = row["value"]

    if not ens_rows:
        return (
            '<section class="section">\n'
            '  <h2>Ensemble Forecast</h2>\n'
            '  <p class="muted">Ensemble model not yet available &mdash; in development.</p>\n'
            '</section>\n'
        )

    # {variable: {lead_hours: (value, spread)}}
    table: dict[str, dict[int, tuple]] = {v: {} for v in VARIABLES}
    lead_valid_at: dict[int, int] = {}
    issued_at = None
    for row in ens_rows:
        if row["variable"] in table:
            table[row["variable"]][row["lead_hours"]] = (row["value"], row["spread"])
        lead_valid_at.setdefault(row["lead_hours"], row["valid_at"])
        if issued_at is None:
            issued_at = row["issued_at"]

    # keep as raw SI (Celsius, m/s, hPa) so _fmt_value can apply display conversions uniformly
    slp_offset = _slp_correction(tempest, elevation_m)
    now: dict[str, float | None] = {}
    if tempest:
        sp = tempest["station_pressure"]
        slp = tempest["sea_level_pressure"]
        if slp is None and sp is not None and elevation_m > 0.0 and tempest["air_temp"] is not None:
            slp = fmt.to_slp(sp, tempest["air_temp"], elevation_m)
        now = {
            "temperature": tempest["air_temp"],
            "dewpoint": tempest["dew_point"],
            "pressure": slp if slp is not None else sp,
            "wind_speed": tempest["wind_avg"],
        }

    def _nws_at(target_ts: int) -> dict | None:
        """Return NWS forecast entry nearest to target_ts, within 90 min."""
        if not nws_forecast:
            return None
        best = min(nws_forecast, key=lambda t: abs(t - target_ts))
        if abs(best - target_ts) > 5400:
            return None
        return nws_forecast[best]

    def _tempest_fcst_at(lead: int) -> dict | None:
        entry = tempest_fcst_by_lead.get(lead)
        return entry if entry else None

    def _card(label: str, is_now: bool, temp_val, dew_val, pres_val, wind_val,
              temp_spread=None, nws=None, tempest_fcst=None, precip_val=None) -> str:
        cls = 'fcst-row now-row' if is_now else 'fcst-row'
        # temperature — always the hero
        if temp_val is not None:
            temp_disp = f"{_to_f(temp_val):.0f}\u00b0F"
            spread_html = ""
            if temp_spread is not None and temp_spread > 0:
                spread_disp = _diff_to_f(temp_spread)
                spread_html = (
                    f'<div class="fcst-temp-spread">&pm;{spread_disp:.1f}\u00b0</div>'
                )
            else:
                spread_html = '<div class="fcst-temp-spread"></div>'
            temp_html = f'<div class="fcst-temp">{temp_disp}</div>{spread_html}'
        else:
            temp_html = '<div class="fcst-no-data">&mdash;</div><div class="fcst-temp-spread"></div>'

        # secondary details
        details = []
        if dew_val is not None:
            details.append(
                f'<span class="detail-label">Dew</span> {_to_f(dew_val):.0f}\u00b0F'
            )
        if pres_val is not None:
            details.append(
                f'<span class="detail-label">Pres</span> {pres_val:.1f} hPa'
            )
        if wind_val is not None:
            details.append(
                f'<span class="detail-label">Wind</span> {_to_mph(wind_val):.0f} mph'
            )
        if precip_val is not None:
            details.append(
                f'<span class="detail-label">Precip</span> {round(precip_val * 100)}%'
            )
        details_html = (
            '<div class="fcst-details">' + "<br>".join(details) + "</div>"
            if details else ""
        )

        tempest_fcst_html = ""
        if tempest_fcst:
            tf_lines = []
            if tempest_fcst.get("temperature") is not None:
                tf_temp_f = _to_f(tempest_fcst["temperature"])
                delta_f = tf_temp_f - _to_f(temp_val) if temp_val is not None else None
                delta_html = ""
                if delta_f is not None:
                    sign = "+" if delta_f >= 0 else ""
                    delta_html = f' <span class="fcst-delta">{sign}{delta_f:.0f}\u00b0</span>'
                tf_lines.append(
                    f'<span class="detail-label">Temp</span> <span class="fcst-ref-temp">{tf_temp_f:.0f}\u00b0F</span>{delta_html}'
                )
            if tempest_fcst.get("dewpoint") is not None:
                tf_lines.append(
                    f'<span class="detail-label">Dew</span> {_to_f(tempest_fcst["dewpoint"]):.0f}\u00b0F'
                )
            if tempest_fcst.get("precip_prob") is not None:
                tf_lines.append(
                    f'<span class="detail-label">Precip</span> {round(tempest_fcst["precip_prob"] * 100)}%'
                )
            if tf_lines:
                tempest_fcst_html = (
                    '<div class="fcst-ref">'
                    '<span class="fcst-ref-lbl">Tempest</span>'
                    + "<br>".join(tf_lines)
                    + "</div>"
                )

        nws_html = ""
        if nws:
            nws_lines = []
            if nws.get("temperature") is not None:
                nws_temp_f = _to_f(nws["temperature"])
                delta_f = nws_temp_f - _to_f(temp_val) if temp_val is not None else None
                delta_html = ""
                if delta_f is not None:
                    sign = "+" if delta_f >= 0 else ""
                    delta_html = f' <span class="fcst-delta">{sign}{delta_f:.0f}\u00b0</span>'
                nws_lines.append(
                    f'<span class="detail-label">Temp</span> <span class="fcst-ref-temp">{nws_temp_f:.0f}\u00b0F</span>{delta_html}'
                )
            if nws.get("dewpoint") is not None:
                nws_lines.append(
                    f'<span class="detail-label">Dew</span> {_to_f(nws["dewpoint"]):.0f}\u00b0F'
                )
            if nws.get("precip_prob") is not None:
                nws_lines.append(
                    f'<span class="detail-label">Precip</span> {round(nws["precip_prob"] * 100)}%'
                )
            if nws_lines:
                nws_html = (
                    '<div class="fcst-ref">'
                    '<span class="fcst-ref-lbl">NWS</span>'
                    + "<br>".join(nws_lines)
                    + "</div>"
                )

        return (
            f'<div class="{cls}">'
            f'<div class="fcst-row-main">'
            f'<div class="fcst-label">{label}</div>'
            f'{temp_html}'
            f'{details_html}'
            f'</div>'
            f'<div class="fcst-row-refs">'
            f'{tempest_fcst_html}'
            f'{nws_html}'
            f'</div>'
            f'</div>'
        )

    cards_html = ""
    # Now card — from live Tempest obs
    now_temp = now.get("temperature")
    now_dew = now.get("dewpoint")
    now_pres = now.get("pressure")
    now_wind = now.get("wind_speed")
    cards_html += _card("Now", True, now_temp, now_dew, now_pres, now_wind)

    for lead in [6, 12, 18, 24]:
        vat = lead_valid_at.get(lead)
        label = (
            datetime.fromtimestamp(vat, tz=fmt.CENTRAL).strftime("%-I %p").lstrip("0")
            if vat else f"+{lead}h"
        )
        t_cell = table.get("temperature", {}).get(lead)
        d_cell = table.get("dewpoint", {}).get(lead)
        p_cell = table.get("pressure", {}).get(lead)
        pp_cell = table.get("precip_prob", {}).get(lead)
        t_val = t_cell[0] if t_cell else None
        t_spread = t_cell[1] if t_cell else None
        d_val = d_cell[0] if d_cell else None
        p_raw = p_cell[0] if p_cell else None
        p_val = p_raw + slp_offset if p_raw is not None else None
        pp_val = pp_cell[0] if pp_cell else None
        nws_entry = _nws_at(vat) if vat else None
        tf_entry = _tempest_fcst_at(lead)
        cards_html += _card(label, False, t_val, d_val, p_val, None, t_spread, nws_entry, tf_entry, pp_val)

    issued_str = fmt.ts(issued_at) if issued_at else "&mdash;"
    return (
        '<section class="section" id="forecast">\n'
        '  <h2>Ensemble Forecast</h2>\n'
        f'  <div class="obs-time">issued {issued_str}</div>\n'
        f'  <div class="forecast-rows">{cards_html}</div>\n'
        '</section>\n'
    )


def _learnings_clearness_index(solar_rad: float | None, lat_deg: float, ts: int) -> float | None:
    """Clearness index for the dashboard: observed / clear-sky solar radiation."""
    import math
    if solar_rad is None:
        return None
    d = datetime.fromtimestamp(ts)
    doy = d.timetuple().tm_yday
    hour = d.hour + d.minute / 60.0
    decl = math.radians(-23.45 * math.cos(math.radians(360 / 365 * (doy + 10))))
    lat = math.radians(lat_deg)
    ha = math.radians(15 * (hour - 12))
    sin_alt = (
        math.sin(lat) * math.sin(decl)
        + math.cos(lat) * math.cos(decl) * math.cos(ha)
    )
    if sin_alt <= 0.05:
        return None
    et_irr = 1361 * (1 + 0.033 * math.cos(math.radians(360 * doy / 365)))
    cs = et_irr * sin_alt * 0.75
    if cs <= 0:
        return None
    return max(0.0, min(1.0, solar_rad / cs))


_SKY_FRAC = {"CLR": 0.0, "SKC": 0.0, "FEW": 0.15, "SCT": 0.40, "BKN": 0.70, "OVC": 1.0}


def _ap_signal_state(conn_in: sqlite3.Connection, obs) -> dict | None:
    """Compute live airmass_precip signal states from the current observation."""
    if obs is None:
        return None
    import models.airmass_precip as ap
    from models.surface_signs import (
        _LOOKUP_SEC,
        _SIGNAL_WINDOW_SEC,
        _build_solar_climo,
        _find_nearest_ts,
        _obs_in_window,
        _solar_cloud_category,
        _wind_rotation_category,
    )
    ts = obs["timestamp"]
    obs_30d = db.tempest_obs_in_range(conn_in, ts - 30 * 86400, ts)
    solar_climo = _build_solar_climo(obs_30d)
    by_ts = {r["timestamp"]: r for r in obs_30d}
    sorted_ts = sorted(by_ts)

    obs_3h = db.nearest_tempest_obs(conn_in, ts - 3 * 3600, window_sec=_LOOKUP_SEC)
    obs_1h = db.nearest_tempest_obs(conn_in, ts - 3600, window_sec=_LOOKUP_SEC)
    window_obs = _obs_in_window(sorted_ts, by_ts, ts - _SIGNAL_WINDOW_SEC, ts)

    m = ap._moisture_cat(obs)
    p = ap._ptend_cat(obs, obs_3h)
    cloud = _solar_cloud_category(obs, solar_climo)

    return {
        1: m,
        2: p,
        3: cloud,
        4: ap._wind_sector_4(obs),
        5: ap._active_precip_cat(obs, obs_1h),
        6: (m, p) if m is not None and p is not None else None,
        7: _wind_rotation_category(window_obs),
        8: (cloud, m) if cloud is not None and m is not None else None,
    }


_AP_SIGNAL_DESC = {
    1: "T−Td spread",
    2: "3h ΔP",
    3: "solar vs climo",
    4: "wind direction",
    5: "last-hour precip",
    6: "T−Td × ΔP",
    7: "3h wind rotation",
    8: "cloud × moisture",
}
_AP_WET = {"moist", "falling", "heavy_cloud", "raining", "backing"}
_AP_DRY = {"dry", "rising", "clear", "veering"}


def _ap_badge(state) -> str:
    if state is None:
        return '<span class="ap-none">–</span>'
    if isinstance(state, tuple):
        return " · ".join(_ap_badge(s) for s in state)
    cls = "ap-wet" if state in _AP_WET else ("ap-dry" if state in _AP_DRY else "ap-neutral")
    return f'<span class="ap-badge {cls}">{state}</span>'


def _ap_signal_state_html(signal_state: dict | None, member_rows: list) -> str:
    """Table of live signal states and per-member precip_prob forecasts."""
    if signal_state is None:
        return '<p class="no-data">No Tempest observation available.</p>'

    from models.airmass_precip import _MEMBER_NAMES

    by_mid: dict[int, dict[int, float | None]] = {}
    for row in member_rows:
        if row["model"] == "airmass_precip":
            mid = row["member_id"]
            if mid > 0 and row["variable"] == "precip_prob":
                by_mid.setdefault(mid, {})[row["lead_hours"]] = row["value"]

    header = (
        "<tr>"
        "<th>#</th><th>member</th><th>signal</th><th>state</th>"
        "<th>+6h</th><th>+12h</th><th>+18h</th><th>+24h</th>"
        "</tr>"
    )
    body = ""
    for mid, name in _MEMBER_NAMES:
        state_cell = _ap_badge(signal_state.get(mid))
        sig = _AP_SIGNAL_DESC.get(mid, "")
        leads = ""
        for lead in [6, 12, 18, 24]:
            v = by_mid.get(mid, {}).get(lead)
            leads += (
                '<td class="ap-none">–</td>' if v is None
                else f"<td>{round(v * 100)}%</td>"
            )
        body += (
            f'<tr><td class="model-id-cell">{mid}</td>'
            f"<td>{name}</td>"
            f'<td style="color:#888;font-size:12px">{sig}</td>'
            f"<td>{state_cell}</td>{leads}</tr>"
        )

    return (
        '<p class="chart-legend-note">Signal state and precip probability at last forecast run. '
        "Blue = precip-favorable &nbsp;·&nbsp; "
        "Amber = unfavorable &nbsp;·&nbsp; "
        "Grey = neutral.</p>"
        f'<table class="ap-signal-table"><thead>{header}</thead>'
        f"<tbody>{body}</tbody></table>"
    )


def _learnings_data(conn_in: sqlite3.Connection, conn_out: sqlite3.Connection) -> dict:
    now = int(time.time())
    start_30d = now - 30 * 86400

    # --- Hypothesis A: airmass_diurnal members 1 vs 3 ---
    mae_rows = conn_out.execute(
        """
        select f.issued_at, f.member_id, f.lead_hours, f.mae
        from forecasts f
        where f.model_id = 7
          and f.member_id in (0, 1, 3)
          and f.variable = 'temperature'
          and f.scored_at is not null
        order by f.issued_at asc
        """
    ).fetchall()

    weight_rows = conn_out.execute(
        """
        select w.member_id, mem.name as member_name, w.variable, w.lead_hours, w.weight
        from weights w
        join members mem on mem.model_id = w.model_id and mem.member_id = w.member_id
        where w.model_id = 7
          and w.member_id in (1, 3)
        order by w.variable, w.lead_hours, w.member_id
        """
    ).fetchall()

    # --- Hypothesis B: clearness index vs NWS sky cover ---
    location = db.tempest_station_location(conn_in)
    lat = location[0] if location else None

    clearness_pts: list[tuple[int, float]] = []
    if lat:
        for row in db.tempest_solar_history(conn_in, start_30d, now):
            k = _learnings_clearness_index(row["solar_radiation"], lat, row["timestamp"])
            if k is not None:
                clearness_pts.append((row["timestamp"], k))

    sky_pts: list[tuple[int, float]] = []
    for row in db.sky_cover_history(conn_in, start_30d, now):
        sc = row["sky_cover"]
        if sc:
            frac = _SKY_FRAC.get(sc[:3].upper())
            if frac is not None:
                sky_pts.append((row["timestamp"], frac))

    # --- Hypothesis C: ensemble underperformance ---
    # per-run MAE for all base models + ensemble, member_id=0
    all_model_mae_rows = conn_out.execute(
        """
        select f.issued_at, f.model, f.variable, f.lead_hours, avg(f.mae) as mae
        from forecasts f
        where f.member_id = 0 and f.scored_at is not null
        group by f.issued_at, f.model, f.variable, f.lead_hours
        order by f.issued_at
        """
    ).fetchall()

    # --- Hypothesis D: pressure_tendency paradox ---
    pt_mae_rows = conn_out.execute(
        """
        select f.issued_at, f.variable, f.lead_hours, avg(f.mae) as mae
        from forecasts f
        where f.model = 'pressure_tendency'
          and f.member_id = 0
          and f.scored_at is not null
        group by f.issued_at, f.variable, f.lead_hours
        order by f.issued_at
        """
    ).fetchall()

    # --- Hypothesis E: climo_deviation signal decay ---
    decay_mae_rows = conn_out.execute(
        """
        select f.issued_at, f.model, f.lead_hours, avg(f.mae) as mae
        from forecasts f
        where f.model in ('climo_deviation', 'persistence')
          and f.member_id = 0
          and f.variable = 'temperature'
          and f.scored_at is not null
        group by f.issued_at, f.model, f.lead_hours
        order by f.issued_at
        """
    ).fetchall()

    # --- Hypothesis F: model specialization map ---
    spec_rows = conn_out.execute(
        """
        select variable, lead_hours, model, avg(mae) as avg_mae, count(*) as n
        from forecasts
        where member_id = 0
          and scored_at is not null
          and model != 'barogram_ensemble'
        group by variable, lead_hours, model
        """
    ).fetchall()

    # --- Hypothesis G: diurnal_curve vs climo_deviation ---
    diurnal_climo_rows = conn_out.execute(
        """
        select f.issued_at, f.model, f.lead_hours, avg(f.mae) as mae
        from forecasts f
        where f.model in ('diurnal_curve', 'climo_deviation')
          and f.member_id = 0
          and f.variable = 'temperature'
          and f.scored_at is not null
        group by f.issued_at, f.model, f.lead_hours
        order by f.issued_at
        """
    ).fetchall()

    def _roll(vals, window=12):
        """Rolling mean over a list, ignoring Nones. Returns same-length list."""
        result = []
        for i in range(len(vals)):
            chunk = [x for x in vals[max(0, i - window + 1):i + 1] if x is not None]
            result.append(round(sum(chunk) / len(chunk), 2) if chunk else None)
        return result

    # structure Hyp A MAE data by lead then member
    mae_by_lead: dict[int, dict[int, dict]] = {}
    for row in mae_rows:
        lead = row["lead_hours"]
        mid = row["member_id"]
        mae_by_lead.setdefault(lead, {}).setdefault(mid, {"x": [], "y": []})
        mae_by_lead[lead][mid]["x"].append(fmt.short_ts(row["issued_at"]))
        mae_by_lead[lead][mid]["y"].append(_diff_to_f(row["mae"]))

    # structure Hyp B: daily-aggregated clearness and sky cover
    # raw clearness_pts / sky_pts are 5-min cadence (8k+ points over 30d);
    # aggregate to daily means so the chart shows ~30 readable points
    daily_k: dict[str, list[float]] = {}
    for ts, k in clearness_pts:
        d = str(date.fromtimestamp(ts))
        daily_k.setdefault(d, []).append(k)
    clearness_daily = [(d, round(sum(v) / len(v), 3)) for d, v in sorted(daily_k.items())]

    daily_sky_frac: dict[str, list[float]] = {}
    for ts, frac in sky_pts:
        d = str(date.fromtimestamp(ts))
        daily_sky_frac.setdefault(d, []).append(frac)
    sky_daily = [(d, round(sum(v) / len(v), 3)) for d, v in sorted(daily_sky_frac.items())]

    # structure Hyp C: rolling gap (ensemble − climo_deviation) per run
    # single line is far clearer than 3 noisy overlapping series
    hyp_c_gap: dict[tuple, dict] = {}
    for key in [("temperature", 6), ("temperature", 24)]:
        var, lead = key
        ens_by_ts: dict[int, float] = {}
        climo_by_ts: dict[int, float] = {}
        for row in all_model_mae_rows:
            if row["variable"] == var and row["lead_hours"] == lead:
                if row["model"] == "barogram_ensemble":
                    ens_by_ts[row["issued_at"]] = row["mae"]
                elif row["model"] == "climo_deviation":
                    climo_by_ts[row["issued_at"]] = row["mae"]
        common = sorted(set(ens_by_ts) & set(climo_by_ts))
        if not common:
            continue
        raw_gaps = [_diff_to_f(ens_by_ts[ts] - climo_by_ts[ts]) for ts in common]
        hyp_c_gap[key] = {
            "x": [fmt.short_ts(ts) for ts in common],
            "y": _roll(raw_gaps, window=10),
        }

    # structure Hyp D: {(variable, lead): {x, y}} with rolling mean
    hyp_d: dict[tuple, dict] = {}
    for row in pt_mae_rows:
        key = (row["variable"], row["lead_hours"])
        hyp_d.setdefault(key, {"x": [], "y_raw": []})
        mae_val = row["mae"]
        if row["variable"] in ("temperature", "dewpoint"):
            mae_val = _diff_to_f(mae_val)
        hyp_d[key]["x"].append(fmt.short_ts(row["issued_at"]))
        hyp_d[key]["y_raw"].append(mae_val)
    for key in hyp_d:
        hyp_d[key]["y"] = _roll(hyp_d[key]["y_raw"], window=10)

    # structure Hyp E: {lead: {model: {x, y}}} with rolling mean
    hyp_e: dict[int, dict[str, dict]] = {}
    for row in decay_mae_rows:
        lead = row["lead_hours"]
        model = row["model"]
        hyp_e.setdefault(lead, {}).setdefault(model, {"x": [], "y_raw": []})
        hyp_e[lead][model]["x"].append(fmt.short_ts(row["issued_at"]))
        hyp_e[lead][model]["y_raw"].append(_diff_to_f(row["mae"]))
    for lead in hyp_e:
        for model in hyp_e[lead]:
            hyp_e[lead][model]["y"] = _roll(hyp_e[lead][model]["y_raw"], window=10)

    # ensemble (model_id=100) weights per base model: {(model_name, variable, lead_hours): weight}
    ens_wt_rows = conn_out.execute(
        """
        select m.name as model_name, w.variable, w.lead_hours, w.weight
        from weights w
        join members m on m.model_id = 100 and m.member_id = w.member_id
        where w.model_id = 100
        """
    ).fetchall()
    ensemble_weights = {
        (r["model_name"], r["variable"], r["lead_hours"]): r["weight"]
        for r in ens_wt_rows
    }

    # structure Hyp F: best model per (variable, lead) → {model, avg_mae, n}
    hyp_f: dict[tuple, dict] = {}
    for row in spec_rows:
        key = (row["variable"], row["lead_hours"])
        if key not in hyp_f or row["avg_mae"] < hyp_f[key]["avg_mae"]:
            hyp_f[key] = {"model": row["model"], "avg_mae": row["avg_mae"], "n": row["n"]}

    # also keep full per-model averages for heatmap annotation
    spec_all: dict[tuple, list] = {}
    for row in spec_rows:
        key = (row["variable"], row["lead_hours"])
        spec_all.setdefault(key, []).append({
            "model": row["model"], "avg_mae": row["avg_mae"], "n": row["n"]
        })

    # structure Hyp G: {lead: {model: {x, y}}} with rolling mean
    hyp_g: dict[int, dict[str, dict]] = {}
    for row in diurnal_climo_rows:
        lead = row["lead_hours"]
        model = row["model"]
        hyp_g.setdefault(lead, {}).setdefault(model, {"x": [], "y_raw": []})
        hyp_g[lead][model]["x"].append(fmt.short_ts(row["issued_at"]))
        hyp_g[lead][model]["y_raw"].append(_diff_to_f(row["mae"]))
    for lead in hyp_g:
        for model in hyp_g[lead]:
            hyp_g[lead][model]["y"] = _roll(hyp_g[lead][model]["y_raw"], window=10)

    return {
        "mae_by_lead": mae_by_lead,
        "weight_rows": [
            {
                "member_id": r["member_id"],
                "member_name": r["member_name"],
                "variable": r["variable"],
                "lead_hours": r["lead_hours"],
                "weight": r["weight"],
            }
            for r in weight_rows
        ],
        "clearness_daily": clearness_daily,
        "sky_daily": sky_daily,
        "hyp_c_gap": hyp_c_gap,
        "hyp_d": hyp_d,
        "hyp_e": hyp_e,
        "hyp_f": hyp_f,
        "spec_all": spec_all,
        "ensemble_weights": ensemble_weights,
        "hyp_g": hyp_g,
    }


def _learnings_weights_table_html(weight_rows: list) -> str:
    if not weight_rows:
        return (
            '<p class="no-data">Tuning weights not yet computed for airmass_diurnal'
            ' &mdash; run <code>barogram tune</code> after sufficient scored forecasts.</p>'
        )
    # group by (variable, lead_hours) → {member_id: weight}
    cells: dict = {}
    for r in weight_rows:
        key = (r["variable"], r["lead_hours"])
        cells.setdefault(key, {})[r["member_id"]] = r["weight"]

    tbody = ""
    for variable, lead in sorted(cells):
        w1 = cells[(variable, lead)].get(1)
        w3 = cells[(variable, lead)].get(3)
        fmt_w = lambda w: f"{w:.4f}" if w is not None else "&mdash;"
        tbody += (
            f"<tr><td>{variable}</td><td>+{lead}h</td>"
            f"<td>{fmt_w(w1)}</td><td>{fmt_w(w3)}</td></tr>"
        )

    return (
        '<div class="learnings-weights">'
        "<h4>Current tuning weights (members 1 and 3)</h4>"
        '<table class="score-table"><thead>'
        "<tr><th>variable</th><th>lead</th>"
        "<th>member 1<br><small>clearness-only</small></th>"
        "<th>member 3<br><small>clearness-pressure-projected</small></th>"
        "</tr></thead>"
        f"<tbody>{tbody}</tbody></table></div>"
    )


def _learnings_section_html(data: dict) -> str:
    has_mae = bool(data["mae_by_lead"])
    has_clearness = bool(data["clearness_daily"]) or bool(data["sky_daily"])
    has_hyp_c = any(data["hyp_c_gap"])
    has_hyp_d = any(data["hyp_d"])
    has_hyp_e = any(data["hyp_e"])
    has_hyp_f = bool(data["hyp_f"])
    has_hyp_g = any(data["hyp_g"])
    weights_html = _learnings_weights_table_html(data["weight_rows"])

    no_data = '<p class="no-data">Not enough scored data yet. Check back after several forecast cycles.</p>'

    return (
        '<section class="section" id="learnings">\n'
        "  <h2>Learnings</h2>\n"
        '  <p class="learnings-intro">Tracked hypotheses that accumulate evidence over time.'
        " Thin data is expected early &mdash; the goal is to watch these relationships evolve.</p>\n"
        "\n"
        # --- Hypothesis A ---
        "  <h3 class=\"obs-subhead\">Hypothesis A: Clearness persistence vs. pressure projection</h3>\n"
        '  <p class="learnings-desc">'
        "<strong>Question:</strong> Does projecting the solar clearness index forward "
        "via pressure tendency (airmass_diurnal member 3) reduce temperature MAE compared to "
        "simply persisting it (member 1)? The weights table shows whether "
        "<code>barogram tune</code> tracks the better performer over time."
        "</p>\n"
        + (
            (
                '  <details class="collapsible-section">\n'
                '  <summary>show charts</summary>\n'
                '  <div class="learnings-hyp-grid">'
                '<div class="chart-container"><div id="learnings-mae-6h"></div></div>'
                '<div class="chart-container"><div id="learnings-mae-12h"></div></div>'
                "</div>\n"
                + f"  {weights_html}\n"
                + "  </details>\n"
            )
            if has_mae
            else no_data + "\n"
        )
        + "\n"
        # --- Hypothesis B ---
        '  <h3 class="obs-subhead">Hypothesis B: Solar clearness index vs. NWS sky cover</h3>\n'
        '  <p class="learnings-desc">'
        "<strong>Question:</strong> Does the Tempest station&rsquo;s solar-derived clearness "
        "index (k) agree with NWS-reported sky cover? Each point is a daily average. "
        "<strong>What to look for:</strong> the two lines should move inversely "
        "(clearness drops on cloudy days, sky cover rises). If they move <em>together</em> or "
        "persistently diverge, there may be a sensor issue or a real local microclimate "
        "difference between the Tempest site and KMSP. "
        "NWS sky cover is never used as a model input &mdash; this is validation only."
        "</p>\n"
        + (
            '  <details class="collapsible-section">\n'
            '  <summary>show chart</summary>\n'
            '  <div class="chart-container"><div id="learnings-clearness-chart"></div></div>\n'
            '  </details>\n'
            if has_clearness
            else no_data + "\n"
        )
        + "\n"
        # --- Hypothesis C ---
        '  <h3 class="obs-subhead">Hypothesis C: Is the ensemble closing the gap on its best member?</h3>\n'
        '  <p class="learnings-desc">'
        "<strong>Question:</strong> The ensemble is currently worse than "
        "<code>climo_deviation</code> on temperature at every lead. "
        "The line shows the rolling gap (ensemble MAE &minus; climo_deviation MAE, "
        "10-run mean) over time. "
        "<strong>What to look for:</strong> the line trending toward or below zero &mdash; "
        "that means the ensemble is learning to match or beat its best member. "
        "A flat or rising line means the weighting is not converging."
        "</p>\n"
        + (
            '  <details class="collapsible-section">\n'
            '  <summary>show charts</summary>\n'
            '  <div class="learnings-hyp-grid">'
            '<div class="chart-container"><div id="learnings-hyp-c-6h"></div></div>'
            '<div class="chart-container"><div id="learnings-hyp-c-24h"></div></div>'
            "</div>\n"
            "  </details>\n"
            if has_hyp_c
            else no_data + "\n"
        )
        + "\n"
        # --- Hypothesis D ---
        '  <h3 class="obs-subhead">Hypothesis D: pressure_tendency &mdash; best and worst simultaneously</h3>\n'
        '  <p class="learnings-desc">'
        "<strong>Question:</strong> <code>pressure_tendency</code> is the best model for "
        "dewpoint at all leads, but its pressure MAE climbs steeply (40+ hPa at 24h vs "
        "persistence&rsquo;s 5 hPa). Both lines are shown at +12h with a 10-run rolling mean. "
        "<strong>What to look for:</strong> the two lines diverging &mdash; low dewpoint, "
        "high pressure. That&rsquo;s expected and confirms the model design trade-off. "
        "If pressure MAE starts dropping back toward dewpoint level, something has changed."
        "</p>\n"
        + (
            '  <details class="collapsible-section">\n'
            '  <summary>show chart</summary>\n'
            '  <div class="chart-container"><div id="learnings-hyp-d-chart"></div></div>\n'
            '  </details>\n'
            if has_hyp_d
            else no_data + "\n"
        )
        + "\n"
        # --- Hypothesis E ---
        '  <h3 class="obs-subhead">Hypothesis E: How long does the climo_deviation advantage last?</h3>\n'
        '  <p class="learnings-desc">'
        "At +6h, <code>climo_deviation</code> beats persistence by ~1.9&deg;F; "
        "by +24h that gap has shrunk to ~0.5&deg;F. Lines are 10-run rolling means. "
        "<strong>What to look for:</strong> the two lines converging at +24h (gap approaching "
        "zero) while staying well separated at +6h. If they converge at +6h too, the recency "
        "signal has lost value. A seasonal shift (gap changes in summer vs winter) would also "
        "be meaningful."
        "</p>\n"
        + (
            '  <details class="collapsible-section">\n'
            '  <summary>show charts</summary>\n'
            '  <div class="learnings-hyp-grid">'
            '<div class="chart-container"><div id="learnings-hyp-e-6h"></div></div>'
            '<div class="chart-container"><div id="learnings-hyp-e-24h"></div></div>'
            "</div>\n"
            "  </details>\n"
            if has_hyp_e
            else no_data + "\n"
        )
        + "\n"
        # --- Hypothesis F ---
        '  <h3 class="obs-subhead">Hypothesis F: Model specialization map</h3>\n'
        '  <p class="learnings-desc">'
        "Which base model wins each (variable &times; lead) cell? Hover for MAE and sample "
        "size. <strong>What to look for:</strong> does the ensemble weighting actually "
        "reflect this map? If the ensemble underperforms for a variable, check whether "
        "the dominant model here gets high weight in that column."
        "</p>\n"
        + (
            '  <details class="collapsible-section">\n'
            '  <summary>show chart</summary>\n'
            '  <div class="chart-container"><div id="learnings-hyp-f-chart"></div></div>\n'
            '  </details>\n'
            if has_hyp_f
            else no_data + "\n"
        )
        + "\n"
        # --- Hypothesis G ---
        '  <h3 class="obs-subhead">Hypothesis G: Does diurnal_curve ever beat climo_deviation?</h3>\n'
        '  <p class="learnings-desc">'
        "<code>diurnal_curve</code> models the daily temperature cycle explicitly. "
        "<code>climo_deviation</code> wins at every lead right now by anchoring to recent "
        "deviations from climatology. Lines are 10-run rolling means. "
        "<strong>What to look for:</strong> <code>diurnal_curve</code> closing the gap, "
        "especially at overnight leads (+18h/+24h) where solar effects matter less. "
        "If it never closes, the recency signal in <code>climo_deviation</code> is the "
        "explanation &mdash; not the diurnal cycle."
        "</p>\n"
        + (
            '  <details class="collapsible-section">\n'
            '  <summary>show charts</summary>\n'
            '  <div class="learnings-hyp-grid">'
            '<div class="chart-container"><div id="learnings-hyp-g-6h"></div></div>'
            '<div class="chart-container"><div id="learnings-hyp-g-24h"></div></div>'
            "</div>\n"
            "  </details>\n"
            if has_hyp_g
            else no_data + "\n"
        )
        + "</section>\n"
    )


def _learnings_js(data: dict) -> str:
    _font = "'-apple-system, sans-serif'"
    _base_layout = (
        f"margin:{{t:40,b:100,l:50,r:16}},"
        f"xaxis:{{tickangle:0,tickfont:{{size:10}},nticks:5}},"
        f"yaxis:{{rangemode:'tozero',tickfont:{{size:11}}}},"
        f"height:320,showlegend:true,"
        f"legend:{{orientation:'h',x:0,y:-0.18,xanchor:'left',yanchor:'top',font:{{size:10}}}},"
        f"paper_bgcolor:'white',plot_bgcolor:'#fafafa'"
    )

    def _line_trace(name, series, color, dash="solid", width=2, marker_size=5):
        x = json.dumps(series.get("x", []))
        y = json.dumps(series.get("y", []))
        return (
            f"{{type:'scatter',mode:'lines+markers',name:{json.dumps(name)},"
            f"x:{x},y:{y},"
            f"line:{{color:{json.dumps(color)},dash:{json.dumps(dash)},width:{width}}},"
            f"marker:{{size:{marker_size},color:{json.dumps(color)}}}}}"
        )

    def _react(chart_id, traces_js, title, extra_layout=""):
        layout = (
            f"{{title:{{text:{json.dumps(title)},font:{{size:13,family:{_font}}}}},"
            f"{_base_layout}{(',' + extra_layout) if extra_layout else ''}}}"
        )
        cid = json.dumps(chart_id)
        call = f"Plotly.react({cid},{traces_js},{layout},{{responsive:true}});"
        return f"if(document.getElementById({cid})){{{call}}}"

    lines = []

    # --- Hypothesis A: airmass_diurnal members 1 vs 3 ---
    member_labels = {0: "ensemble mean", 1: "m1: clearness-only", 3: "m3: pressure-projected"}
    member_colors = {0: "#9467bd", 1: "#1f77b4", 3: "#ff7f0e"}
    member_dash = {0: "dash", 1: "solid", 3: "dot"}
    for lead in [6, 12]:
        lead_data = data["mae_by_lead"].get(lead, {})
        traces = [
            _line_trace(member_labels[mid], lead_data.get(mid, {}),
                        member_colors[mid], member_dash[mid])
            for mid in [1, 3, 0]
        ]
        lines.append(_react(
            f"learnings-mae-{lead}h",
            "[" + ",".join(traces) + "]",
            f"+{lead}h \u2014 airmass_diurnal temperature MAE (\u00b0F)",
        ))

    # --- Hypothesis B: clearness index vs sky cover (daily aggregates) ---
    clearness_traces = []
    if data["clearness_daily"]:
        k_x = json.dumps([d for d, _ in data["clearness_daily"]])
        k_y = json.dumps([k for _, k in data["clearness_daily"]])
        clearness_traces.append(
            f"{{type:'scatter',mode:'lines+markers',name:'clearness index k (Tempest, daily avg)',"
            f"x:{k_x},y:{k_y},"
            f"line:{{color:'#f6a623',width:2}},"
            f"marker:{{size:5,color:'#f6a623'}},"
            f"yaxis:'y'}}"
        )
    if data["sky_daily"]:
        s_x = json.dumps([d for d, _ in data["sky_daily"]])
        s_y = json.dumps([f for _, f in data["sky_daily"]])
        clearness_traces.append(
            f"{{type:'scatter',mode:'lines+markers',name:'sky cover fraction (NWS, daily avg)',"
            f"x:{s_x},y:{s_y},"
            f"line:{{color:'#4a90d9',width:2,dash:'dot'}},"
            f"marker:{{color:'#4a90d9',size:5}},"
            f"yaxis:'y2'}}"
        )
    if clearness_traces:
        cl_layout = (
            f"{{title:{{text:'Daily avg clearness (Tempest) vs sky cover fraction (NWS KMSP)',"
            f"font:{{size:13,family:{_font}}}}},"
            f"margin:{{t:50,b:110,l:60,r:16}},"
            f"xaxis:{{tickangle:0,tickfont:{{size:10}},nticks:5,title:'date'}},"
            f"yaxis:{{title:'clearness index k (1=clear, 0=overcast)',"
            f"range:[0,1.05],tickfont:{{size:11}}}},"
            f"yaxis2:{{title:'sky cover fraction (1=OVC, 0=CLR)',"
            f"range:[0,1.05],overlaying:'y',side:'right',tickfont:{{size:11}}}},"
            f"height:380,showlegend:true,"
            f"legend:{{orientation:'h',x:0,y:-0.18,xanchor:'left',yanchor:'top',font:{{size:10}}}},"
            f"paper_bgcolor:'white',plot_bgcolor:'#fafafa'}}"
        )
        lines.append(
            f"if(document.getElementById('learnings-clearness-chart'))"
            f"{{Plotly.react('learnings-clearness-chart',"
            f"[{','.join(clearness_traces)}],{cl_layout},{{responsive:true}});}}"
        )

    # --- Hypothesis C: rolling gap (ensemble - climo_deviation) per lead ---
    for lead in [6, 24]:
        key = ("temperature", lead)
        gap = data["hyp_c_gap"].get(key, {})
        if not gap:
            continue
        x_json = json.dumps(gap["x"])
        y_json = json.dumps(gap["y"])
        # zero reference spanning the full x range
        x_ends = json.dumps([gap["x"][0], gap["x"][-1]])
        traces = [
            f"{{type:'scatter',mode:'lines',name:'ensemble \u2212 climo_deviation (10-run mean)',"
            f"x:{x_json},y:{y_json},"
            f"line:{{color:'#d62728',width:2.5}}}}",
            f"{{type:'scatter',mode:'lines',name:'break even',"
            f"x:{x_ends},y:[0,0],"
            f"line:{{color:'#888',dash:'dash',width:1}},showlegend:false}}",
        ]
        lines.append(_react(
            f"learnings-hyp-c-{lead}h",
            "[" + ",".join(traces) + "]",
            f"+{lead}h temperature \u2014 ensemble gap over climo_deviation (\u00b0F)",
            "yaxis:{tickfont:{size:11},zeroline:true,zerolinewidth:2,zerolinecolor:'#bbb',"
            "title:{text:'gap (+ = ensemble worse)',font:{size:11}}}",
        ))

    # --- Hypothesis D: pressure_tendency paradox at 12h ---
    pt_colors = {"dewpoint": "#1f77b4", "pressure": "#d62728"}
    pt_dash_map = {"dewpoint": "solid", "pressure": "dot"}
    pt_traces = []
    for var in ["dewpoint", "pressure"]:
        key = (var, 12)
        series = data["hyp_d"].get(key, {})
        if series.get("x") and series.get("y"):
            unit = "\u00b0F" if var == "dewpoint" else "hPa"
            x = json.dumps(series["x"])
            y = json.dumps(series["y"])
            pt_traces.append(
                f"{{type:'scatter',mode:'lines',name:{json.dumps(f'{var} MAE ({unit})')},"
                f"x:{x},y:{y},"
                f"line:{{color:{json.dumps(pt_colors[var])},dash:{json.dumps(pt_dash_map[var])},width:2}}}}"
            )
    if pt_traces:
        lines.append(_react(
            "learnings-hyp-d-chart",
            "[" + ",".join(pt_traces) + "]",
            "+12h \u2014 pressure_tendency MAE: dewpoint vs pressure (10-run mean)",
            "yaxis:{rangemode:'tozero',tickfont:{size:11},title:{text:'MAE (mixed units)',font:{size:11}}}",
        ))

    # --- Hypothesis E: climo_deviation vs persistence signal decay at 6h and 24h ---
    e_colors = {"climo_deviation": "#2ca02c", "persistence": "#7f7f7f"}
    e_dash = {"climo_deviation": "solid", "persistence": "dot"}
    for lead in [6, 24]:
        lead_data = data["hyp_e"].get(lead, {})
        traces = []
        for m in ["climo_deviation", "persistence"]:
            s = lead_data.get(m, {})
            if s.get("x") and s.get("y"):
                x = json.dumps(s["x"])
                y = json.dumps(s["y"])
                traces.append(
                    f"{{type:'scatter',mode:'lines',name:{json.dumps(m)},"
                    f"x:{x},y:{y},"
                    f"line:{{color:{json.dumps(e_colors[m])},dash:{json.dumps(e_dash[m])},width:2}}}}"
                )
        if traces:
            lines.append(_react(
                f"learnings-hyp-e-{lead}h",
                "[" + ",".join(traces) + "]",
                f"+{lead}h temperature MAE (\u00b0F) \u2014 10-run rolling mean",
                "yaxis:{rangemode:'tozero',tickfont:{size:11}}",
            ))

    # --- Hypothesis F: model specialization heatmap ---
    variables = ["temperature", "dewpoint", "pressure"]
    leads = [6, 12, 18, 24]
    var_labels = {"temperature": "temp", "dewpoint": "dewpt", "pressure": "pressure"}

    # collect unique model names and assign stable colors/indices
    all_best_models = sorted({v["model"] for v in data["hyp_f"].values()})
    model_idx = {m: i for i, m in enumerate(all_best_models)}
    # build z (model index), text (model name + MAE), for heatmap
    z_rows = []
    text_rows = []
    y_labels = [f"+{lt}h" for lt in leads]
    x_labels = [var_labels[v] for v in variables]
    for lt in leads:
        z_row = []
        text_row = []
        for var in variables:
            key = (var, lt)
            best = data["hyp_f"].get(key)
            if best:
                z_row.append(model_idx[best["model"]])
                mae_val = best["avg_mae"]
                if var in ("temperature", "dewpoint"):
                    mae_val = _diff_to_f(mae_val)
                unit = "\u00b0F" if var in ("temperature", "dewpoint") else ("hPa" if var == "pressure" else "m/s")
                text_row.append(f"{best['model']}<br>MAE={mae_val:.2f}{unit}<br>n={best['n']}")
            else:
                z_row.append(None)
                text_row.append("")
        z_rows.append(z_row)
        text_rows.append(text_row)

    if data["hyp_f"]:
        z_json = json.dumps(z_rows)
        text_json = json.dumps(text_rows)
        x_json = json.dumps(x_labels)
        y_json = json.dumps(y_labels)
        colorscale_js = json.dumps(
            [[i / max(len(all_best_models) - 1, 1), c]
             for i, c in enumerate(["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728",
                                     "#9467bd", "#8c564b", "#e377c2", "#bcbd22"][:len(all_best_models)])]
        )
        f_trace = (
            f"{{type:'heatmap',z:{z_json},x:{x_json},y:{y_json},"
            f"text:{text_json},hoverinfo:'text',"
            f"colorscale:{colorscale_js},"
            f"showscale:false,"
            f"zmin:0,zmax:{max(len(all_best_models)-1,1)}}}"
        )
        _model_abbrev = {
            "climo_deviation": "climo_dev",
            "persistence": "persist",
            "pressure_tendency": "p_tendency",
            "diurnal_curve": "diurnal",
            "weighted_climatological_mean": "wtd_climo",
            "climatological_mean": "climo_mean",
            "barogram_ensemble": "ensemble",
            "airmass_diurnal": "airmass",
        }
        # build annotations: model name on line 1, MAE on line 2
        annotations_js = "["
        for ri, lt in enumerate(leads):
            for ci, var in enumerate(variables):
                key = (var, lt)
                best = data["hyp_f"].get(key)
                if best:
                    abbrev = _model_abbrev.get(best["model"], best["model"])
                    mae_val = best["avg_mae"]
                    if var in ("temperature", "dewpoint"):
                        mae_val = _diff_to_f(mae_val)
                    unit = "\u00b0F" if var in ("temperature", "dewpoint") else ("hPa" if var == "pressure" else "m/s")
                    wt = data["ensemble_weights"].get((best["model"], var, lt))
                    wt_str = f"wt:{wt:.0%}" if wt is not None else "not tuned"
                    ann_text = f"{abbrev}<br><b>{mae_val:.2f}{unit}</b> n={best['n']}<br><i>{wt_str}</i>"
                else:
                    ann_text = ""
                annotations_js += (
                    f"{{x:{ci},y:{ri},text:{json.dumps(ann_text)},"
                    f"font:{{size:10,color:'white'}},"
                    f"showarrow:false}},"
                )
        annotations_js += "]"
        f_layout = (
            f"{{title:{{text:'Best model per variable \u00d7 lead (all-time avg MAE)',"
            f"font:{{size:13,family:{_font}}}}},"
            f"margin:{{t:50,b:60,l:80,r:16}},"
            f"xaxis:{{tickfont:{{size:12}}}},"
            f"yaxis:{{tickfont:{{size:12}}}},"
            f"annotations:{annotations_js},"
            f"height:320,paper_bgcolor:'white',plot_bgcolor:'#fafafa'}}"
        )
        lines.append(
            f"if(document.getElementById('learnings-hyp-f-chart'))"
            f"{{Plotly.react('learnings-hyp-f-chart',[{f_trace}],{f_layout},{{responsive:true}});}}"
        )

    # --- Hypothesis G: diurnal_curve vs climo_deviation at 6h and 24h ---
    g_colors = {"diurnal_curve": "#17becf", "climo_deviation": "#2ca02c"}
    g_dash = {"diurnal_curve": "dot", "climo_deviation": "solid"}
    for lead in [6, 24]:
        lead_data = data["hyp_g"].get(lead, {})
        traces = []
        for m in ["climo_deviation", "diurnal_curve"]:
            s = lead_data.get(m, {})
            if s.get("x") and s.get("y"):
                x = json.dumps(s["x"])
                y = json.dumps(s["y"])
                traces.append(
                    f"{{type:'scatter',mode:'lines',name:{json.dumps(m)},"
                    f"x:{x},y:{y},"
                    f"line:{{color:{json.dumps(g_colors[m])},dash:{json.dumps(g_dash[m])},width:2}}}}"
                )
        if traces:
            lines.append(_react(
                f"learnings-hyp-g-{lead}h",
                "[" + ",".join(traces) + "]",
                f"+{lead}h temperature MAE (\u00b0F) \u2014 10-run rolling mean",
                "yaxis:{rangemode:'tozero',tickfont:{size:11}}",
            ))

    return "\n".join(lines)


def _trajectory_data(rows: list) -> dict:
    """Build trajectory data structure for _trajectory_js.

    Returns {
        "valid_at_label": str,
        "variables": {
            var: {
                "observed": float | None,  # display units
                "unit": str,
                "models": {model_name: {"x": [iso_str, ...], "y": [float, ...]}}
            }
        }
    }
    """
    if not rows:
        return {"valid_at_label": "", "variables": {}}

    # representative valid_at for title (use median of all valid_at values)
    all_valid_at = [r["valid_at"] for r in rows]
    target_ts = sorted(all_valid_at)[len(all_valid_at) // 2]
    valid_at_label = fmt.ts(target_ts)

    obs_by_var: dict[str, list[float]] = {}
    by_var_model: dict[str, dict[str, list]] = {}

    for row in rows:
        var = row["variable"]
        model = row["model"]
        issued_at = row["issued_at"]
        value = row["value"]
        observed = row["observed"]

        if observed is not None:
            obs_by_var.setdefault(var, []).append(observed)
        by_var_model.setdefault(var, {}).setdefault(model, []).append((issued_at, value))

    result: dict = {"valid_at_label": valid_at_label, "variables": {}}

    for var in VARIABLES:
        if var not in by_var_model:
            continue
        obs_vals = obs_by_var.get(var, [])
        obs_mean = sum(obs_vals) / len(obs_vals) if obs_vals else None
        if var in ("temperature", "dewpoint"):
            obs_disp = _to_f(obs_mean)
            unit = "\u00b0F"
        else:
            obs_disp = obs_mean
            unit = "hPa"

        models_data: dict = {}
        for model, points in by_var_model[var].items():
            xs, ys = [], []
            for issued_at, val in sorted(points, key=lambda p: p[0]):
                if val is None:
                    continue
                if var in ("temperature", "dewpoint"):
                    val_disp = _to_f(val)
                else:
                    val_disp = val
                dt = datetime.fromtimestamp(issued_at, tz=timezone.utc)
                xs.append(dt.strftime("%Y-%m-%dT%H:%M:%S"))
                ys.append(round(val_disp, 2) if val_disp is not None else None)
            if xs:
                models_data[model] = {"x": xs, "y": ys}

        result["variables"][var] = {
            "observed": round(obs_disp, 2) if obs_disp is not None else None,
            "unit": unit,
            "models": models_data,
        }

    return result


def _trajectory_js(data: dict) -> str:
    if not data.get("variables"):
        return "/* trajectory: no scored data yet */"
    data_json = json.dumps(data)
    return f"""const trajectoryData = {data_json};

const TRAJECTORY_PALETTE = [
    '#1f77b4','#ff7f0e','#2ca02c','#d62728','#9467bd',
    '#8c564b','#e377c2','#7f7f7f','#bcbd22','#17becf'
];
const trajectoryVars = Object.keys(trajectoryData.variables);
const trajectoryAllModels = [...new Set(
    Object.values(trajectoryData.variables).flatMap(function(v) {{
        return Object.keys(v.models);
    }})
)].sort();
const trajectoryModelColors = {{}};
trajectoryAllModels.forEach(function(m, i) {{
    trajectoryModelColors[m] = TRAJECTORY_PALETTE[i % TRAJECTORY_PALETTE.length];
}});
// override well-known models for consistency
if (trajectoryModelColors['barogram_ensemble'] !== undefined) trajectoryModelColors['barogram_ensemble'] = '#1a47b8';
if (trajectoryModelColors['nws'] !== undefined) trajectoryModelColors['nws'] = '#d95f02';
if (trajectoryModelColors['tempest_forecast'] !== undefined) trajectoryModelColors['tempest_forecast'] = '#7570b3';
if (trajectoryModelColors['persistence'] !== undefined) trajectoryModelColors['persistence'] = '#aaaaaa';
if (trajectoryModelColors['bogo'] !== undefined) trajectoryModelColors['bogo'] = '#b0d8b0';

let trajectoryActiveVar = trajectoryVars.includes('temperature') ? 'temperature' : trajectoryVars[0];

function drawTrajectoryChart() {{
    const vd = trajectoryData.variables[trajectoryActiveVar];
    if (!vd) {{ Plotly.react('trajectory-chart', [], {{}}); return; }}
    const unit = vd.unit || '';
    const traces = Object.entries(vd.models).map(function([model, pts]) {{
        const color = trajectoryModelColors[model] || '#888888';
        const isExt = model === 'nws' || model === 'tempest_forecast';
        return {{
            type: 'scatter', mode: 'lines+markers',
            name: model,
            x: pts.x, y: pts.y,
            line: {{ width: isExt ? 2.5 : 1.5, dash: isExt ? 'solid' : 'solid', color: color }},
            marker: {{ size: isExt ? 7 : 5, color: color }},
            connectgaps: false
        }};
    }});
    if (vd.observed !== null && vd.observed !== undefined) {{
        const allX = Object.values(vd.models).flatMap(function(m) {{ return m.x; }}).sort();
        if (allX.length >= 2) {{
            traces.push({{
                type: 'scatter', mode: 'lines',
                name: 'observed',
                x: [allX[0], allX[allX.length - 1]],
                y: [vd.observed, vd.observed],
                line: {{ dash: 'dash', width: 2, color: '#000000' }},
                showlegend: true
            }});
        }}
    }}
    Plotly.react('trajectory-chart', traces, {{
        title: {{ text: 'Forecast trajectory \u2014 valid ' + (trajectoryData.valid_at_label || ''),
                  font: {{ size: 13, family: '-apple-system, sans-serif' }} }},
        margin: {{ t: 40, b: 100, l: 55, r: 16 }},
        xaxis: {{ title: 'Issued at', type: 'date', tickfont: {{ size: 11 }} }},
        yaxis: {{ title: unit, tickfont: {{ size: 11 }} }},
        height: 380,
        showlegend: true,
        legend: {{ orientation: 'h', x: 0, y: -0.18, xanchor: 'left', yanchor: 'top', font: {{ size: 10 }} }},
        paper_bgcolor: 'white',
        plot_bgcolor: '#fafafa'
    }}, {{responsive: true}});
}}

document.querySelectorAll('.trajectory-filter-btn').forEach(function(btn) {{
    btn.addEventListener('click', function() {{
        document.querySelectorAll('.trajectory-filter-btn').forEach(function(b) {{ b.classList.remove('active'); }});
        btn.classList.add('active');
        trajectoryActiveVar = btn.dataset.var;
        drawTrajectoryChart();
    }});
}});

drawTrajectoryChart();
"""


_ACC_VARIABLES = ["temperature", "dewpoint", "pressure", "precip_prob"]


def _skill_score(mae: float | None, climo_mae: float | None) -> float | None:
    """Skill score relative to climatological_mean. 100%=perfect, 0%=matches climo."""
    if mae is None or climo_mae is None or climo_mae == 0:
        return None
    return (1.0 - mae / climo_mae) * 100.0


def _acc_cls(pct: float | None) -> str:
    """Color suffix class for skill score cells. Negatives = worse than climo."""
    if pct is None:
        return ""
    if pct >= 80:
        return " acc-excellent"
    if pct >= 50:
        return " acc-high"
    if pct >= 20:
        return " acc-mid"
    if pct >= 0:
        return " acc-ok"
    if pct >= -50:
        return " acc-low"
    return " acc-poor"


def _accuracy_lead_table_html(rows: list, lead_times: list) -> str:
    """Forecast skill table: rows=models, cols=lead times, filterable by variable."""
    if not rows:
        return '<p class="muted">no scored forecasts</p>'

    # extract climatological_mean MAE as the reference for skill scores
    climo_mae: dict = {}
    for r in rows:
        if r["model"] == "climatological_mean" and r["variable"] in _ACC_VARIABLES:
            climo_mae[(r["variable"], r["lead_hours"])] = r["avg_mae"]

    model_data: dict = {}
    model_meta: dict = {}
    for r in rows:
        name = r["model"]
        if name not in model_data:
            model_data[name] = {v: {} for v in _ACC_VARIABLES}
            model_meta[name] = {"model_id": r["model_id"], "type": r["type"]}
        var = r["variable"]
        if var in _ACC_VARIABLES:
            ref = climo_mae.get((var, r["lead_hours"]))
            model_data[name][var][r["lead_hours"]] = _skill_score(r["avg_mae"], ref)

    def _sort_key(k):
        t = model_meta[k]["type"]
        mid = model_meta[k]["model_id"]
        if t == "ensemble":
            return (0, mid)
        if t == "external":
            return (1, -mid)   # 201 before 200
        return (2, mid)

    model_order = sorted(model_data.keys(), key=_sort_key)
    lts = sorted(lead_times)

    header = "<th>Model</th>" + "".join(f"<th>+{lt}h</th>" for lt in lts)
    body_rows = []
    for name in model_order:
        meta = model_meta[name]
        if name == "climatological_mean":
            badge = '<span class="baseline-badge">baseline</span>'
            row_cls = ' class="baseline-row"'
        elif name == "persistence":
            badge = ""
            row_cls = ' class="baseline-row"'
        elif meta["type"] == "ensemble":
            badge = '<span class="ensemble-badge">ensemble</span>'
            row_cls = ""
        elif meta["type"] == "external":
            badge = '<span class="external-badge">external</span>'
            row_cls = ""
        elif name == "bogo":
            badge = '<span class="fun-badge">fun</span>'
            row_cls = ""
        else:
            badge = ""
            row_cls = ""
        cells = ""
        for lt in lts:
            data_attrs = "".join(
                f' data-{var}="{model_data[name][var].get(lt):.0f}"'
                if model_data[name][var].get(lt) is not None
                else f' data-{var}=""'
                for var in _ACC_VARIABLES
            )
            def_skill = model_data[name]["temperature"].get(lt)
            display = f"{def_skill:.0f}%" if def_skill is not None else "—"
            cls = _acc_cls(def_skill)
            cells += f'<td class="acc-cell{cls}"{data_attrs}>{display}</td>'
        body_rows.append(
            f'<tr{row_cls}><th class="model-name-cell">{name} {badge}</th>{cells}</tr>'
        )

    return (
        f'<table class="obs-history-table acc-lead-table">'
        f'<thead><tr>{header}</tr></thead>'
        f'<tbody>{"".join(body_rows)}</tbody>'
        f'</table>'
    )


def _overall_accuracy_html(rows: list) -> str:
    """Avg forecast skill per model across all variables and lead times."""
    if not rows:
        return '<p class="muted">no scored forecasts</p>'

    climo_mae: dict = {}
    for r in rows:
        if r["model"] == "climatological_mean" and r["variable"] in _ACC_VARIABLES:
            climo_mae[(r["variable"], r["lead_hours"])] = r["avg_mae"]

    model_skills: dict[str, list] = {}
    model_meta: dict = {}
    for r in rows:
        name = r["model"]
        var = r["variable"]
        if var not in _ACC_VARIABLES:
            continue
        ref = climo_mae.get((var, r["lead_hours"]))
        skill = _skill_score(r["avg_mae"], ref)
        if skill is None:
            continue
        if name not in model_skills:
            model_skills[name] = []
            model_meta[name] = {"model_id": r["model_id"], "type": r["type"]}
        model_skills[name].append(skill)

    if not model_skills:
        return '<p class="muted">no scored forecasts</p>'

    def _sort_key(k):
        t = model_meta[k]["type"]
        mid = model_meta[k]["model_id"]
        if t == "ensemble":
            return (0, mid)
        if t == "external":
            return (1, -mid)
        return (2, mid)

    model_order = sorted(model_skills.keys(), key=_sort_key)
    body_rows = []
    for name in model_order:
        avg_skill = sum(model_skills[name]) / len(model_skills[name])
        meta = model_meta[name]
        if name == "climatological_mean":
            badge = '<span class="baseline-badge">baseline</span>'
            row_cls = ' class="baseline-row"'
        elif name == "persistence":
            badge = ""
            row_cls = ' class="baseline-row"'
        elif meta["type"] == "ensemble":
            badge = '<span class="ensemble-badge">ensemble</span>'
            row_cls = ""
        elif meta["type"] == "external":
            badge = '<span class="external-badge">external</span>'
            row_cls = ""
        elif name == "bogo":
            badge = '<span class="fun-badge">fun</span>'
            row_cls = ""
        else:
            badge = ""
            row_cls = ""
        cls = _acc_cls(avg_skill)
        body_rows.append(
            f'<tr{row_cls}>'
            f'<td class="model-name-cell" style="text-align:left;font-weight:500">{name} {badge}</td>'
            f'<td class="acc-cell{cls}" style="font-size:15px;font-weight:600">{avg_skill:.0f}%</td>'
            f'</tr>'
        )

    return (
        f'<table class="obs-history-table acc-overall-table">'
        f'<thead><tr><th>Model</th><th>Forecast Skill</th></tr></thead>'
        f'<tbody>{"".join(body_rows)}</tbody>'
        f'</table>'
    )


def _trend_values(ys: list) -> list | None:
    """Least-squares linear trend over ys (integer x-indices); returns y for every index."""
    pairs = [(i, y) for i, y in enumerate(ys) if y is not None]
    if len(pairs) < 2:
        return None
    n = len(pairs)
    xs = [p[0] for p in pairs]
    yv = [p[1] for p in pairs]
    x_mean = sum(xs) / n
    y_mean = sum(yv) / n
    num = sum((x - x_mean) * (y - y_mean) for x, y in zip(xs, yv))
    den = sum((x - x_mean) ** 2 for x in xs)
    if den == 0:
        return None
    slope = num / den
    intercept = y_mean - slope * x_mean
    return [round(intercept + slope * i, 2) for i in range(len(ys))]


def _skill_timeseries_data(rows: list) -> dict:
    """Pivot per-day avg_skill rows into per-model series.

    Skill is already computed per (variable, lead_hours) and averaged in the DB query,
    so this just reorganises the flat rows into parallel lists for Plotly.
    """
    from collections import defaultdict
    by_day: dict = defaultdict(dict)
    for r in rows:
        by_day[r["day"]][r["model_id"]] = r["avg_skill"]
    days, ensemble, nws, tempest = [], [], [], []
    for day in sorted(by_day):
        days.append(day)
        for out_list, mid in [(ensemble, 100), (nws, 200), (tempest, 201)]:
            skill = by_day[day].get(mid)
            out_list.append(round(skill, 1) if skill is not None else None)
    return {"days": days, "ensemble": ensemble, "nws": nws, "tempest": tempest}


def _skill_timeseries_html(rows_14d: list, rows_120d: list, rows_alltime: list) -> str:
    """Heading + three window-toggled chart containers for the skill-over-time section."""
    windows = [("14d", ""), ("120d", ' style="display:none"'), ("alltime", ' style="display:none"')]
    parts = [
        '<h3 class="obs-subhead">Skill Over Time</h3>',
        '<p class="chart-legend-note">Daily forecast skill vs. climatological mean (0% line). '
        'Averaged across temperature, dew point, and pressure.</p>',
    ]
    for wid, hidden in windows:
        parts.append(
            f'<div id="skill-timeseries-{wid}"{hidden}>'
            f'<div class="chart-container"><div id="skill-timeseries-chart-{wid}"></div></div>'
            f'</div>'
        )
    return "\n".join(parts)


def _skill_timeseries_js(rows_14d: list, rows_120d: list, rows_alltime: list) -> str:
    """Plotly initialization calls for the three skill-over-time charts."""
    windows = [("14d", rows_14d), ("120d", rows_120d), ("alltime", rows_alltime)]
    calls = []
    for wid, rows in windows:
        d = _skill_timeseries_data(rows)
        eid = f"skill-timeseries-chart-{wid}"
        days_j = json.dumps(d["days"])
        ens_j = json.dumps(d["ensemble"])
        nws_j = json.dumps(d["nws"])
        tst_j = json.dumps(d["tempest"])
        trend = _trend_values(d["ensemble"])
        trend_j = json.dumps(trend) if trend else "[]"
        calls.append(
            f'Plotly.react("{eid}",['
            f'{{x:{days_j},y:{ens_j},name:"barogram_ensemble",type:"scatter",'
            f'mode:"lines+markers",connectgaps:false,line:{{color:"#1f77b4"}},'
            f'marker:{{size:4}}}},'
            f'{{x:{days_j},y:{nws_j},name:"nws",type:"scatter",'
            f'mode:"lines+markers",connectgaps:false,line:{{color:"#ff7f0e"}},'
            f'marker:{{size:4}}}},'
            f'{{x:{days_j},y:{tst_j},name:"tempest_forecast",type:"scatter",'
            f'mode:"lines+markers",connectgaps:false,line:{{color:"#2ca02c"}},'
            f'marker:{{size:4}}}},'
            f'{{x:{days_j},y:{trend_j},name:"ensemble trend",type:"scatter",'
            f'mode:"lines",connectgaps:true,line:{{color:"#1f77b4",dash:"dash",width:1.5}},'
            f'showlegend:true}}'
            f'],{{'
            f'height:340,margin:{{t:30,b:100,l:50,r:16}},'
            f'paper_bgcolor:"white",plot_bgcolor:"#fafafa",'
            f'yaxis:{{title:"Skill (%)",zeroline:true,zerolinecolor:"#888",zerolinewidth:2}},'
            f'xaxis:{{type:"date"}},'
            f'legend:{{orientation:"h",x:0,y:-0.18,xanchor:"left",yanchor:"top",font:{{size:10}}}},'
            f'shapes:[{{type:"line",xref:"paper",x0:0,x1:1,y0:0,y1:0,'
            f'line:{{color:"#888",width:2,dash:"dash"}}}}]'
            f'}},{{responsive:true}});'
        )
    return "\n".join(calls)


def _accuracy_table_js() -> str:
    return """\
function updateAccTable(varName) {
    document.querySelectorAll('.acc-lead-table .acc-cell').forEach(function(cell) {
        var raw = cell.getAttribute('data-' + varName);
        if (!raw && raw !== '0') {
            cell.textContent = '\u2014';
            cell.className = 'acc-cell';
        } else {
            var pct = parseFloat(raw);
            cell.textContent = pct.toFixed(0) + '%';
            var suffix = pct >= 80 ? ' acc-excellent' : pct >= 50 ? ' acc-high' : pct >= 20 ? ' acc-mid' : pct >= 0 ? ' acc-ok' : pct >= -50 ? ' acc-low' : ' acc-poor';
            cell.className = 'acc-cell' + suffix;
        }
    });
}

document.querySelectorAll('.acc-filter-btn').forEach(function(btn) {
    btn.addEventListener('click', function() {
        document.querySelectorAll('.acc-filter-btn').forEach(function(b) { b.classList.remove('active'); });
        btn.classList.add('active');
        updateAccTable(btn.dataset.var);
    });
});

function updateAccWindow(win) {
    ['14d', '120d', 'alltime'].forEach(function(w) {
        ['acc-overall-', 'acc-lead-', 'skill-timeseries-'].forEach(function(pfx) {
            var el = document.getElementById(pfx + w);
            if (el) el.style.display = (w === win) ? '' : 'none';
        });
    });
    var activeBtn = document.querySelector('.acc-filter-btn.active');
    if (activeBtn) updateAccTable(activeBtn.dataset.var);
    window.setTimeout(function() {
        var c = document.getElementById('skill-timeseries-chart-' + win);
        if (c) Plotly.Plots.resize(c);
    }, 0);
}

document.querySelectorAll('.acc-window-btn').forEach(function(btn) {
    btn.addEventListener('click', function() {
        document.querySelectorAll('.acc-window-btn').forEach(function(b) { b.classList.remove('active'); });
        btn.classList.add('active');
        updateAccWindow(btn.dataset.window);
    });
});
"""


def _recent_misses_html(rows: list) -> str:
    if not rows:
        return '<p class="muted">No scored forecasts in the last 14 days.</p>'

    # rows are pre-sorted by model then mae desc; emit a group header on model change
    header = (
        '<table class="obs-history-table">'
        '<thead><tr>'
        '<th>Variable</th><th>Lead</th><th>Valid</th>'
        '<th>Predicted</th><th>Observed</th><th>Error</th>'
        '</tr></thead><tbody>'
    )
    body_rows = []
    current_model = None
    for row in rows:
        model = row["model"]
        if model != current_model:
            current_model = model
            body_rows.append(
                f'<tr class="model-header"><th colspan="6">{model}</th></tr>'
            )
        var = row["variable"]
        val = row["value"]
        obs = row["observed"]
        err = row["error"]
        if var in ("temperature", "dewpoint"):
            pred_str = f"{_to_f(val):.1f}\u00b0F" if val is not None else "\u2014"
            obs_str = f"{_to_f(obs):.1f}\u00b0F" if obs is not None else "\u2014"
            err_disp = _diff_to_f(err)
        else:
            pred_str = f"{val:.1f} hPa" if val is not None else "\u2014"
            obs_str = f"{obs:.1f} hPa" if obs is not None else "\u2014"
            err_disp = err
        if err_disp is not None:
            sign = "+" if err_disp >= 0 else ""
            err_cls = "mae-worse" if abs(err_disp) >= 3 else ""
            err_str = f'<span class="{err_cls}">{sign}{err_disp:.1f}</span>'
        else:
            err_str = "\u2014"
        valid_label = fmt.short_ts(row["valid_at"])
        body_rows.append(
            f'<tr>'
            f'<td>{_VARIABLE_LABEL.get(var, var)}</td>'
            f'<td>+{row["lead_hours"]}h</td>'
            f'<td>{valid_label}</td>'
            f'<td style="text-align:right">{pred_str}</td>'
            f'<td style="text-align:right">{obs_str}</td>'
            f'<td style="text-align:right">{err_str}</td>'
            f'</tr>'
        )
    return header + "".join(body_rows) + "</tbody></table>"


def _write_fragment(html: str, out_dir: Path) -> None:
    css_start = html.index("<style>\n") + len("<style>\n")
    css_end = html.index("\n</style>")
    css = html[css_start:css_end]

    # scope bare-element selectors so they don't bleed into the host site
    css = re.sub(r"(?m)^body \{", ".barogram {", css)
    css = re.sub(r"(?m)^header h1 \{", ".barogram-header h1 {", css)
    css = re.sub(r"(?m)^header \{", ".barogram-header {", css)
    css = re.sub(r"(?m)^h2 \{", ".barogram h2 {", css)
    css = re.sub(r"(?m)^h3 \{", ".barogram h3 {", css)
    # disable sticky header when embedded — site nav already handles that
    css += "\n.barogram-header { position: relative; top: auto; z-index: auto; }\n"

    body_start = html.index("<body>\n") + len("<body>\n")
    script_anchor = '\n<script src="https://cdn.jsdelivr.net/'
    body_end = html.index(script_anchor)
    body_html = html[body_start:body_end]
    body_html = body_html.replace("<header>", '<div class="barogram-header">', 1)
    body_html = body_html.replace("</header>", "</div>", 1)
    body_html = f'<div class="barogram">\n{body_html}\n</div>'

    scripts_start = html.index(script_anchor) + 1
    scripts_end = html.index("\n</body>")
    scripts_html = html[scripts_start:scripts_end]

    (out_dir / "barogram-style.css").write_text(css, encoding="utf-8")
    (out_dir / "barogram-body.html").write_text(body_html, encoding="utf-8")
    (out_dir / "barogram-scripts.html").write_text(scripts_html, encoding="utf-8")


def generate(
    conn_in: sqlite3.Connection,
    conn_out: sqlite3.Connection,
    output_path: Path,
) -> None:
    db.sync_ensemble_members(conn_out)
    elevation_m = db.tempest_station_elevation(conn_in)
    all_rows = db.latest_forecast_per_model(conn_out)
    if not all_rows:
        raise ValueError(
            "no forecasts in output database \u2014 run barogram.py forecast first"
        )

    # for multi-member models, use only member_id=0 (ensemble mean) in all displays;
    # for single-member models, member_id=0 is already their only member
    mean_rows = [r for r in all_rows if r["member_id"] == 0]
    member_forecast_rows = [r for r in all_rows if r["member_id"] > 0]

    # count named members per model for the member toggle button
    model_member_ids: dict = {}
    for row in all_rows:
        if row["member_id"] > 0:
            model_member_ids.setdefault(row["model_id"], set()).add(row["member_id"])
    # exclude barogram_ensemble (100) — its members are the base models already shown above
    member_counts = {mid: len(mids) for mid, mids in model_member_ids.items() if mid != 100}

    tempest = db.latest_tempest_obs(conn_in)
    nws = db.latest_nws_obs(conn_in)
    tempest_history = db.recent_tempest_obs(conn_in)
    nws_history = db.recent_nws_obs(conn_in)

    loc = db.tempest_station_location(conn_in)
    nws_forecast = _fetch_nws_forecast(*loc) if loc else {}

    now = int(time.time())
    today = date.today()
    midnight_7d_ago = int(
        datetime(today.year, today.month, today.day, tzinfo=timezone.utc).timestamp()
    ) - 7 * 86400
    _scores_multi = db.score_summary_last_n_runs_multi(conn_out, [30, 10])
    all_scores_30 = _scores_multi[30]
    all_scores_10 = _scores_multi[10]
    summary_30 = [r for r in all_scores_30 if r["member_id"] == 0]
    summary_10 = [r for r in all_scores_10 if r["member_id"] == 0]
    members_10 = [r for r in all_scores_10 if r["member_id"] > 0]
    member_models = {r["model"] for r in members_10}
    summary_7d = db.score_summary_since(conn_out, now - 7 * 86400)
    timeseries = db.score_timeseries(conn_out, since=midnight_7d_ago)
    all_time_summary = [r for r in db.score_summary(conn_out) if r["member_id"] == 0]
    bias_ts_rows = db.bias_timeseries(conn_out, since=midnight_7d_ago)
    diurnal_rows = db.diurnal_errors(conn_out)
    error_dist_rows = db.error_distribution(conn_out)
    weight_rows = db.all_weights_with_members(conn_out)
    all_members = db.all_members_for_ensemble_models(conn_out)
    trajectory_rows = db.forecast_trajectory(conn_out, now - 72 * 3600)
    misses_rows = db.recent_misses(conn_out, now - 14 * 86400)
    _14d = now - 14 * 86400
    _120d = now - 120 * 86400
    _acc = db.accuracy_windows(conn_out, [_14d, _120d, 0])
    acc_rows_14d = _acc[_14d]
    acc_rows_120d = _acc[_120d]
    acc_rows_alltime = _acc[0]
    _skill_ts = db.skill_timeseries_multi(conn_out, [_14d, _120d, 0])
    skill_ts_html = _skill_timeseries_html(_skill_ts[_14d], _skill_ts[_120d], _skill_ts[0])
    skill_ts_js = _skill_timeseries_js(_skill_ts[_14d], _skill_ts[_120d], _skill_ts[0])
    _counts = db.accuracy_run_count_multi(conn_out, [_14d, _120d, 0])
    acc_count_14d = _counts[_14d]
    acc_count_120d = _counts[_120d]
    acc_count_alltime = _counts[0]

    lead_times = sorted({row["lead_hours"] for row in mean_rows})
    charts = _chart_data(mean_rows)
    mae_ts = _mae_timeseries_data(timeseries)
    bias_ts = _bias_timeseries_data(bias_ts_rows)
    lead_skill = _lead_skill_data(all_time_summary)
    heatmap = _heatmap_data(all_time_summary)
    diurnal = _diurnal_data(diurnal_rows)
    error_dist = _error_dist_data(error_dist_rows)
    trajectory = _trajectory_data(trajectory_rows)
    recent_misses_html = _recent_misses_html(misses_rows)
    acc_lead_times = sorted({r["lead_hours"] for r in acc_rows_14d}) or lead_times
    _acc_windows = [
        ("14d", acc_rows_14d, acc_count_14d, "14 days"),
        ("120d", acc_rows_120d, acc_count_120d, "120 days"),
        ("alltime", acc_rows_alltime, acc_count_alltime, "all time"),
    ]
    overall_parts, lead_parts = [], []
    for wid, rows, n_runs, label in _acc_windows:
        hidden = ' style="display:none"' if wid != "14d" else ""
        run_note = f'<p class="chart-legend-note acc-run-note">{label} \u00b7 {n_runs} runs</p>'
        overall_parts.append(
            f'<div id="acc-overall-{wid}"{hidden}>'
            f'{run_note}'
            f'{_overall_accuracy_html(rows)}'
            f'</div>'
        )
        lead_parts.append(
            f'<div id="acc-lead-{wid}"{hidden}>'
            f'{run_note}'
            f'{_accuracy_lead_table_html(rows, acc_lead_times)}'
            f'</div>'
        )
    overall_accuracy_html = "".join(overall_parts)
    acc_lead_table_html = "".join(lead_parts)
    generated_at = fmt.ts(now)
    _lf = db.get_metadata(conn_out, "last_forecast")
    _lt = db.get_metadata(conn_out, "last_tune")
    last_forecast_str = fmt.ts(int(_lf)) if _lf else "\u2014"
    last_tune_str = fmt.ts(int(_lt)) if _lt else "\u2014"

    # staleness check: models whose last issued_at is >2h behind last_forecast
    stale_models: list[str] = []
    if _lf:
        lf_ts = int(_lf)
        model_last_rows = conn_out.execute(
            "select model, max(issued_at) as last_run from forecasts group by model"
        ).fetchall()
        for r in model_last_rows:
            if r["last_run"] is not None and lf_ts - r["last_run"] > 7200:
                stale_models.append(r["model"])
    staleness_banner = ""
    if stale_models:
        model_list = ", ".join(f"<code>{m}</code>" for m in sorted(stale_models))
        staleness_banner = (
            f'<div class="stale-banner">'
            f'<strong>Warning:</strong> the following models did not run in the last forecast cycle '
            f'and may have crashed: {model_list}. '
            f'Check stderr output from <code>barogram forecast</code> for details.'
            f'</div>'
        )

    learnings = _learnings_data(conn_in, conn_out)
    learnings_section = _learnings_section_html(learnings)

    ap_state = _ap_signal_state(conn_in, tempest)
    ap_signal_html = _ap_signal_state_html(ap_state, member_forecast_rows)

    zambretti = pressure_tendency.zambretti_text(tempest, conn_in, elevation_m) if tempest else None
    zambretti_panel = _zambretti_panel_html(zambretti)
    tempest_card = _conditions_card("Tempest", tempest, elevation_m)
    nws_card = _conditions_card("NWS", nws)
    slp_offset = _slp_correction(tempest, elevation_m)
    ensemble_section = _ensemble_forecast_section(mean_rows, tempest, elevation_m, nws_forecast)
    model_runs = _model_runs_html(mean_rows, lead_times, member_counts, member_forecast_rows, slp_offset)
    obs_section = _obs_history_section(tempest_history, nws_history, elevation_m)
    tempest_rows = [_tempest_obs_row(r, elevation_m) for r in tempest_history]
    nws_rows = [_nws_obs_row(r) for r in nws_history]

    all_models = {r["model"]: {"model_id": r["model_id"], "type": r["type"]} for r in mean_rows}
    table_30 = _score_summary_table(summary_30, "last 30 scored runs", member_models, all_models)
    table_10 = _score_summary_table(summary_10, "last 10 scored runs", member_models, all_models)
    table_7d = _score_summary_table(summary_7d, "last 7 days", member_models, all_models)
    weights_section = _weights_section_html(weight_rows, all_members)
    filter_btns = "".join(
        f'<button class="mae-filter-btn{" active" if i == 0 else ""}" data-var="{v}">{lbl}</button>'
        for i, (v, lbl) in enumerate([
            ("avg", "Average"), ("temperature", "Temperature"),
            ("dewpoint", "Dew Point"), ("pressure", "Pressure"),
        ])
    )
    mae_chart_divs = "".join(
        f'<div class="chart-container"><div id="mae-chart-{lt}"></div></div>'
        for lt in lead_times
    )

    fcst_filter_btns = "".join(
        f'<button class="fcst-filter-btn{" active" if i == 0 else ""}" data-var="{v}">{lbl}</button>'
        for i, (v, lbl) in enumerate([
            ("temperature", "Temperature"), ("dewpoint", "Dew Point"),
            ("pressure", "Pressure"),
        ])
    )
    acc_filter_btns = "".join(
        f'<button class="acc-filter-btn{" active" if i == 0 else ""}" data-var="{v}">{lbl}</button>'
        for i, (v, lbl) in enumerate([
            ("temperature", "Temperature"), ("dewpoint", "Dew Point"),
            ("pressure", "Pressure"),
        ])
    )
    acc_window_btns = "".join(
        f'<button class="acc-window-btn{" active" if i == 0 else ""}" data-window="{wid}">{lbl}</button>'
        for i, (wid, _, _, lbl) in enumerate(_acc_windows)
    )

    _var_btns = [
        ("temperature", "Temperature"), ("dewpoint", "Dew Point"),
        ("pressure", "Pressure"),
    ]
    bias_filter_btns = "".join(
        f'<button class="bias-filter-btn{" active" if i == 0 else ""}" data-var="{v}">{lbl}</button>'
        for i, (v, lbl) in enumerate(_var_btns)
    )
    bias_chart_divs = "".join(
        f'<div class="chart-container"><div id="bias-chart-{lt}"></div></div>'
        for lt in lead_times
    )
    lead_skill_filter_btns = "".join(
        f'<button class="lead-skill-filter-btn{" active" if i == 0 else ""}" data-var="{v}">{lbl}</button>'
        for i, (v, lbl) in enumerate(_var_btns)
    )
    heatmap_filter_btns = "".join(
        f'<button class="heatmap-filter-btn{" active" if i == 0 else ""}" data-var="{v}">{lbl}</button>'
        for i, (v, lbl) in enumerate(_var_btns)
    )
    diurnal_filter_btns = "".join(
        f'<button class="diurnal-filter-btn{" active" if i == 0 else ""}" data-var="{v}">{lbl}</button>'
        for i, (v, lbl) in enumerate(_var_btns)
    )
    error_dist_var_btns = "".join(
        f'<button class="error-dist-var-btn{" active" if i == 0 else ""}" data-var="{v}">{lbl}</button>'
        for i, (v, lbl) in enumerate(_var_btns)
    )
    error_dist_lead_btns = "".join(
        f'<button class="error-dist-lead-btn{" active" if i == 0 else ""}" data-lead="{lt}">+{lt}h</button>'
        for i, lt in enumerate(lead_times)
    )
    _traj_vars = [("temperature", "Temperature"), ("dewpoint", "Dew Point"),
                  ("pressure", "Pressure")]
    trajectory_filter_btns = "".join(
        f'<button class="trajectory-filter-btn{" active" if i == 0 else ""}" data-var="{v}">{lbl}</button>'
        for i, (v, lbl) in enumerate(_traj_vars)
    )

    html = f"""\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>barogram</title>
<style>
{_CSS}
</style>
</head>
<body>
<div class="container">

<header>
  <div class="header-top">
    <h1>barogram</h1>
    <div class="generated">
      <span>generated {generated_at}</span>
      <span>last forecast: {last_forecast_str}</span>
      <span>last tune: {last_tune_str}</span>
    </div>
  </div>
  <nav class="jump-nav">
    <a href="#conditions">Conditions</a>
    <a href="#forecast">Forecast</a>
    <a href="#verification">Verification</a>
    <a href="#analysis">Analysis</a>
    <a href="#weights">Weights</a>
    <a href="#learnings">Learnings</a>
    <a href="#latest-run">Latest Run</a>
  </nav>
</header>
{staleness_banner}
<section class="section" id="about">
  <p>Barogram is a pet forecast ensemble, a small collection of models I run for fun and to learn more about how forecasting actually works. Every three hours, they look at the latest readings from a backyard Tempest weather station and a nearby NWS airport station, then each independently predict local temperature, dew point, pressure, and precipitation probability for the next 6 to 24 hours.</p>
  <p style="margin-top:10px">After each run, the previous predictions get scored against what actually happened. Models that have been performing better lately carry more weight in the ensemble&#x2019;s combined output. The base models use simple approaches and none of them are impressive on their own. The ensemble is what makes them useful.</p>
</section>
<section class="section" id="conditions">
  <h2>Latest Conditions</h2>
  <div class="conditions-grid">
    {tempest_card}
    {nws_card}
  </div>
  {zambretti_panel}
</section>

{ensemble_section}

<section class="section" id="verification">
  <h2>Verification</h2>
  <div class="mae-filter-bar">{acc_window_btns}</div>
  <h3 class="obs-subhead">Overall Forecast Skill</h3>
  <p class="chart-legend-note">Skill score vs. climatological mean, averaged across all variables and lead times. 100% = perfect · 0% = matches climatological mean · negative = worse than climatological mean.</p>
  <div class="table-scroll">{overall_accuracy_html}</div>
  {skill_ts_html}
  <details class="collapsible-section">
    <summary class="obs-subhead">Recent Misses (14 days)</summary>
    <p class="chart-legend-note">Largest forecast errors per source over the last 14 days, sorted biggest miss first within each group.</p>
    <div class="table-scroll">{recent_misses_html}</div>
  </details>
  <h3 class="obs-subhead">Forecast Skill by Lead Time</h3>
  <p class="chart-legend-note">Skill score vs. climatological mean at each lead time for the selected variable. Negative = worse than climatology.</p>
  <div class="mae-filter-bar">{acc_filter_btns}</div>
  <div class="table-scroll">{acc_lead_table_html}</div>
  <h3 class="obs-subhead section-dig-deeper">Detailed MAE</h3>
  <div class="verification-primary">
    {table_30}
  </div>
  <div class="verification-windows">
    {table_10}
    {table_7d}
  </div>
  <h3 class="obs-subhead">MAE over time</h3>
  <div class="mae-filter-bar">{filter_btns}<button id="smooth-toggle" class="mae-raw-btn">Per-run detail</button><button id="raw-toggle" class="mae-raw-btn">Raw values</button></div>
  <p class="chart-legend-note">Grey: reference lines (climo = long-dash, persistence = dotted) &nbsp;·&nbsp; Per-run detail: solid with dash-dot rolling avg overlay</p>
  <div class="mae-charts-grid">
    {mae_chart_divs}
  </div>
</section>

<section class="section analysis-section" id="analysis">
  <h2>Model Analysis</h2>

  <h3 class="obs-subhead">Bias Over Time</h3>
  <div class="mae-filter-bar">{bias_filter_btns}</div>
  <div class="mae-charts-grid">
    {bias_chart_divs}
  </div>

  <h3 class="obs-subhead">Lead-Time Skill Curves</h3>
  <div class="mae-filter-bar">{lead_skill_filter_btns}</div>
  <div class="chart-container"><div id="lead-skill-chart"></div></div>

  <h3 class="obs-subhead">Score Heatmap</h3>
  <div class="mae-filter-bar">{heatmap_filter_btns}</div>
  <div class="chart-container"><div id="heatmap-chart"></div></div>

  <h3 class="obs-subhead">Forecast Trajectory</h3>
  <p class="chart-legend-note">How each source's prediction for the most recently scored valid time evolved. Dashed black line = observed.</p>
  <div class="mae-filter-bar">{trajectory_filter_btns}</div>
  <div class="chart-container"><div id="trajectory-chart"></div></div>

  <h3 class="obs-subhead">Diurnal Stratification</h3>
  <div class="mae-filter-bar">
    {diurnal_filter_btns}
    <button id="diurnal-mode-btn" class="mae-raw-btn">Show MAE</button>
  </div>
  <div class="chart-container"><div id="diurnal-chart"></div></div>

  <h3 class="obs-subhead">Error Distribution</h3>
  <div class="mae-filter-bar"><span class="filter-label">Variable</span>{error_dist_var_btns}<span class="filter-label filter-sep-left">Lead</span>{error_dist_lead_btns}</div>
  <div class="chart-container"><div id="error-dist-chart"></div></div>

  <h3 class="obs-subhead">airmass_precip &mdash; Signal State</h3>
  {ap_signal_html}
</section>

<section class="section" id="weights">
  <h2>Weights</h2>
  <p class="learnings-intro">Inverse-MAE member weights computed by <code>barogram tune</code>. Higher weight means the tuner is trusting that member more based on recent scoring history. Sector columns show how trust shifts across time-of-day.</p>
  {weights_section}
</section>

{learnings_section}

<section class="section" id="latest-run">
  <h2>Latest Forecast Run</h2>
  <div class="mae-filter-bar">{fcst_filter_btns}</div>
  <div class="chart-container"><div id="chart-forecast"></div></div>
  <div class="model-runs" style="margin-top:16px">
    {model_runs}
  </div>
</section>

{obs_section}

</div>
<script src="https://cdn.jsdelivr.net/npm/plotly.js-dist-min@2/plotly.min.js"></script>
<script>
{_chart_js(charts)}
{_obs_history_js(tempest_rows, nws_rows)}
{_mae_timeseries_js(mae_ts)}
{_member_forecast_js(member_forecast_rows, lead_times)}
{_member_detail_js(members_10)}
{_bias_timeseries_js(bias_ts)}
{_lead_skill_js(lead_skill)}
{_heatmap_js(heatmap)}
{_trajectory_js(trajectory)}
{_diurnal_js(diurnal)}
{_error_dist_js(error_dist)}
{_learnings_js(learnings)}
{_accuracy_table_js()}
{skill_ts_js}
</script>
</body>
</html>
"""

    output_path.write_text(html, encoding="utf-8")
    _write_fragment(html, output_path.parent)
