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

VARIABLES = ["temperature", "dewpoint", "pressure"]

_MODEL_TOOLTIPS: dict[str, str] = {
    "persistence": "Current observed value held constant across all lead times. The null hypothesis: any useful model has to beat this.",
    "climatological_mean": "Historical average for this month and hour from the local Tempest archive. Ignores current conditions entirely.",
    "weighted_climatological_mean": "Like climatological_mean, but recent observations carry more weight. Multiple members test different recency weighting strategies.",
    "climo_deviation": "Adds the current anomaly (how today differs from climatology) to the future baseline, with multiple decay rates for how fast the anomaly fades.",
    "pressure_tendency": "Extrapolates from the recent pressure time series using polynomial regression and a categorical Zambretti classifier.",
    "diurnal_curve": "Fits a daily temperature/dewpoint cycle to recent observations and projects it forward using sine, piecewise, and asymmetric cosine curves.",
    "airmass_diurnal": "Scales the diurnal curve by solar clearness index and other Tempest signals: wind sector, dewpoint depression, pressure departure, cloud character.",
    "analog": "Finds historical days most similar to current conditions and uses their subsequent weather as the forecast. Improves as the local archive grows.",
    "surface_signs": "Reads physical cues (wind rotation, moisture trend, solar cover, convective activity) and applies historically learned conditional deltas for each signal independently.",
    "synoptic_state_machine": "Classifies current conditions as a joint state from four signals (wind rotation, moisture trend, solar cover, convective activity), so signal interactions, not just each signal in isolation, shape the learned deltas.",
    "bogo": "A collection of deliberately wrong forecasting strategies. Scored for entertainment; expected to perform poorly.",
    "barogram_ensemble": "Weighted average of all base models, with weights set by Huber skill scores per variable, lead, and time-of-day sector.",
    "nws": "NWS hourly forecast from api.weather.gov, snapped to the standard 6/12/18/24h lead times. Not included in the barogram ensemble.",
    "tempest_forecast": "Tempest station's built-in forecast from the Tempest API, snapped to the standard lead times. Not included in the barogram ensemble.",
    "external_corrected": "NWS and Tempest forecasts with bias corrections learned from historical scoring, conditioned on time of day, season, and airmass state. Not included in the barogram ensemble.",
    "wind_veer_detector": "Classifies wind-direction change over a trailing 3h window with no (or a much lower) minimum wind-speed floor than surface_signs uses, to test whether light-wind veers are still informative.",
    "frontal_trigger": "Joint pressure-tendency + wind-veer trigger, combined with strict/loose/weighted variants, kept separate from synoptic_state_machine since wind rotation hurts that model as a joint dimension.",
    "dewpoint_tendency": "Extrapolates dewpoint's own recent linear trend forward with OU mean reversion, mirroring pressure_tendency's regression members but for moisture instead of pressure.",
}

_VARIABLE_LABEL = {
    "temperature": "Temperature",
    "dewpoint": "Dew Point",
    "pressure": "Pressure",
}

_UNIT = {
    "temperature": "\u00b0F",
    "dewpoint": "\u00b0F",
    "pressure": "hPa",
}

_FMT = {
    "temperature": ".1f",
    "dewpoint": ".1f",
    "pressure": ".1f",
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
            result[ts] = {"temperature": temp_c, "dewpoint": dew_c}
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
    display: grid;
    grid-template-columns: 1fr auto;
    grid-template-areas: "title generated" "nav generated";
    row-gap: 6px;
    column-gap: 24px;
}
header h1 { grid-area: title; font-size: 22px; letter-spacing: -0.5px; }
.header-top .generated { grid-area: generated; align-self: start; }
.generated { font-size: 12px; color: #666; display: flex; flex-direction: column; align-items: flex-end; gap: 2px; }
@media (max-width: 700px) {
    .header-top {
        grid-template-columns: auto 1fr;
        grid-template-areas: "title generated" "nav nav";
    }
}
@media (max-width: 450px) {
    .header-top {
        grid-template-columns: 1fr;
        grid-template-areas: "title" "generated" "nav";
    }
}
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
.stale-age-banner { background: #fff0e0; border-color: #e07000; border-left-color: #c05000; }
.stale-age-banner code { background: #ffd8a8; }
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
.obs-fallback { font-size: 0.8em; color: #999; margin-left: 3px; }
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
.mae-filter-btn,
.bias-filter-btn, .lead-skill-filter-btn, .heatmap-filter-btn,
.diurnal-filter-btn, .error-dist-var-btn, .error-dist-lead-btn,
.acc-filter-btn, .acc-window-btn { padding: 4px 12px; font-size: 12px; font-family: inherit; background: #fff; border: 1px solid #ccc; border-radius: 3px; cursor: pointer; color: #444; }
.mae-filter-btn:hover,
.bias-filter-btn:hover, .lead-skill-filter-btn:hover, .heatmap-filter-btn:hover,
.diurnal-filter-btn:hover, .error-dist-var-btn:hover, .error-dist-lead-btn:hover,
.acc-filter-btn:hover, .acc-window-btn:hover { background: #f0f0f0; }
.mae-filter-btn.active,
.bias-filter-btn.active, .lead-skill-filter-btn.active, .heatmap-filter-btn.active,
.diurnal-filter-btn.active, .error-dist-var-btn.active, .error-dist-lead-btn.active,
.acc-filter-btn.active, .acc-window-btn.active { background: #1a1a1a; color: #fff; border-color: #1a1a1a; }
.mae-raw-btn { margin-left: auto; padding: 4px 12px; font-size: 12px; font-family: inherit; background: #fff; border: 1px solid #ccc; border-radius: 3px; cursor: pointer; color: #666; }
.mae-raw-btn:hover { background: #f0f0f0; }
.mae-raw-btn.active { background: #555; color: #fff; border-color: #555; }
.run-browser-nav { display: flex; justify-content: center; align-items: center; gap: 4px; margin-bottom: 10px; }
.run-browser-nav-btn { padding: 4px 10px; font-size: 12px; font-family: inherit; background: #fff; border: 1px solid #ccc; border-radius: 3px; cursor: pointer; color: #666; }
.run-browser-nav-btn:hover { background: #f0f0f0; }
#run-browser-select { padding: 4px 8px; font-size: 12px; font-family: inherit; background: #fff; border: 1px solid #ccc; border-radius: 3px; color: #444; }
.run-browser-var-toggle { display: flex; justify-content: center; gap: 18px; margin-bottom: 10px; font-size: 12px; color: #444; }
.run-browser-checkboxes { display: grid; grid-template-columns: repeat(auto-fill, minmax(190px, 1fr)); gap: 4px 10px; margin-bottom: 10px; font-size: 12px; color: #444; }
.run-browser-model-actions { display: flex; justify-content: center; gap: 8px; margin-bottom: 8px; }
.run-browser-select-btn { padding: 3px 9px; font-size: 11px; font-family: inherit; background: #fff; border: 1px solid #ccc; border-radius: 3px; cursor: pointer; color: #666; }
.run-browser-select-btn:hover { background: #f0f0f0; }
.run-browser-checkbox { display: flex; align-items: center; gap: 4px; cursor: pointer; white-space: nowrap; }
.run-browser-mid { display: inline-block; width: 2em; text-align: right; margin-right: 4px; color: #888; font-variant-numeric: tabular-nums; }
.run-browser-swatch { display: inline-block; width: 10px; height: 10px; border-radius: 2px; margin: 0 4px; flex-shrink: 0; border: 1px solid rgba(128,128,128,.4); }
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
.acc-cell.acc-best { background: #d7ecd7; }
.acc-cell.acc-worst { background: #f0d8d8; }
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
.table-scroll { overflow-x: auto; margin-bottom: 8px; max-width: 100%; }
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
    white-space: nowrap;
}
.member-btn:hover { background: #f0f0f0; }
.member-detail-row td { padding: 6px 10px; }
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
.wt101-checkbox { display: none; }
.wt101-toggle-bar { margin: 4px 0 8px; }
.wt101-toggle-bar label { display: inline-block; }
.wt101-view-without { display: none; }
.wt101-checkbox:checked ~ .wt101-view-with { display: none; }
.wt101-checkbox:checked ~ .wt101-view-without { display: block; }
.wt101-checkbox:checked ~ .wt101-toggle-bar label { background: #555; color: #fff; border-color: #555; }
.weight-group-hdr th { background: #f5f5f5; font-size: 11px; color: #888; font-weight: 600;
    letter-spacing: 0.04em; text-transform: uppercase; padding: 3px 10px; }
.learnings-intro { margin-bottom: 14px; color: #555; font-size: 13px; line-height: 1.6; }
.no-data { color: #888; font-style: italic; font-size: 13px; margin: 8px 0 16px; }
.filter-label { font-size: 11px; color: #888; white-space: nowrap; align-self: center; }
.filter-sep-left { margin-left: 8px; }
.ap-signal-cards { display: none; }
@media (max-width: 768px) {
    body { padding: 12px 10px; }
    .generated { font-size: 11px; }
    .conditions-grid, .charts-grid { grid-template-columns: 1fr; }
    .verification-windows { grid-template-columns: 1fr; }
    .weights-section { grid-template-columns: 1fr; }
    .conditions-grid > *, .charts-grid > *, .verification-windows > *,
    .weights-section > * { min-width: 0; }
    .section { scroll-margin-top: 70px; }
    .fcst-row-refs { gap: 12px; }
    .mae-raw-btn { margin-left: 0; }
    .jump-nav a { font-size: 11px; padding: 3px 8px; }
    table { font-size: 11px; width: 100%; }
    table th, table td { padding: 4px 5px; }
    .obs-history-table { min-width: unset; width: 100%; }
    .obs-history-table th, .obs-history-table td { overflow-wrap: break-word; white-space: normal; }
    .mae-summary-table, .score-table, .weight-table,
    .member-detail-table, .acc-lead-table { width: 100%; }
    .mae-summary-table th, .mae-summary-table td,
    .score-table th, .score-table td,
    .weight-table th, .weight-table td,
    .member-detail-table th, .member-detail-table td,
    .acc-lead-table th, .acc-lead-table td { overflow-wrap: break-word; white-space: normal; }
    .mae-summary-table tbody th,
    .acc-lead-table th.model-name-cell,
    .weight-table tbody th,
    .score-table tbody th { word-break: break-word; }
    .mae-summary-table .model-id-cell,
    .acc-lead-table .model-id-cell,
    .score-table .model-id-cell { white-space: nowrap; overflow-wrap: normal; }
    .member-btn { white-space: nowrap; }
    .recent-misses-table { table-layout: fixed; width: 100%; }
    .recent-misses-table th, .recent-misses-table td { word-break: break-word; white-space: normal; }
    .tempest-obs thead, .nws-obs thead { display: none; }
    .tempest-obs tbody, .nws-obs tbody { display: block; width: 100%; }
    .tempest-obs tr, .nws-obs tr {
        display: grid;
        grid-template-columns: 1fr 1fr;
        border: 1px solid #e0e0e0;
        border-radius: 4px;
        padding: 8px 10px;
        margin-bottom: 6px;
        gap: 5px 12px;
        background: #fff;
    }
    .tempest-obs td, .nws-obs td {
        display: block;
        padding: 0;
        border: none;
        font-size: 12px;
        line-height: 1.4;
    }
    .tempest-obs td::before, .nws-obs td::before {
        content: attr(data-label);
        display: block;
        font-size: 9px;
        color: #999;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        font-weight: 600;
        margin-bottom: 1px;
    }
    .tempest-obs td:first-child, .nws-obs td:first-child {
        grid-column: 1 / -1;
        font-size: 11px;
        font-weight: 600;
        color: #555;
        padding-bottom: 3px;
        border-bottom: 1px solid #f0f0f0;
        margin-bottom: 2px;
    }
    .tempest-obs td:first-child::before, .nws-obs td:first-child::before { display: none; }
    .ap-signal-container .table-scroll { display: none; }
    .ap-signal-cards { display: flex; flex-direction: column; gap: 4px; }
    .ap-signal-card {
        border: 1px solid #e0e0e0;
        border-radius: 4px;
        padding: 7px 10px;
        background: #fff;
    }
    .ap-card-info {
        display: flex;
        align-items: center;
        gap: 6px;
        min-width: 0;
    }
    .ap-card-num { font-size: 10px; color: #bbb; flex-shrink: 0; width: 18px; }
    .ap-card-name {
        font-weight: 600;
        font-size: 12px;
        flex: 1;
        min-width: 0;
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
    }
    .ap-card-signal {
        font-size: 10px;
        color: #999;
        flex-shrink: 0;
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
        max-width: 95px;
    }
    .ap-card-state { flex-shrink: 0; }
    .ap-card-leads {
        display: flex;
        justify-content: space-between;
        margin-top: 5px;
        padding-top: 5px;
        border-top: 1px solid #f0f0f0;
    }
    .ap-card-lead { text-align: center; }
    .ap-lead-lbl {
        display: block;
        font-size: 9px;
        color: #bbb;
        text-transform: uppercase;
        letter-spacing: 0.03em;
        line-height: 1.3;
        margin-bottom: 1px;
    }
    .ap-card-lead-val { font-size: 12px; color: #333; }
}
.forecast-rows { display: flex; flex-direction: row; gap: 6px; overflow-x: auto; padding-bottom: 6px; }
.fcst-row {
    display: flex;
    flex-direction: column;
    gap: 16px;
    min-width: 150px;
    flex: 0 0 auto;
    background: #fff;
    border: 1px solid #ddd;
    border-radius: 4px;
    padding: 12px 16px;
    align-items: stretch;
}
.fcst-row.now-row { border-color: #b0c4de; background: #f5f8fc; }
.fcst-row-main { }
.fcst-row-refs { display: flex; flex-direction: column; gap: 20px; }
.fcst-label { font-size:11px; font-weight:700; text-transform:uppercase; letter-spacing:.06em; color:#888; margin-bottom:6px; }
.fcst-temp { font-size:26px; font-weight:700; color:#1a1a1a; line-height:1; }
.fcst-confidence { font-size:11px; color:#aaa; margin-top:2px; margin-bottom:8px; }
.fcst-details { font-size:12px; color:#555; line-height:1.8; }
.fcst-details .detail-label { color:#999; }
.fcst-no-data { color:#bbb; font-size:13px; }
.fcst-ref { flex: 1; min-width: 0; font-size:11px; color:#999; line-height:1.8; }
.fcst-ref-lbl { font-size:10px; font-weight:700; text-transform:uppercase; letter-spacing:.05em; color:#ccc; display:block; line-height:1.6; }
.fcst-ref .detail-label { color:#bbb; }
.fcst-ref-temp { font-size:14px; }
.fcst-delta { font-size:10px; color:#999; margin-left:3px; }
.jump-nav {
    grid-area: nav;
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
.zambretti-tendency { font-size: 13px; color: #555; margin-top: 4px; }
.zambretti-algo { font-size: 11px; color: #888; margin-top: 4px; }
"""


def _weights_section_html(
    rows: list,
    all_members: list | None = None,
    ext_corrected_html: str = "",
) -> str:
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

    _EXT_CORRECTED_ID = 202

    # ensemble_bias_correction (model 101) has no weight table of its own --
    # it re-enters barogram_ensemble as member 101, so its tuned weight lives
    # in avg_weights[100][101]. Surfaced via the exclude-101 toggle on model
    # 100's own block below, not a separate pinned block.
    _ENSEMBLE_ID = 100
    _BIAS_CORRECTION_ID = 101

    blocks = []
    def _sectored_table(model_id, data, eq_w):
        # per-sector columns: compute max weight per sector for coloring
        sector_max = {}
        for sector in range(4):
            vals = [sectors.get(sector, eq_w) for sectors in data.values()]
            sector_max[sector] = max(vals)

        header_cells = "".join(
            f'<th class="wt-pct">{_SECTOR_LABELS[s]}<br>'
            f'<span style="font-weight:400;color:#999">{_SECTOR_HOURS[s]}</span></th>'
            for s in range(4)
        )
        table_rows = []
        prev_group = None
        for mem_id in sorted(data):
            sectors = data[mem_id]
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
                w = sectors.get(sector, eq_w)
                spread = sector_max[sector] - eq_w
                if spread > 0 and w > eq_w:
                    opacity = min((w - eq_w) / spread, 1.0) * 0.45
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
        return thead, "".join(table_rows)

    for model_id in sorted(avg_weights):
        if model_id == _EXT_CORRECTED_ID:
            continue
        members_data = avg_weights[model_id]
        sectored = has_sectors.get(model_id, False)
        n = len(members_data)
        equal_w = 1.0 / n
        tuned = model_id in tuned_ids
        table_html = None

        if sectored:
            thead, table_rows_html = _sectored_table(model_id, members_data, equal_w)

            if model_id == _ENSEMBLE_ID and _BIAS_CORRECTION_ID in members_data:
                # purely visual counterfactual: what would the other members'
                # weights be if ensemble_bias_correction weren't a member,
                # renormalized so they still sum to 1. Does not touch the
                # actual weights table or how tune/blend_cells compute this.
                w101 = {
                    s: members_data[_BIAS_CORRECTION_ID].get(s, equal_w)
                    for s in range(4)
                }
                members_without = {}
                for mem_id, sectors in members_data.items():
                    if mem_id == _BIAS_CORRECTION_ID:
                        continue
                    without_sectors = {}
                    for s in range(4):
                        w = sectors.get(s, equal_w)
                        denom = 1 - w101[s]
                        without_sectors[s] = w / denom if denom > 0 else w
                    members_without[mem_id] = without_sectors
                equal_w_without = 1.0 / len(members_without)
                thead_wo, rows_wo = _sectored_table(model_id, members_without, equal_w_without)

                table_html = (
                    f'<input type="checkbox" id="wt101-toggle-{model_id}" class="wt101-checkbox">'
                    f'<div class="wt101-toggle-bar">'
                    f'<label for="wt101-toggle-{model_id}" class="mae-raw-btn">'
                    f'Exclude ensemble_bias_correction (model 101)</label>'
                    f'</div>'
                    f'<div class="table-scroll wt101-view-with">'
                    f'<table class="weight-table">{thead}<tbody>{table_rows_html}</tbody></table>'
                    f'</div>'
                    f'<div class="table-scroll wt101-view-without">'
                    f'<table class="weight-table">{thead_wo}<tbody>{rows_wo}</tbody></table>'
                    f'</div>'
                )
            else:
                table_html = (
                    f'<div class="table-scroll">'
                    f'<table class="weight-table">{thead}<tbody>{table_rows_html}</tbody></table>'
                    f'</div>'
                )
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
            table_html = (
                f'<div class="table-scroll">'
                f'<table class="weight-table">{thead}<tbody>{"".join(table_rows)}</tbody></table>'
                f'</div>'
            )

        untrained_note = (
            '' if tuned
            else ' <span style="color:#aaa;font-style:italic;font-weight:400">(not tuned)</span>'
        )
        block_inner = (
            f'<div class="weights-model-block">'
            f'<h3>{model_names[model_id]}{untrained_note}'
            f' <span class="model-id-cell">(model {model_id})</span></h3>'
            f'<p class="window-label">equal weight: {equal_w:.1%} per member</p>'
            f'{table_html}'
            f'</div>'
        )
        blocks.append((model_id, block_inner))

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

    return ensemble_html + others_html + ext_corrected_html


def _external_corrected_source_weights_html(rows: list) -> str:
    """Build a source-skill weight table for external_corrected.

    Shows NWS vs. Tempest group weights per (variable, lead_hours, hour_bucket) cell,
    derived from historical MAE of each group's corrected forecasts.
    """
    _MIN_SAMPLES = 3
    _SOURCE_FLOOR = 0.10
    _VARIABLES = ["temperature", "dewpoint"]
    _LEADS = [6, 12, 18, 24]
    _BUCKET_LABELS = [
        ("night", "00-05"),
        ("morning", "06-11"),
        ("afternoon", "12-17"),
        ("evening", "18-23"),
    ]

    mae: dict = {}
    for r in rows:
        if r["n"] >= _MIN_SAMPLES:
            mae[(r["src"], r["variable"], r["lead_hours"], r["hour_bucket"])] = r["avg_mae"]

    def cell_weights(variable, lead, hb):
        nm = mae.get(("nws", variable, lead, hb))
        tm = mae.get(("tempest", variable, lead, hb))
        if nm is None and tm is None:
            return None, None
        if nm is None:
            return 0.0, 1.0
        if tm is None:
            return 1.0, 0.0
        ni = 1.0 / nm if nm > 0 else 1e9
        ti = 1.0 / tm if tm > 0 else 1e9
        total = ni + ti
        nw, tw = ni / total, ti / total
        if nw < _SOURCE_FLOOR:
            nw = _SOURCE_FLOOR
            tw = 1.0 - nw
        elif tw < _SOURCE_FLOOR:
            tw = _SOURCE_FLOOR
            nw = 1.0 - tw
        return nw, tw

    header_cells = "".join(
        f'<th class="wt-pct">{label}<br>'
        f'<span style="font-weight:400;color:#999">{hours}</span></th>'
        for label, hours in _BUCKET_LABELS
    )
    thead = f'<thead><tr><th>Variable / Lead</th>{header_cells}</tr></thead>'

    table_rows = []
    for variable in _VARIABLES:
        for lead in _LEADS:
            cells = []
            for hb in range(4):
                nw, tw = cell_weights(variable, lead, hb)
                if nw is None:
                    cells.append('<td class="wt-pct" style="color:#bbb">—</td>')
                    continue
                deviation = abs(nw - 0.5)
                opacity = min(deviation / 0.4, 1.0) * 0.45
                if nw > 0.5 + 0.02:
                    color = f'background:rgba(59,91,219,{opacity:.3f})'
                    label = f'NWS {nw:.0%}'
                elif tw > 0.5 + 0.02:
                    color = f'background:rgba(219,120,59,{opacity:.3f})'
                    label = f'Tmp {tw:.0%}'
                else:
                    color = ''
                    label = '50 / 50'
                style = f' style="{color}"' if color else ''
                cells.append(f'<td class="wt-pct"{style}>{label}</td>')
            row_label = f'{variable}&nbsp;{lead}h'
            table_rows.append(
                f'<tr><th>{row_label}</th>{"".join(cells)}</tr>'
            )

    table_html = (
        f'<div class="weights-model-block">'
        f'<h3>external_corrected'
        f' <span class="model-id-cell">(model 202)</span></h3>'
        f'<p class="window-label" style="margin-bottom:6px">'
        f'NWS group (members 1–5) vs. Tempest group (members 6–10), '
        f'inverse-MAE weighting per variable, lead, and time of day. '
        f'Blue = NWS leads, orange = Tempest leads. \u2014 means insufficient history.'
        f'</p>'
        f'<div class="table-scroll">'
        f'<table class="weight-table">'
        f'{thead}'
        f'<tbody>{"".join(table_rows)}</tbody>'
        f'</table>'
        f'</div>'
        f'</div>'
    )
    return (
        f'<details class="collapsible-section" style="margin-top:12px">'
        f'<summary>external_corrected'
        f' <span class="model-id-cell" style="font-weight:400">(model 202)</span>'
        f'</summary>'
        f'<div class="weights-section" style="margin-top:8px">{table_html}</div>'
        f'</details>'
    )


def _zambretti_panel_html(z: dict | None) -> str:
    if z is None or z.get("letter") == "\u2014":
        return ""
    cat = z["category"].replace("_", " ")
    rate = z["rate_hpa_per_h"]
    rate_str = f"{rate:+.2f} hPa/h" if rate is not None else "\u2014"
    return (
        f'<div class="card" style="margin-top:12px">'
        f'<h3>Zambretti forecast for today: {z["description"]}'
        f' <span class="station-id">({z["letter"]})</span></h3>'
        f'<p class="zambretti-tendency">'
        f'Tendency: {cat} ({rate_str})'
        f'</p>'
        f'<p class="zambretti-tendency">'
        f'Wind: {z["wind_dir_text"]}, {z["season_text"]}'
        f'</p>'
        f'<p class="zambretti-algo">'
        f'Zambretti algorithm: sea-level pressure, trend, wind direction, season, '
        f'as of ~09:00 solar time'
        f'</p>'
        f'</div>'
    )


_NWS_FALLBACK_FIELDS = ("air_temp", "dew_point", "wind_speed", "wind_direction", "sea_level_pressure", "sky_cover")


def _fill_nws_gaps(latest: dict | None, history: list) -> tuple[dict | None, dict]:
    if latest is None:
        return None, {}
    filled = dict(latest)
    fallback_ts: dict[str, int] = {}
    for field in _NWS_FALLBACK_FIELDS:
        if filled.get(field) is None:
            for row in history:
                if row.get(field) is not None:
                    filled[field] = row[field]
                    fallback_ts[field] = row["timestamp"]
                    break
    return filled, fallback_ts


def _conditions_card(label: str, obs, elevation_m: float = 0.0, fallback_ts: dict | None = None) -> str:
    if obs is None:
        return (
            f'<div class="card"><h3>{label}</h3>'
            f'<p class="muted">no data</p></div>'
        )

    fallback_ts = fallback_ts or {}
    station_id = obs["station_id"]
    name = obs["name"] or station_id
    timestamp = fmt.ts(obs["timestamp"])

    def _fb(field: str) -> str:
        ts = fallback_ts.get(field)
        if ts is None:
            return ""
        t = datetime.fromtimestamp(ts, tz=fmt.CENTRAL).strftime("%H:%M")
        return f' <span class="obs-fallback">({t})</span>'

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
            f'<tr><th>Temperature</th><td style="font-weight:700;font-size:1.2em">{fmt.temp(obs["air_temp"])}</td></tr>'
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
            f'<tr><th>Temperature</th><td>{fmt.temp(obs["air_temp"])}{_fb("air_temp")}</td></tr>'
            f'<tr><th>Dew Point</th><td>{fmt.temp(obs["dew_point"])}{_fb("dew_point")}</td></tr>'
            f'<tr><th>Wind</th><td>{fmt.wind_dir(obs["wind_direction"])} {fmt.val(_to_mph(obs["wind_speed"]), ".1f", " mph")}{_fb("wind_speed")}</td></tr>'
            f'<tr><th>Pressure</th><td>{fmt.val(nws_slp, ".1f", " hPa")}{_fb("sea_level_pressure")}</td></tr>'
            f'<tr><th>Sky</th><td>{obs["sky_cover"] or "\u2014"}{_fb("sky_cover")}</td></tr>'
            f'<tr><th>METAR</th><td>{obs["raw_metar"] or "\u2014"}</td></tr>'
        )

    heading = name if label == "Tempest" else f"{label}: {name}"
    return (
        f'<div class="card">'
        f'<h3>{heading}'
        + (f' <span class="station-id">({station_id})</span>' if station_id else "")
        + '</h3>'
        f'<p class="obs-time">{timestamp}</p>'
        f'<table class="obs-table"><tbody>{rows_html}</tbody></table>'
        f'</div>'
    )


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
        slp_cell = f'<td data-label="SLP">{fmt.val(slp, ".1f", " hPa")}</td>'
    return (
        "<tr>"
        f"<td>{fmt.short_ts(row['timestamp'])}</td>"
        f'<td data-label="Temp">{fmt.temp(row["air_temp"])}</td>'
        f'<td data-label="Dew Pt">{fmt.temp(row["dew_point"])}</td>'
        f'<td data-label="Station P">{fmt.val(sp, ".1f", " hPa")}</td>'
        f"{slp_cell}"
        f'<td data-label="Wind">{wind}</td>'
        f'<td data-label="Precip">{fmt.val(_to_in(row["precip_accum_day"]), ".2f", " in")}</td>'
        f'<td data-label="Lightning">{lc if lc is not None else 0}</td>'
        "</tr>"
    )


def _nws_obs_row(row) -> str:
    return (
        "<tr>"
        f"<td>{fmt.short_ts(row['timestamp'])}</td>"
        f'<td data-label="Temp">{fmt.temp(row["air_temp"])}</td>'
        f'<td data-label="Dew Pt">{fmt.temp(row["dew_point"])}</td>'
        f'<td data-label="Wind">{fmt.wind_dir(row["wind_direction"])} {fmt.val(_to_mph(row["wind_speed"]), ".1f", " mph")}</td>'
        f'<td data-label="Pressure">{fmt.val(row["sea_level_pressure"], ".1f", " hPa")}</td>'
        f'<td data-label="Sky">{row["sky_cover"] or "\u2014"}</td>'
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

    def table_block(label: str, obs_list: list, tbody_id: str, headers: list, extra_class: str = "") -> str:
        heading = station_heading(label, obs_list)
        header_html = "".join(f"<th>{h}</th>" for h in headers)
        empty = (
            f'<tr><td colspan="{len(headers)}" class="muted">no data</td></tr>'
            if not obs_list else ""
        )
        cls = f"obs-history-table {extra_class}".strip()
        return (
            f'<h3 class="obs-subhead">{heading}</h3>'
            f'<table class="{cls}">'
            f'<thead><tr>{header_html}</tr></thead>'
            f'<tbody id="{tbody_id}">{empty}</tbody>'
            f'</table>'
        )

    tempest_headers = (
        ["Time", "Temperature", "Dew Point", "Station P", "SLP", "Wind", "Precip (day)", "Lightning"]
        if elevation_m > 0.0 else
        ["Time", "Temperature", "Dew Point", "Pressure", "Wind", "Precip (day)", "Lightning"]
    )
    tempest_block = table_block(
        "Tempest", tempest_obs, "tempest-obs-tbody",
        tempest_headers, extra_class="tempest-obs",
    )
    nws_block = table_block(
        "NWS", nws_obs, "nws-obs-tbody",
        ["Time", "Temperature", "Dew Point", "Wind", "Pressure", "Sky"],
        extra_class="nws-obs",
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

function renderAll(rows, tbodyId) {{
    document.getElementById(tbodyId).innerHTML = rows.slice(0, 10).join('');
}}

renderAll(tempestHistory, 'tempest-obs-tbody');
renderAll(nwsHistory, 'nws-obs-tbody');
"""



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
                    x.append(fmt.iso_ts(issued))
                    y.append(bias_display)
                series[var] = {"x": x, "y": y}
            result[str(lead)][model] = {
                "is_persistence": is_persistence,
                "is_ensemble": is_ensemble,
                "model_id": model_meta[model]["model_id"],
                "series": series,
            }
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
            "avg_confidence": row["avg_confidence"],
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
        if (!members[key][r.variable]) {{
            members[key][r.variable] = {{sum: 0, n: 0, confSum: 0, confN: 0}};
        }}
        if (r.avg_mae !== null) {{
            members[key][r.variable].sum += r.avg_mae * r.n;
            members[key][r.variable].n += r.n;
        }}
        if (r.avg_confidence !== null && r.avg_confidence !== undefined) {{
            members[key][r.variable].confSum += r.avg_confidence * r.n;
            members[key][r.variable].confN += r.n;
        }}
    }});
    const varCols = memberVariables.filter(function(v) {{
        return rows.some(function(r) {{ return r.variable === v; }});
    }});
    const headerCells = varCols.map(function(v) {{
        const unit = memberUnits[v] ? ' (' + memberUnits[v] + ')' : '';
        return '<th>Avg MAE' + unit + '</th><th>Avg Confidence</th>';
    }}).join('');
    const bodyRows = Object.entries(members).map(function([key, varData]) {{
        const label = key.split(':')[1];
        const cells = varCols.map(function(v) {{
            const d = varData[v];
            const maeCell = (!d || d.n === 0) ? '<td>\u2014</td>' : '<td>' + (d.sum / d.n).toFixed(2) + '</td>';
            const confCell = (!d || d.confN === 0) ? '<td>\u2014</td>' : '<td>' + Math.round((d.confSum / d.confN) * 100) + '%</td>';
            return maeCell + confCell;
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
            line: {{ color: plotBg().zero, width: 1, dash: 'dot' }}
        }}];
        Plotly.react('bias-chart-' + lead, traces, {{
            title: {{ text: '+' + lead + 'h \u2014 ' + (biasFilterLabels[biasActiveVar] || biasActiveVar),
                      font: {{ size: 13, family: '-apple-system, sans-serif' }} }},
            margin: {{ t: 40, b: 60, l: 50, r: 16 }},
            xaxis: {{ type: 'date', tickangle: 0, tickfont: {{ size: 10 }}, nticks: 4 }},
            yaxis: {{ tickfont: {{ size: 11 }} }},
            height: 380,
            showlegend: false,
            shapes: shapes,
            font: {{ color: plotBg().font }},
            paper_bgcolor: plotBg().paper,
            plot_bgcolor: plotBg().plot
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

const biasSection = document.querySelector('#analysis > details.collapsible-section');
if (biasSection) {{
    biasSection.addEventListener('toggle', function() {{
        if (biasSection.open) {{
            biasLeads.forEach(function(lead) {{
                const c = document.getElementById('bias-chart-' + lead);
                if (c) Plotly.Plots.resize(c);
            }});
        }}
    }});
}}
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
    Plotly.react('heatmap-chart', [{{
        type: 'heatmap',
        x: leads,
        y: models,
        z: z,
        colorscale: 'RdYlGn',
        reversescale: true,
        showscale: true,
        hovertemplate: '%{{y}}<br>+%{{x}}h<br>MAE: %{{z:.2f}}<extra></extra>'
    }}], {{
        title: {{ text: 'Score Heatmap \u2014 ' + heatmapActiveVar.replace('_', ' '),
                  font: {{ size: 13, family: '-apple-system, sans-serif' }} }},
        margin: {{ t: 40, b: 60, l: 180, r: 16 }},
        xaxis: {{ title: 'Lead hours', tickvals: leads, tickfont: {{ size: 11 }} }},
        yaxis: {{ tickmode: 'array', tickvals: models, ticktext: models, tickfont: {{ size: 11 }}, automargin: true }},
        height: Math.max(300, models.length * 28 + 120),
        showlegend: false,
        font: {{ color: plotBg().font }},
        paper_bgcolor: plotBg().paper,
        plot_bgcolor: plotBg().plot
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
        line: {{ color: plotBg().zero, width: 1, dash: 'dot' }}
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
        font: {{ color: plotBg().font }},
        paper_bgcolor: plotBg().paper,
        plot_bgcolor: plotBg().plot
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
def _ensemble_forecast_section(
    mean_rows: list, tempest, elevation_m: float = 0.0, nws_forecast: dict | None = None
) -> str:
    ens_rows = [
        r for r in mean_rows
        if r["model"] == "barogram_ensemble" and r["member_id"] == 0
    ]

    if not ens_rows:
        return (
            '<section class="section">\n'
            '  <h2>Ensemble Forecast</h2>\n'
            '  <p class="muted">Ensemble model not yet available (in development).</p>\n'
            '</section>\n'
        )

    issued_at = ens_rows[0]["issued_at"]
    issued_str = datetime.fromtimestamp(issued_at, tz=fmt.CENTRAL).strftime(
        "Generated at %H:%M on %B %-d, %Y"
    )

    # {variable: {lead_hours: (value, spread, confidence)}} for barogram ensemble
    # pressure excluded: the ensemble no longer combines or outputs it
    ens_table: dict[str, dict[int, tuple]] = {
        v: {} for v in VARIABLES if v != "pressure"
    }
    lead_valid_at: dict[int, int] = {}
    for row in ens_rows:
        if row["variable"] in ens_table:
            ens_table[row["variable"]][row["lead_hours"]] = (row["value"], row["spread"], row["confidence"])
        lead_valid_at.setdefault(row["lead_hours"], row["valid_at"])

    # {lead_hours: {variable: value}} and {lead_hours: valid_at} for reference models
    tempest_by_lead: dict[int, dict] = {}
    tempest_vat: dict[int, int] = {}
    for row in mean_rows:
        if row["model"] == "tempest_forecast" and row["member_id"] == 0:
            tempest_by_lead.setdefault(row["lead_hours"], {})[row["variable"]] = row["value"]
            tempest_vat.setdefault(row["lead_hours"], row["valid_at"])

    corrected_by_lead: dict[int, dict] = {}
    corrected_vat: dict[int, int] = {}
    for row in mean_rows:
        if row["model"] == "external_corrected" and row["member_id"] == 0:
            corrected_by_lead.setdefault(row["lead_hours"], {})[row["variable"]] = row["value"]
            corrected_vat.setdefault(row["lead_hours"], row["valid_at"])

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

    def _nws_at(target_ts: int) -> tuple[int, dict] | None:
        if not nws_forecast:
            return None
        best = min(nws_forecast, key=lambda t: abs(t - target_ts))
        if abs(best - target_ts) > 5400:
            return None
        return best, nws_forecast[best]

    def _fmt_time(ts: int) -> str:
        return datetime.fromtimestamp(ts, tz=fmt.CENTRAL).strftime("%H:%M")

    def _lead_label(lead: int) -> str:
        vat = lead_valid_at.get(lead)
        if vat:
            return datetime.fromtimestamp(vat, tz=fmt.CENTRAL).strftime("%H:%M")
        return f"+{lead}h"

    def _ref_panel(label: str, temp_val, dew_val,
                   ens_temp_val=None, valid_time_str: str | None = None) -> str:
        lines = []
        if temp_val is not None:
            ref_temp_f = _to_f(temp_val)
            delta_html = ''
            if ens_temp_val is not None:
                delta_f = ref_temp_f - _to_f(ens_temp_val)
                sign = '+' if delta_f >= 0 else ''
                delta_html = f' <span class="fcst-delta">{sign}{delta_f:.0f}\u00b0</span>'
            lines.append(
                f'<span class="detail-label">Temp</span>'
                f' <span class="fcst-ref-temp">{ref_temp_f:.0f}\u00b0F</span>{delta_html}'
            )
        if dew_val is not None:
            lines.append(f'<span class="detail-label">Dew</span> {_to_f(dew_val):.0f}\u00b0F')
        if not lines:
            return ''
        _ref_tooltips = {
            "Tempest": "Built-in forecast from the Tempest API. Not included in the barogram ensemble.",
            "NWS": "External forecast from the National Weather Service (api.weather.gov). Not included in the barogram ensemble.",
            "Corrected": "NWS and Tempest forecasts with bias corrections learned from historical scoring, conditioned on time of day, season, and airmass state. Not included in the barogram ensemble.",
        }
        tip = _ref_tooltips.get(label, "")
        title_attr = f' title="{tip}"' if tip else ""
        display_label = f"{label} ({valid_time_str})" if valid_time_str else label
        return (
            f'<div class="fcst-ref">'
            f'<span class="fcst-ref-lbl"{title_attr}>{display_label}</span>'
            + '<br>'.join(lines)
            + '</div>'
        )

    def _card(label: str, is_now: bool,
              temp_val, dew_val, wind_val,
              temp_confidence=None,
              tempest_ref=None, nws_ref=None, corrected_ref=None,
              tempest_time_str: str | None = None,
              nws_time_str: str | None = None,
              corrected_time_str: str | None = None) -> str:
        cls = 'fcst-row now-row' if is_now else 'fcst-row'
        if temp_val is not None:
            if temp_confidence is not None:
                conf_html = (
                    f'<div class="fcst-confidence" title="Ensemble confidence">'
                    f'{round(temp_confidence * 100)}%</div>'
                )
            else:
                conf_html = '<div class="fcst-confidence"></div>'
            temp_html = (
                f'<div class="fcst-temp">{_to_f(temp_val):.0f}\u00b0F</div>'
                f'{conf_html}'
            )
        else:
            temp_html = (
                '<div class="fcst-no-data">&mdash;</div>'
                '<div class="fcst-confidence"></div>'
            )
        details = []
        if dew_val is not None:
            details.append(f'<span class="detail-label">Dew</span> {_to_f(dew_val):.0f}\u00b0F')
        if wind_val is not None:
            details.append(f'<span class="detail-label">Wind</span> {_to_mph(wind_val):.0f} mph')
        details_html = (
            '<div class="fcst-details">' + '<br>'.join(details) + '</div>'
            if details else ''
        )
        main_html = (
            f'<div class="fcst-row-main">'
            f'<div class="fcst-label" title="Barogram ensemble forecast">{label}</div>'
            f'{temp_html}'
            f'{details_html}'
            f'</div>'
        )
        refs = []
        if corrected_ref is not None:
            refs.append(_ref_panel(
                'Corrected',
                corrected_ref.get('temperature'), corrected_ref.get('dewpoint'),
                temp_val,
                corrected_time_str,
            ))
        if tempest_ref is not None:
            refs.append(_ref_panel(
                'Tempest',
                tempest_ref.get('temperature'), tempest_ref.get('dewpoint'),
                temp_val,
                tempest_time_str,
            ))
        if nws_ref is not None:
            refs.append(_ref_panel(
                'NWS',
                nws_ref.get('temperature'), nws_ref.get('dewpoint'),
                temp_val,
                nws_time_str,
            ))
        refs_html = (
            '<div class="fcst-row-refs">' + ''.join(refs) + '</div>'
            if refs else ''
        )
        return f'<div class="{cls}">{main_html}{refs_html}</div>\n'

    cards = ""
    for lead in sorted(lead_valid_at):
        t_cell = ens_table.get("temperature", {}).get(lead)
        d_cell = ens_table.get("dewpoint", {}).get(lead)
        vat = lead_valid_at.get(lead)
        nws_result = _nws_at(vat) if vat else None
        nws_ts, nws_entry = nws_result if nws_result else (None, None)
        cards += _card(
            _lead_label(lead), False,
            t_cell[0] if t_cell else None,
            d_cell[0] if d_cell else None,
            None,
            t_cell[2] if t_cell else None,
            tempest_by_lead.get(lead) or None,
            nws_entry,
            corrected_by_lead.get(lead) or None,
            tempest_time_str=_fmt_time(tempest_vat[lead]) if lead in tempest_vat else None,
            nws_time_str=_fmt_time(nws_ts) if nws_ts else None,
            corrected_time_str=_fmt_time(corrected_vat[lead]) if lead in corrected_vat else None,
        )

    return (
        '<section class="section" id="forecast">\n'
        f'  <h2>Ensemble Forecast: {issued_str}</h2>\n'
        '  <div class="forecast-rows">\n'
        f'{cards}'
        '  </div>\n'
        '</section>\n'
    )


_ACC_VARIABLES = ["temperature", "dewpoint", "pressure"]

# lead hours shown in the accuracy-by-lead table and bias-over-time charts —
# a fixed checkpoint set, not the full hourly resolution used elsewhere
_KEY_LEADS = {1, 6, 12, 18, 24}


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


def _accuracy_lead_table_html(rows: list, lead_times: list, member_models: set | None = None) -> str:
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

    header = "<th>#</th><th>Model</th>" + "".join(f"<th>+{lt}h</th>" for lt in lts)
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
                f' data-{var}="{model_data[name][var].get(lt)!r}"'
                if model_data[name][var].get(lt) is not None
                else f' data-{var}=""'
                for var in _ACC_VARIABLES
            )
            def_skill = model_data[name]["temperature"].get(lt)
            display = f"{def_skill:.0f}%" if def_skill is not None else "—"
            cls = _acc_cls(def_skill)
            cells += (
                f'<td class="acc-cell{cls}" data-lead="{lt}"'
                f' data-mid="{meta["model_id"]}"{data_attrs}>{display}</td>'
            )
        mbtn = ""
        if member_models and name in member_models:
            mbtn = f' <button class="member-btn" data-model="{name}">members</button>'
        body_rows.append(
            f'<tr{row_cls}>'
            f'<td class="model-id-cell">{meta["model_id"]}</td>'
            f'<th class="model-name-cell">{name} {badge}{mbtn}</th>{cells}</tr>'
        )
        if member_models and name in member_models:
            safe = name.replace("_", "-").replace(" ", "-")
            n_cols = 2 + len(lts)
            body_rows.append(
                f'<tr class="member-detail-row" id="mdr-{safe}" style="display:none">'
                f'<td colspan="{n_cols}" id="md-{safe}"></td>'
                f'</tr>'
            )

    return (
        f'<table class="obs-history-table acc-lead-table">'
        f'<thead><tr>{header}</tr></thead>'
        f'<tbody>{"".join(body_rows)}</tbody>'
        f'</table>'
    )


def _confidence_lead_table_html(rows: list, lead_times: list) -> str:
    """Confidence table: rows=models, cols=lead times, filterable by variable."""
    if not rows:
        return '<p class="muted">no confidence data</p>'

    model_data: dict = {}
    model_meta: dict = {}
    for r in rows:
        name = r["model"]
        if name not in model_data:
            model_data[name] = {v: {} for v in _ACC_VARIABLES}
            model_meta[name] = {"model_id": r["model_id"], "type": r["type"]}
        var = r["variable"]
        if var in _ACC_VARIABLES and r["avg_confidence"] is not None:
            model_data[name][var][r["lead_hours"]] = r["avg_confidence"] * 100

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

    header = "<th>#</th><th>Model</th>" + "".join(f"<th>+{lt}h</th>" for lt in lts)
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
                f' data-{var}="{model_data[name][var].get(lt)!r}"'
                if model_data[name][var].get(lt) is not None
                else f' data-{var}=""'
                for var in _ACC_VARIABLES
            )
            def_conf = model_data[name]["temperature"].get(lt)
            display = f"{def_conf:.0f}%" if def_conf is not None else "—"
            cls = _acc_cls(def_conf)
            cells += (
                f'<td class="acc-cell{cls}" data-lead="{lt}"'
                f' data-mid="{meta["model_id"]}"{data_attrs}>{display}</td>'
            )
        body_rows.append(
            f'<tr{row_cls}>'
            f'<td class="model-id-cell">{meta["model_id"]}</td>'
            f'<th class="model-name-cell">{name} {badge}</th>{cells}</tr>'
        )

    return (
        f'<table class="obs-history-table acc-lead-table">'
        f'<thead><tr>{header}</tr></thead>'
        f'<tbody>{"".join(body_rows)}</tbody>'
        f'</table>'
    )


def _overall_accuracy_html(rows: list) -> str:
    """Avg forecast skill per model across temperature, dewpoint, and pressure."""
    if not rows:
        return '<p class="muted">no scored forecasts</p>'

    _OVERALL_VARS: tuple = ("temperature", "dewpoint", "pressure")

    climo_mae: dict = {}
    for r in rows:
        if r["model"] == "climatological_mean" and r["variable"] in _OVERALL_VARS:
            climo_mae[(r["variable"], r["lead_hours"])] = r["avg_mae"]

    model_skills: dict[str, list] = {}
    model_meta: dict = {}
    for r in rows:
        name = r["model"]
        var = r["variable"]
        if var not in _OVERALL_VARS:
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
        tooltip = _MODEL_TOOLTIPS.get(name, "")
        title_attr = f' title="{tooltip}"' if tooltip else ""
        body_rows.append(
            f'<tr{row_cls}>'
            f'<td class="model-id-cell">{meta["model_id"]}</td>'
            f'<td class="model-name-cell" style="text-align:left;font-weight:500"><span{title_attr}>{name}</span> {badge}</td>'
            f'<td class="acc-cell{cls}" style="font-size:15px;font-weight:600">{avg_skill:.0f}%</td>'
            f'</tr>'
        )

    return (
        f'<table class="obs-history-table acc-overall-table">'
        f'<thead><tr><th>#</th><th>Model</th><th>Forecast Skill</th></tr></thead>'
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
    """Pivot per-run avg_skill rows into per-model series for all models."""
    from collections import defaultdict
    by_day: dict = defaultdict(dict)
    model_names: dict = {}
    for r in rows:
        by_day[r["day"]][r["model_id"]] = r["avg_skill"]
        model_names[r["model_id"]] = r["model"]

    days = sorted(by_day.keys())
    models: dict = {}
    for mid, name in model_names.items():
        series = [by_day[d].get(mid) for d in days]
        models[mid] = {"name": name, "skill": series}

    ens = models.get(100)
    if ens:
        ens["trend"] = _trend_values(ens["skill"]) or []

    return {"days": days, "models": models}


def _skill_timeseries_html() -> str:
    """Heading + window-toggled chart containers + all-models toggle for skill-over-time."""
    windows = [
        ("14d", ""),
        ("120d", ' style="display:none"'),
        ("alltime", ' style="display:none"'),
        ("10r", ' style="display:none"'),
    ]
    parts = [
        '<details class="collapsible-section" id="skill-timeseries-section">',
        '<summary class="obs-subhead">Skill Over Time</summary>',
        '<div class="mae-filter-bar"><button id="skill-all-models-toggle" class="mae-raw-btn">All models</button></div>',
        '<p class="chart-legend-note">Per-run forecast skill vs. climatological mean (0% line). '
        'Averaged across all variables. Default: ensemble, NWS, Tempest Forecast.</p>',
    ]
    for wid, hidden in windows:
        parts.append(
            f'<div id="skill-timeseries-{wid}"{hidden}>'
            f'<div class="chart-container"><div id="skill-timeseries-chart-{wid}"></div></div>'
            f'</div>'
        )
    parts.append('</details>')
    return "\n".join(parts)


def _skill_timeseries_js(data_14d: dict, data_120d: dict, data_alltime: dict, data_10r: dict) -> str:
    """Plotly + toggle logic for the four skill-over-time charts."""
    import json as _json
    windows = [("14d", data_14d), ("120d", data_120d), ("alltime", data_alltime), ("10r", data_10r)]
    data_by_window: dict = {}
    for wid, d in windows:
        data_by_window[wid] = d
    data_j = _json.dumps({
        wid: {"days": d["days"], "models": {str(mid): m for mid, m in d["models"].items()}}
        for wid, d in data_by_window.items()
    })
    return f"""const _skillRawData = {data_j};
const SKILL_DEFAULT_MIDS = [100, 200, 201, 202];
const SKILL_KNOWN_COLORS = {{100: '#1f77b4', 200: '#ff7f0e', 201: '#2ca02c', 202: '#9467bd'}};
const SKILL_EXTRA_PALETTE = ['#d62728','#9467bd','#8c564b','#e377c2','#7f7f7f','#bcbd22','#17becf'];
let skillShowAll = false;

function renderSkillTimeseries(wid) {{
    const raw = _skillRawData[wid];
    if (!raw) return;
    const days = raw.days;
    const models = raw.models;
    const allMids = Object.keys(models).map(Number);
    // climatological_mean is always at 0% by definition — exclude from all-models view
    const nonClimo = allMids.filter(function(m) {{ return models[String(m)].name !== 'climatological_mean'; }});
    const show = skillShowAll ? nonClimo : nonClimo.filter(function(m) {{ return SKILL_DEFAULT_MIDS.includes(m); }});
    let extIdx = 0;
    const traces = [];
    show.slice().sort(function(a, b) {{ return a - b; }}).forEach(function(mid) {{
        const color = SKILL_KNOWN_COLORS[mid] || SKILL_EXTRA_PALETTE[extIdx++ % SKILL_EXTRA_PALETTE.length];
        const mdata = models[String(mid)];
        traces.push({{x: days, y: mdata.skill, name: mdata.name, type: 'scatter',
            mode: 'lines+markers', connectgaps: false,
            line: {{color: color}}, marker: {{size: 4}}}});
    }});
    const ens = models['100'];
    if (ens && ens.trend && ens.trend.length) {{
        traces.push({{x: days, y: ens.trend, name: 'ensemble trend', type: 'scatter',
            mode: 'lines', connectgaps: true, showlegend: true,
            line: {{color: '#1f77b4', dash: 'dash', width: 1.5}}}});
    }}
    Plotly.react('skill-timeseries-chart-' + wid, traces, {{
        height: 340, margin: {{t: 30, b: 100, l: 50, r: 16}},
        font: {{ color: plotBg().font }}, paper_bgcolor: plotBg().paper, plot_bgcolor: plotBg().plot,
        yaxis: {{title: 'Skill (%)', zeroline: true, zerolinecolor: '#888', zerolinewidth: 2}},
        xaxis: {{type: 'date'}},
        legend: {{orientation: 'h', x: 0, y: -0.18, xanchor: 'left', yanchor: 'top', font: {{size: 10}}}},
        shapes: [{{type: 'line', xref: 'paper', x0: 0, x1: 1, y0: 0, y1: 0,
            line: {{color: '#888', width: 2, dash: 'dash'}}}}]
    }}, {{responsive: true}});
}}

['14d', '120d', 'alltime', '10r'].forEach(function(wid) {{ renderSkillTimeseries(wid); }});

document.getElementById('skill-all-models-toggle').addEventListener('click', function() {{
    skillShowAll = !skillShowAll;
    this.classList.toggle('active', skillShowAll);
    ['14d', '120d', 'alltime', '10r'].forEach(function(wid) {{ renderSkillTimeseries(wid); }});
}});

const skillSection = document.getElementById('skill-timeseries-section');
if (skillSection) {{
    skillSection.addEventListener('toggle', function() {{
        if (skillSection.open) {{
            ['14d', '120d', 'alltime', '10r'].forEach(function(wid) {{
                const c = document.getElementById('skill-timeseries-chart-' + wid);
                if (c) Plotly.Plots.resize(c);
            }});
        }}
    }});
}}
"""


_RUN_BROWSER_DEFAULT_MODELS = {100, 200, 201, 202}


def _run_browser_data(forecast_rows: list, obs_rows: list) -> dict:
    """Shape run-browser rows into one shared obs series plus a lightweight per-run manifest.

    The 24h obs window is shared and sliced client-side per selected run, rather than
    duplicated per run — adjacent runs' 24h windows overlap ~87% (3h cadence), so
    embedding a full copy per run would bloat the page for no reason.
    """
    obs_times, obs_temp, obs_dew = [], [], []
    for r in obs_rows:
        obs_times.append(r["timestamp"])
        t = _to_f(r["air_temp"])
        d = _to_f(r["dew_point"])
        obs_temp.append(round(t, 1) if t is not None else None)
        obs_dew.append(round(d, 1) if d is not None else None)

    runs: dict = {}
    for r in forecast_rows:
        issued = r["issued_at"]
        run = runs.setdefault(issued, {
            "issued_at": issued, "n_rows": 0, "n_scored": 0, "forecasts": {},
        })
        if r["value"] is not None:
            run["n_rows"] += 1
            if r["scored_at"] is not None:
                run["n_scored"] += 1
        lead = r["lead_hours"]
        if not 1 <= lead <= 24:
            continue
        by_model = run["forecasts"].setdefault(
            str(r["model_id"]), {
                "temperature": [None] * 24, "dewpoint": [None] * 24,
                "temperature_conf": [None] * 24, "dewpoint_conf": [None] * 24,
            }
        )
        val = _to_f(r["value"])
        by_model[r["variable"]][lead - 1] = round(val, 1) if val is not None else None
        conf = r["confidence"]
        by_model[r["variable"] + "_conf"][lead - 1] = round(conf, 3) if conf is not None else None

    run_list = sorted(runs.values(), key=lambda x: x["issued_at"])
    for run in run_list:
        run["fully_scored"] = run["n_rows"] > 0 and run["n_rows"] == run["n_scored"]
        del run["n_rows"]
        del run["n_scored"]

    default_index = next(
        (i for i in range(len(run_list) - 1, -1, -1) if run_list[i]["fully_scored"]),
        len(run_list) - 1 if run_list else 0,
    )

    return {
        "obs": {"times": obs_times, "temp": obs_temp, "dew": obs_dew},
        "runs": run_list,
        "default_index": default_index,
    }


def _run_browser_html(models: list) -> str:
    """Run picker (dropdown + prev/next) and per-model checkboxes for the forecast-vs-actual browser."""
    checkbox_items = [
        '<label class="run-browser-checkbox">'
        '<input type="checkbox" id="run-browser-obs-cb" checked>'
        '<span class="run-browser-swatch" style="background:#111"></span>Observed</label>'
    ]
    for m in models:
        checked = " checked" if m["id"] in _RUN_BROWSER_DEFAULT_MODELS else ""
        checkbox_items.append(
            f'<label class="run-browser-checkbox">'
            f'<input type="checkbox" class="run-browser-model-cb" data-model="{m["id"]}"{checked}>'
            f'<span class="run-browser-swatch" data-swatch-model="{m["id"]}"></span>'
            f'<span class="run-browser-mid">{m["id"]}</span>{m["name"]}</label>'
        )
    checkboxes_html = "\n    ".join(checkbox_items)
    return f"""
<div id="run-browser">
  <h3 class="obs-subhead">Forecast vs. Actual</h3>
  <p class="chart-legend-note">One forecast run at a time: dashed lines are observed
  temperature/dewpoint, solid lines are each source's forecast across that run's 24-hour
  window. Observed data starts 3 hours before the run to show the incoming trend. Default
  run is the most recent one fully scored. Last 14 days of runs browsable.</p>
  <div class="run-browser-nav">
    <button id="run-browser-prev" class="run-browser-nav-btn" title="previous run">&#9664;</button>
    <select id="run-browser-select"></select>
    <button id="run-browser-next" class="run-browser-nav-btn" title="next run">&#9654;</button>
  </div>
  <div class="run-browser-var-toggle">
    <label class="run-browser-checkbox"><input type="checkbox" id="run-browser-temp-cb" checked>Temperature</label>
    <label class="run-browser-checkbox"><input type="checkbox" id="run-browser-dew-cb" checked>Dew Point</label>
  </div>
  <div class="run-browser-model-actions">
    <button id="run-browser-select-all" class="run-browser-select-btn" type="button">Select all</button>
    <button id="run-browser-deselect-all" class="run-browser-select-btn" type="button">Deselect all</button>
  </div>
  <div class="run-browser-checkboxes">
    {checkboxes_html}
  </div>
  <div class="chart-container"><div id="run-browser-chart"></div></div>
  <h3 class="obs-subhead">Confidence by Lead Time</h3>
  <p class="chart-legend-note">Same run and model selection as above: how much each
  model's own scored history says to trust its forecast at each lead hour.</p>
  <div class="chart-container"><div id="run-browser-confidence-chart"></div></div>
</div>
"""


def _run_browser_js(data: dict, model_names: dict) -> str:
    """Plotly rendering, checkbox toggling, and run navigation for the forecast-vs-actual browser."""
    data_j = json.dumps(data)
    names_j = json.dumps(model_names)
    return f"""const _runBrowserData = {data_j};
const _runBrowserModelNames = {names_j};
const RUN_BROWSER_KNOWN_COLORS = {{'100': '#1f77b4', '200': '#ff7f0e', '201': '#2ca02c', '202': '#9467bd'}};
const RUN_BROWSER_EXTRA_PALETTE = ['#d62728','#8c564b','#e377c2','#7f7f7f','#bcbd22','#17becf','#aec7e8','#ffbb78','#98df8a','#ff9896','#c5b0d5','#c49c94','#f7b6d2','#c7c7c7','#dbdb8d','#9edae5'];
let runBrowserIndex = {data["default_index"]};

function runBrowserModelColor(mid) {{
    if (RUN_BROWSER_KNOWN_COLORS[mid]) return RUN_BROWSER_KNOWN_COLORS[mid];
    const idx = Object.keys(_runBrowserModelNames).sort(function(a, b) {{ return a - b; }}).indexOf(mid);
    return RUN_BROWSER_EXTRA_PALETTE[idx % RUN_BROWSER_EXTRA_PALETTE.length];
}}

// checkbox row doubles as the legend (color swatch matches its trace color) so the
// in-plot Plotly legend can be dropped entirely — that legend was competing with the
// chart for a fixed total height, shrinking the plot area every time more models (and
// so more legend rows) got checked on.
document.querySelectorAll('.run-browser-swatch[data-swatch-model]').forEach(function(el) {{
    el.style.background = runBrowserModelColor(el.dataset.swatchModel);
}});

function populateRunBrowserSelect() {{
    const sel = document.getElementById('run-browser-select');
    sel.innerHTML = '';
    _runBrowserData.runs.forEach(function(run, i) {{
        const d = new Date(run.issued_at * 1000);
        const label = d.toLocaleString('en-US', {{
            month: 'short', day: 'numeric', hour: 'numeric', minute: '2-digit', hour12: false
        }}) + (run.fully_scored ? '' : ' (scoring)');
        const opt = document.createElement('option');
        opt.value = i;
        opt.textContent = label;
        sel.appendChild(opt);
    }});
    sel.value = runBrowserIndex;
}}

// grey vertical marker at the latest available observation time, shown only when
// that time falls inside the chart's own window (i.e. the window spans "now").
function lastObsShapes(winStartSec, winEndSec) {{
    const obs = _runBrowserData.obs;
    if (!obs.times.length) return [];
    const lastObsSec = obs.times[obs.times.length - 1];
    if (lastObsSec < winStartSec || lastObsSec > winEndSec) return [];
    const x = lastObsSec * 1000;
    return [{{
        type: 'line', xref: 'x', yref: 'paper', x0: x, x1: x, y0: 0, y1: 1,
        line: {{color: '#888', width: 1, dash: 'dot'}},
    }}];
}}

const RUN_BROWSER_LEAD_IN_SEC = 3 * 3600;
let rbBaseShapes = [];
let rbConfBaseShapes = [];

function renderRunBrowser() {{
    const run = _runBrowserData.runs[runBrowserIndex];
    if (!run) return;
    document.getElementById('run-browser-select').value = runBrowserIndex;

    const showTemp = document.getElementById('run-browser-temp-cb').checked;
    const showDew = document.getElementById('run-browser-dew-cb').checked;
    const traces = [];
    const winStart = run.issued_at - RUN_BROWSER_LEAD_IN_SEC;
    const winEnd = run.issued_at + 24 * 3600;
    const obs = _runBrowserData.obs;
    const obsIdx = [];
    for (let i = 0; i < obs.times.length; i++) {{
        if (obs.times[i] >= winStart && obs.times[i] <= winEnd) obsIdx.push(i);
    }}
    const obsTimes = obsIdx.map(function(i) {{ return obs.times[i] * 1000; }});

    if (document.getElementById('run-browser-obs-cb').checked) {{
        if (showTemp) traces.push({{
            x: obsTimes, y: obsIdx.map(function(i) {{ return obs.temp[i]; }}),
            name: 'observed (temp)', type: 'scatter', mode: 'lines', connectgaps: false,
            line: {{color: '#111', width: 2, dash: 'dash', shape: 'spline', smoothing: 0.3}},
        }});
        if (showDew) traces.push({{
            x: obsTimes, y: obsIdx.map(function(i) {{ return obs.dew[i]; }}),
            name: 'observed (dew)', type: 'scatter', mode: 'lines', connectgaps: false,
            line: {{color: '#111', width: 2, dash: 'dot', shape: 'spline', smoothing: 0.3}},
        }});
    }}

    const leadTimes = [];
    for (let lead = 1; lead <= 24; lead++) leadTimes.push((run.issued_at + lead * 3600) * 1000);

    // every model's forecast is computed from the same latest-obs snapshot at issued_at
    // (see cmd_forecast in barogram.py) — it's never persisted as its own row, but we
    // already have the continuous obs series, so pull the same anchor back out and
    // prepend it to each model's series. Gives every forecast line a shared starting
    // point that connects straight back to the observed line instead of floating loose.
    // station outages (Tempest offline for hours) leave "latest obs" far older than
    // issued_at — anchoring to it would stretch every forecast line (and the plot's
    // autoranged x-axis) back across the whole outage. Drop the anchor once it's
    // stale beyond one obs cadence's worth of slack (5 min cadence, so 30 min is
    // already several missed reports, not just normal lag).
    const ANCHOR_STALENESS_LIMIT_SEC = 30 * 60;
    let anchorIdx = -1;
    for (let i = 0; i < obs.times.length; i++) {{
        if (obs.times[i] <= run.issued_at) anchorIdx = i; else break;
    }}
    if (anchorIdx >= 0 && run.issued_at - obs.times[anchorIdx] > ANCHOR_STALENESS_LIMIT_SEC) {{
        anchorIdx = -1;
    }}
    const anchorTime = anchorIdx >= 0 ? obs.times[anchorIdx] * 1000 : null;
    const anchorTemp = anchorIdx >= 0 ? obs.temp[anchorIdx] : null;
    const anchorDew = anchorIdx >= 0 ? obs.dew[anchorIdx] : null;

    // drop nulls rather than lean on connectgaps: a legacy run only ever wrote leads
    // 6/12/18/24, so 20 of 24 slots are null and connectgaps:false leaves every real
    // point isolated with nothing to connect to. Compacting first connects the real
    // points directly, sparse-legacy or full-hourly alike.
    function compact(values, anchorVal) {{
        const xs = [], ys = [];
        if (anchorTime !== null && anchorVal !== null && anchorVal !== undefined) {{
            xs.push(anchorTime); ys.push(anchorVal);
        }}
        for (let i = 0; i < values.length; i++) {{
            if (values[i] !== null) {{ xs.push(leadTimes[i]); ys.push(values[i]); }}
        }}
        return {{x: xs, y: ys}};
    }}

    document.querySelectorAll('.run-browser-model-cb:checked').forEach(function(cb) {{
        const mid = cb.dataset.model;
        const fc = run.forecasts[mid];
        if (!fc) return;
        const color = runBrowserModelColor(mid);
        if (showTemp) {{
            const s = compact(fc.temperature, anchorTemp);
            traces.push({{
                x: s.x, y: s.y, name: mid + ' (temp)', type: 'scatter',
                mode: 'lines+markers', line: {{color: color, shape: 'spline', smoothing: 0.3}},
                marker: {{size: 5, color: color}},
            }});
        }}
        if (showDew) {{
            const s = compact(fc.dewpoint, anchorDew);
            traces.push({{
                x: s.x, y: s.y, name: mid + ' (dew)', type: 'scatter',
                mode: 'lines+markers', line: {{color: color, shape: 'spline', smoothing: 0.3}},
                marker: {{size: 5, color: color, symbol: 'diamond'}},
            }});
        }}
    }});

    rbBaseShapes = lastObsShapes(winStart, winEnd);
    return Plotly.react('run-browser-chart', traces, {{
        height: 420, margin: {{t: 20, b: 40, l: 50, r: 16}},
        font: {{color: plotBg().font}}, paper_bgcolor: plotBg().paper, plot_bgcolor: plotBg().plot,
        yaxis: {{title: '\\u00b0F'}},
        xaxis: {{
            type: 'date', range: [winStart * 1000, winEnd * 1000],
            showspikes: true, spikemode: 'across', spikesnap: 'cursor',
            spikethickness: 1, spikedash: 'solid', spikecolor: '#888',
        }},
        hovermode: 'x',
        showlegend: false,
        shapes: rbBaseShapes,
    }}, {{responsive: true}});
}}

function renderRunBrowserConfidence() {{
    const run = _runBrowserData.runs[runBrowserIndex];
    if (!run) return;

    const showTemp = document.getElementById('run-browser-temp-cb').checked;
    const showDew = document.getElementById('run-browser-dew-cb').checked;
    const leadTimes = [];
    for (let lead = 1; lead <= 24; lead++) leadTimes.push((run.issued_at + lead * 3600) * 1000);
    const winStart = run.issued_at - RUN_BROWSER_LEAD_IN_SEC;
    const winEnd = run.issued_at + 24 * 3600;

    function compactConf(values) {{
        const xs = [], ys = [];
        for (let i = 0; i < values.length; i++) {{
            if (values[i] !== null) {{ xs.push(leadTimes[i]); ys.push(values[i] * 100); }}
        }}
        return {{x: xs, y: ys}};
    }}

    const traces = [];
    document.querySelectorAll('.run-browser-model-cb:checked').forEach(function(cb) {{
        const mid = cb.dataset.model;
        const fc = run.forecasts[mid];
        if (!fc) return;
        const color = runBrowserModelColor(mid);
        if (showTemp) {{
            const s = compactConf(fc.temperature_conf);
            traces.push({{
                x: s.x, y: s.y, name: mid + ' (temp)', type: 'scatter',
                mode: 'lines+markers', line: {{color: color, shape: 'spline', smoothing: 0.3}},
                marker: {{size: 5, color: color}},
            }});
        }}
        if (showDew) {{
            const s = compactConf(fc.dewpoint_conf);
            traces.push({{
                x: s.x, y: s.y, name: mid + ' (dew)', type: 'scatter',
                mode: 'lines+markers', line: {{color: color, shape: 'spline', smoothing: 0.3}},
                marker: {{size: 5, color: color, symbol: 'diamond'}},
            }});
        }}
    }});

    rbConfBaseShapes = lastObsShapes(winStart, winEnd);
    return Plotly.react('run-browser-confidence-chart', traces, {{
        height: 300, margin: {{t: 20, b: 40, l: 50, r: 16}},
        font: {{color: plotBg().font}}, paper_bgcolor: plotBg().paper, plot_bgcolor: plotBg().plot,
        yaxis: {{title: 'confidence %', range: [0, 100]}},
        xaxis: {{
            type: 'date', range: [winStart * 1000, winEnd * 1000],
            showspikes: true, spikemode: 'across', spikesnap: 'cursor',
            spikethickness: 1, spikedash: 'solid', spikecolor: '#888',
        }},
        hovermode: 'x',
        showlegend: false,
        shapes: rbConfBaseShapes,
    }}, {{responsive: true}});
}}

function renderRunBrowserAll() {{
    return Promise.all([renderRunBrowser(), renderRunBrowserConfidence()]);
}}

function withHoverLine(baseShapes, hoverX) {{
    return baseShapes.concat([{{
        type: 'line', xref: 'x', yref: 'paper', x0: hoverX, x1: hoverX, y0: 0, y1: 1,
        line: {{color: '#888', width: 1, dash: 'solid'}},
    }}]);
}}

populateRunBrowserSelect();
// Plotly.react's own promise only resolves once each div is a real, fully-initialized
// graph div — waiting on it (rather than calling .on right after renderRunBrowserAll())
// avoids a race where .on isn't attached yet on the very first render.
renderRunBrowserAll().then(function() {{
    // mirror only the vertical hover line onto the other chart, not Plotly's own
    // value tooltip — that stays local to whichever chart the cursor is actually on.
    document.getElementById('run-browser-chart').on('plotly_hover', function(evt) {{
        if (!evt.points || !evt.points.length) return;
        Plotly.relayout('run-browser-confidence-chart', {{shapes: withHoverLine(rbConfBaseShapes, evt.points[0].x)}});
    }});
    document.getElementById('run-browser-chart').on('plotly_unhover', function() {{
        Plotly.relayout('run-browser-confidence-chart', {{shapes: rbConfBaseShapes}});
    }});
    document.getElementById('run-browser-confidence-chart').on('plotly_hover', function(evt) {{
        if (!evt.points || !evt.points.length) return;
        Plotly.relayout('run-browser-chart', {{shapes: withHoverLine(rbBaseShapes, evt.points[0].x)}});
    }});
    document.getElementById('run-browser-confidence-chart').on('plotly_unhover', function() {{
        Plotly.relayout('run-browser-chart', {{shapes: rbBaseShapes}});
    }});
}});

document.getElementById('run-browser-select').addEventListener('change', function() {{
    runBrowserIndex = parseInt(this.value, 10);
    renderRunBrowserAll();
}});
document.getElementById('run-browser-prev').addEventListener('click', function() {{
    if (runBrowserIndex > 0) {{ runBrowserIndex--; renderRunBrowserAll(); }}
}});
document.getElementById('run-browser-next').addEventListener('click', function() {{
    if (runBrowserIndex < _runBrowserData.runs.length - 1) {{ runBrowserIndex++; renderRunBrowserAll(); }}
}});
document.querySelectorAll(
    '#run-browser-obs-cb, #run-browser-temp-cb, #run-browser-dew-cb, .run-browser-model-cb'
).forEach(function(cb) {{
    cb.addEventListener('change', renderRunBrowserAll);
}});
document.getElementById('run-browser-select-all').addEventListener('click', function() {{
    document.querySelectorAll('.run-browser-model-cb').forEach(function(cb) {{ cb.checked = true; }});
    renderRunBrowserAll();
}});
document.getElementById('run-browser-deselect-all').addEventListener('click', function() {{
    document.querySelectorAll('.run-browser-model-cb').forEach(function(cb) {{ cb.checked = false; }});
    renderRunBrowserAll();
}});
"""


def _accuracy_table_js() -> str:
    return """\
function updateAccTable(varName) {
    document.querySelectorAll('.acc-lead-table').forEach(function(table) {
        var byLead = {};
        table.querySelectorAll('.acc-cell').forEach(function(cell) {
            var raw = cell.getAttribute('data-' + varName);
            if (raw === '' || raw === null) {
                cell.textContent = '\u2014';
                cell.className = 'acc-cell';
                return;
            }
            var pct = parseFloat(raw);
            cell.textContent = pct.toFixed(0) + '%';
            var suffix = pct >= 80 ? ' acc-excellent' : pct >= 50 ? ' acc-high' : pct >= 20 ? ' acc-mid' : pct >= 0 ? ' acc-ok' : pct >= -50 ? ' acc-low' : ' acc-poor';
            cell.className = 'acc-cell' + suffix;
            var lead = cell.getAttribute('data-lead');
            (byLead[lead] = byLead[lead] || []).push(
                { cell: cell, val: pct, mid: parseInt(cell.getAttribute('data-mid'), 10) }
            );
        });
        // highlight best (any model) and worst (excluding models 1 and 2) per lead column
        var eps = 1e-9;
        Object.keys(byLead).forEach(function(lead) {
            var cells = byLead[lead];
            var maxVal = -Infinity, minVal = Infinity;
            cells.forEach(function(c) {
                if (c.val > maxVal) maxVal = c.val;
                if (c.mid !== 1 && c.mid !== 2 && c.val < minVal) minVal = c.val;
            });
            cells.forEach(function(c) {
                if (Math.abs(c.val - maxVal) < eps) c.cell.classList.add('acc-best');
            });
            if (minVal < maxVal - eps) {
                cells.forEach(function(c) {
                    if (c.mid !== 1 && c.mid !== 2 && Math.abs(c.val - minVal) < eps) {
                        c.cell.classList.add('acc-worst');
                    }
                });
            }
        });
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
    ['14d', '120d', 'alltime', '10r'].forEach(function(w) {
        var el = document.getElementById('skill-timeseries-' + w);
        if (el) el.style.display = (w === win) ? '' : 'none';
        el = document.getElementById('acc-overall-' + w);
        if (el) el.style.display = (w === win) ? '' : 'none';
        el = document.getElementById('acc-lead-' + w);
        if (el) el.style.display = (w === win) ? '' : 'none';
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

var accInitBtn = document.querySelector('.acc-filter-btn.active');
updateAccTable(accInitBtn ? accInitBtn.dataset.var : 'temperature');
"""


def _recent_misses_html(rows: list) -> str:
    if not rows:
        return '<p class="muted">No scored forecasts in the last 14 days.</p>'

    # rows are pre-sorted by model then mae desc; group into one table per model
    groups: dict = {}
    for row in rows:
        groups.setdefault(row["model"], []).append(row)

    sections = []
    for model, model_rows in groups.items():
        body_rows = []
        for row in model_rows:
            var = row["variable"]
            val = row["value"]
            obs = row["observed"]
            err = row["error"]
            if var in ("temperature", "dewpoint"):
                pred_str = f"{_to_f(val):.1f}\u00b0F" if val is not None else "\u2014"
                obs_str = f"{_to_f(obs):.1f}\u00b0F" if obs is not None else "\u2014"
                err_disp = _diff_to_f(err)
                err_thresh = 3
            else:
                pred_str = f"{val:.1f} hPa" if val is not None else "\u2014"
                obs_str = f"{obs:.1f} hPa" if obs is not None else "\u2014"
                err_disp = err
                err_thresh = 3
            if err_disp is not None:
                sign = "+" if err_disp >= 0 else ""
                err_cls = "mae-worse" if abs(err_disp) >= err_thresh else ""
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
        table = (
            '<table class="obs-history-table recent-misses-table">'
            '<thead><tr>'
            '<th>Variable</th><th>Lead</th><th>Valid</th>'
            '<th style="text-align:right">Predicted</th>'
            '<th style="text-align:right">Observed</th>'
            '<th style="text-align:right">Error</th>'
            '</tr></thead><tbody>'
            + "".join(body_rows)
            + '</tbody></table>'
        )
        sections.append(
            f'<details class="collapsible-section" style="margin-top:12px">'
            f'<summary>{model}</summary>'
            f'<div class="table-scroll" style="margin-top:8px">{table}</div>'
            f'</details>'
        )
    return "".join(sections)


def _write_fragment(html: str, out_dir: Path) -> None:
    css_start = html.index("<style>\n") + len("<style>\n")
    css_end = html.index("\n</style>")
    css = html[css_start:css_end]

    # scope bare-element selectors so they don't bleed into the host site;
    # capture leading whitespace so rules inside @media blocks are also rewritten
    css = re.sub(r"(?m)^( *)body \{", r"\1.barogram {", css)
    css = re.sub(r"(?m)^( *)header h1 \{", r"\1.barogram-header h1 {", css)
    css = re.sub(r"(?m)^( *)header \{", r"\1.barogram-header {", css)
    css = re.sub(r"(?m)^( *)h2 \{", r"\1.barogram h2 {", css)
    css = re.sub(r"(?m)^( *)h3 \{", r"\1.barogram h3 {", css)
    # .barogram inherits host page background; strip hardcoded value so the
    # host site background always shows through
    css = re.sub(r"(?m)^(    color: #1a1a1a;\n)    background: #f5f5f5;\n(    padding:)", r"\1\2", css)
    # .barogram-header must be opaque (sticky), but should match host page bg
    css = re.sub(r"(?m)^(    z-index: 100;\n)    background: #f5f5f5;\n(    display: flex;)", r"\1    background: var(--bg, #f5f5f5);\n\2", css)

    body_start = html.index("<body>\n") + len("<body>\n")
    script_anchor = '\n<script src="https://cdn.jsdelivr.net/'
    body_end = html.index(script_anchor)
    body_html = html[body_start:body_end]
    body_html = re.sub(r"<header\b[^>]*>", '<div class="barogram-header">', body_html, count=1)
    body_html = body_html.replace("</header>", "</div>", 1)
    body_html = f'<div class="barogram" id="barogram-top">\n{body_html}\n</div>'

    scripts_start = html.index(script_anchor) + 1
    scripts_end = html.index("\n</body>")
    scripts_html = html[scripts_start:scripts_end]

    def _strip_trailing(s: str) -> str:
        return "\n".join(line.rstrip() for line in s.splitlines()) + "\n"

    (out_dir / "barogram-style.css").write_text(_strip_trailing(css), encoding="utf-8")
    (out_dir / "barogram-body.html").write_text(_strip_trailing(body_html), encoding="utf-8")
    (out_dir / "barogram-scripts.html").write_text(_strip_trailing(scripts_html), encoding="utf-8")


def generate(
    conn_in: sqlite3.Connection,
    conn_out: sqlite3.Connection,
    output_path: Path,
    machine_id: str | None = None,
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

    tempest = db.latest_tempest_obs(conn_in)
    nws = db.latest_nws_obs(conn_in)
    tempest_history = db.recent_tempest_obs(conn_in)
    nws_history = db.recent_nws_obs(conn_in)
    nws_filled, nws_fallback_ts = _fill_nws_gaps(nws, nws_history)

    loc = db.tempest_station_location(conn_in)
    nws_forecast = _fetch_nws_forecast(*loc) if loc else {}

    now = int(time.time())
    today = date.today()
    midnight_7d_ago = int(
        datetime(today.year, today.month, today.day, tzinfo=timezone.utc).timestamp()
    ) - 7 * 86400
    _scores_10 = db.score_summary_last_n_runs_multi(conn_out, [10])[10]
    members_10 = [r for r in _scores_10 if r["member_id"] > 0]
    member_models = {r["model"] for r in members_10}
    all_time_summary = [r for r in db.score_summary(conn_out) if r["member_id"] == 0]
    bias_ts_rows = [
        r for r in db.bias_timeseries(conn_out, since=midnight_7d_ago)
        if r["lead_hours"] in _KEY_LEADS
    ]
    diurnal_rows = db.diurnal_errors(conn_out)
    weight_rows = db.all_weights_with_members(conn_out)
    all_members = db.all_members_for_ensemble_models(conn_out)
    ext_corrected_mae_rows = db.external_corrected_source_mae(conn_out)
    misses_rows = db.recent_misses(conn_out, now - 14 * 86400)
    _14d = now - 14 * 86400
    _120d = now - 120 * 86400
    _acc = db.accuracy_windows(conn_out, [_14d, _120d, 0])
    acc_rows_14d = [r for r in _acc[_14d] if r["lead_hours"] in _KEY_LEADS]
    acc_rows_120d = [r for r in _acc[_120d] if r["lead_hours"] in _KEY_LEADS]
    acc_rows_alltime = [r for r in _acc[0] if r["lead_hours"] in _KEY_LEADS]
    acc_rows_10r = [r for r in db.accuracy_by_lead(conn_out, 10) if r["lead_hours"] in _KEY_LEADS]
    acc_count_10r = db.accuracy_run_count_last_n(conn_out, 10)
    _skill_ts = db.skill_timeseries_multi(conn_out, [_14d, _120d, 0])
    _skill_ts_10r = db.skill_timeseries_last_n_runs(conn_out, 10)
    skill_ts_html = _skill_timeseries_html()
    skill_ts_js = _skill_timeseries_js(
        _skill_timeseries_data(_skill_ts[_14d]),
        _skill_timeseries_data(_skill_ts[_120d]),
        _skill_timeseries_data(_skill_ts[0]),
        _skill_timeseries_data(_skill_ts_10r),
    )
    all_models = db.list_models(conn_out)
    run_browser_forecast_rows = db.run_browser_forecasts(conn_out, _14d)
    run_browser_obs_rows = db.run_browser_obs(conn_in, _14d, now)
    run_browser_data = _run_browser_data(run_browser_forecast_rows, run_browser_obs_rows)
    run_browser_html = _run_browser_html(all_models)
    run_browser_js = _run_browser_js(
        run_browser_data, {str(m["id"]): m["name"] for m in all_models}
    )
    _counts = db.accuracy_run_count_multi(conn_out, [_14d, _120d, 0])
    acc_count_14d = _counts[_14d]
    acc_count_120d = _counts[_120d]
    acc_count_alltime = _counts[0]

    lead_times = sorted({row["lead_hours"] for row in mean_rows})
    bias_ts = _bias_timeseries_data(bias_ts_rows)
    heatmap = _heatmap_data(all_time_summary)
    diurnal = _diurnal_data(diurnal_rows)
    recent_misses_html = _recent_misses_html(misses_rows)
    # fixed column set regardless of what's scored yet — a lead with no data
    # shows a dash instead of the column disappearing (e.g. a brand-new lead
    # that hasn't reached its valid_at time to be scored)
    acc_lead_times = sorted(_KEY_LEADS)
    _acc_windows = [
        ("14d", acc_rows_14d, acc_count_14d, "14 days"),
        ("120d", acc_rows_120d, acc_count_120d, "120 days"),
        ("alltime", acc_rows_alltime, acc_count_alltime, "all time"),
        ("10r", acc_rows_10r, acc_count_10r, "last 10 runs"),
    ]
    overall_parts, lead_parts = [], []
    for wid, rows, n_runs, label in _acc_windows:
        hidden = ' style="display:none"' if wid != "14d" else ""
        if wid == "10r":
            run_note = f'<p class="chart-legend-note acc-run-note">last {n_runs} runs</p>'
        else:
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
            f'{_accuracy_lead_table_html(rows, acc_lead_times, member_models if wid == "10r" else None)}'
            f'</div>'
        )
    overall_accuracy_html = "".join(overall_parts)
    acc_lead_table_html = "".join(lead_parts)
    conf_rows = [r for r in db.confidence_by_lead(conn_out, 14) if r["lead_hours"] in _KEY_LEADS]
    conf_run_count = db.accuracy_run_count_last_n(conn_out, 14)
    conf_lead_table_html = _confidence_lead_table_html(conf_rows, acc_lead_times)
    generated_at = fmt.ts(now)
    machine_label = f'<span class="machine-id">({machine_id})</span>' if machine_id else ""
    _lf = db.get_metadata(conn_out, "last_forecast")
    _lt = db.get_metadata(conn_out, "last_tune")
    last_forecast_str = fmt.ts(int(_lf)) if _lf else "\u2014"
    last_forecast_epoch = int(_lf) if _lf else 0
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
            f'and may have stale data: {model_list}.'
            f'</div>'
        )

    # observation staleness: last Tempest reading >2h old means every model is
    # forecasting off dead input even if the forecast job itself ran on schedule
    obs_age = now - tempest["timestamp"] if tempest else None
    obs_staleness_banner = ""
    if obs_age is None or obs_age > 7200:
        if obs_age is None:
            age_str = "no readings found"
        else:
            age_str = f"{obs_age / 3600:.1f} hours old"
        obs_staleness_banner = (
            f'<div class="stale-banner">'
            f'<strong>Warning:</strong> latest Tempest observation is {age_str} '
            f'Forecasts may be based on stale input regardless of when they were generated.'
            f'</div>'
        )

    zambretti = pressure_tendency.zambretti_text(conn_in, elevation_m, now)
    zambretti_panel = _zambretti_panel_html(zambretti)
    tempest_card = _conditions_card("Tempest", tempest, elevation_m)
    nws_card = _conditions_card("NWS", nws_filled, fallback_ts=nws_fallback_ts)
    ensemble_section = _ensemble_forecast_section(mean_rows, tempest, elevation_m, nws_forecast)
    obs_section = _obs_history_section(tempest_history, nws_history, elevation_m)
    tempest_rows = [_tempest_obs_row(r, elevation_m) for r in tempest_history]
    nws_rows = [_nws_obs_row(r) for r in nws_history]

    ext_corrected_html = _external_corrected_source_weights_html(ext_corrected_mae_rows)
    weights_section = _weights_section_html(weight_rows, all_members, ext_corrected_html)
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
        for lt in sorted(_KEY_LEADS)
    )
    heatmap_filter_btns = "".join(
        f'<button class="heatmap-filter-btn{" active" if i == 0 else ""}" data-var="{v}">{lbl}</button>'
        for i, (v, lbl) in enumerate(_var_btns)
    )
    diurnal_filter_btns = "".join(
        f'<button class="diurnal-filter-btn{" active" if i == 0 else ""}" data-var="{v}">{lbl}</button>'
        for i, (v, lbl) in enumerate(_var_btns)
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
<div class="container" id="barogram-top">

<header>
  <div class="header-top">
    <h1>barogram</h1>
    <div class="generated">
      <span>generated {generated_at}</span>
      {machine_label}
      <span>last forecast: {last_forecast_str}</span>
      <span>last tune: {last_tune_str}</span>
    </div>
    <nav class="jump-nav">
      <a href="#">Top</a>
      <a href="#verification">Verification</a>
      <a href="#analysis">Analysis</a>
      <a href="#weights">Weights</a>
    </nav>
  </div>
</header>
{staleness_banner}
{obs_staleness_banner}
<div id="stale-age-banner" class="stale-banner stale-age-banner" style="display:none">
  <strong>Heads up:</strong> the latest forecast is more than 6 hours old and may not reflect current conditions.
</div>
<section class="section" id="about">
  <p>Barogram is a pet forecast ensemble, a small collection of models I run for fun and to learn more about how forecasting actually works. Every three hours, they look at the latest readings from a backyard Tempest weather station in the Twin Cities, MN and a nearby NWS airport station, then each independently predict local temperature, dew point, pressure, and precipitation probability for the next 6 to 24 hours.</p>
  <p style="margin-top:10px">After each run, the previous predictions get scored against what actually happened. Models that beat a naive baseline earn more weight in the ensemble&#x2019;s combined output; models that don&#x2019;t are demoted toward a floor. The base models use simple approaches and none of them are impressive on their own. The ensemble is what makes them useful.</p>
  <p style="margin-top:10px">These forecasts are specific to that one station. This is a personal project running on data from my own equipment; it says nothing about conditions where you are.</p>
</section>
<section class="section" id="conditions">
  <h2>Latest conditions in the Twin Cities</h2>
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
  <p class="chart-legend-note">Skill score vs. climatological mean, averaged across temperature, dewpoint, and pressure (plus Precip Prob BSS once enough rain events have been observed). 100% = perfect · 0% = matches climatological mean · negative = worse than climatological mean.</p>
  <div class="table-scroll">{overall_accuracy_html}</div>
  {skill_ts_html}
  {run_browser_html}
  <details class="collapsible-section">
    <summary class="obs-subhead">Recent Misses (14 days)</summary>
    <p class="chart-legend-note">Largest forecast errors per source over the last 14 days, sorted biggest miss first within each group.</p>
    <div class="table-scroll">{recent_misses_html}</div>
  </details>
  <h3 class="obs-subhead">Forecast Skill by Lead Time</h3>
  <p class="chart-legend-note">Skill score vs. climatological mean at each lead time for the selected variable. Negative = worse than climatology.</p>
  <div class="mae-filter-bar">{acc_filter_btns}</div>
  <div class="table-scroll">{acc_lead_table_html}</div>
  <h3 class="obs-subhead">Confidence by Lead Time (14-Run Average)</h3>
  <p class="chart-legend-note">Average forecast confidence at each lead time over the last {conf_run_count} runs, for the selected variable.</p>
  <div class="table-scroll">{conf_lead_table_html}</div>
</section>

<section class="section analysis-section" id="analysis">
  <h2>Model Analysis</h2>

  <details class="collapsible-section">
    <summary class="obs-subhead">Bias Over Time</summary>
    <div class="mae-filter-bar">{bias_filter_btns}</div>
    <div class="mae-charts-grid">
      {bias_chart_divs}
    </div>
  </details>

  <h3 class="obs-subhead">Score Heatmap</h3>
  <div class="mae-filter-bar">{heatmap_filter_btns}</div>
  <div class="chart-container"><div id="heatmap-chart"></div></div>

  <h3 class="obs-subhead">Diurnal Stratification</h3>
  <div class="mae-filter-bar">
    {diurnal_filter_btns}
    <button id="diurnal-mode-btn" class="mae-raw-btn">Show MAE</button>
  </div>
  <div class="chart-container"><div id="diurnal-chart"></div></div>

</section>

<section class="section" id="weights">
  <h2>Weights</h2>
  <p class="learnings-intro">Skill-score weights computed by <code>barogram tune</code>. Each member is scored by how much it improves over a naive baseline; members that beat the baseline earn proportional weight, and those that don&#x2019;t are floored or subfloored. Sector columns show how trust shifts across time-of-day.</p>
  {weights_section}
</section>

{obs_section}

</div>
<script src="https://cdn.jsdelivr.net/npm/plotly.js-dist-min@2/plotly.min.js"></script>
<script>
const LAST_FORECAST = {last_forecast_epoch};
if (LAST_FORECAST && Date.now() / 1000 - LAST_FORECAST > 6 * 3600) {{
  document.getElementById('stale-age-banner').style.display = '';
}}
function plotBg() {{
    return {{ paper: 'white', plot: '#fafafa', font: '#333333', zero: '#dddddd' }};
}}
{_obs_history_js(tempest_rows, nws_rows)}
{_member_detail_js(members_10)}
{_bias_timeseries_js(bias_ts)}
{_heatmap_js(heatmap)}
{_diurnal_js(diurnal)}
{_accuracy_table_js()}
{skill_ts_js}
{run_browser_js}
</script>
</body>
</html>
"""

    output_path.write_text(html, encoding="utf-8")
    _write_fragment(html, output_path.parent)
