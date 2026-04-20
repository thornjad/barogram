import json
import sqlite3
import time
import urllib.request
from datetime import date, datetime, timezone
from pathlib import Path

import db
import fmt
import models.pressure_tendency as pressure_tendency

VARIABLES = ["temperature", "dewpoint", "pressure", "wind_speed"]

_VARIABLE_LABEL = {
    "temperature": "Temperature",
    "dewpoint": "Dew Point",
    "pressure": "Pressure",
    "wind_speed": "Wind Speed",
}

_UNIT = {
    "temperature": "\u00b0F",
    "dewpoint": "\u00b0F",
    "pressure": "hPa",
    "wind_speed": "mph",
}


def _to_f(c):
    return None if c is None else c * 9 / 5 + 32


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
            ws_str = period.get("windSpeed") or ""
            wind_ms = None
            if ws_str:
                nums = [
                    float(p) for p in ws_str.replace(" to ", " ").split()
                    if p.replace(".", "").isdigit()
                ]
                if nums:
                    wind_ms = sum(nums) / len(nums) * 0.44704  # avg mph → m/s
            result[ts] = {"temperature": temp_c, "dewpoint": dew_c, "wind_speed": wind_ms}
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
    display: flex;
    justify-content: space-between;
    align-items: baseline;
    margin-bottom: 24px;
    padding-bottom: 12px;
    border-bottom: 2px solid #1a1a1a;
}
header h1 { font-size: 22px; letter-spacing: -0.5px; }
.generated { font-size: 12px; color: #666; display: flex; flex-direction: column; align-items: flex-end; gap: 2px; }
.section { margin-bottom: 32px; }
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
.diurnal-filter-btn, .error-dist-var-btn, .error-dist-lead-btn { padding: 4px 12px; font-size: 12px; font-family: inherit; background: #fff; border: 1px solid #ccc; border-radius: 3px; cursor: pointer; color: #444; }
.mae-filter-btn:hover, .fcst-filter-btn:hover,
.bias-filter-btn:hover, .lead-skill-filter-btn:hover, .heatmap-filter-btn:hover,
.diurnal-filter-btn:hover, .error-dist-var-btn:hover, .error-dist-lead-btn:hover { background: #f0f0f0; }
.mae-filter-btn.active, .fcst-filter-btn.active,
.bias-filter-btn.active, .lead-skill-filter-btn.active, .heatmap-filter-btn.active,
.diurnal-filter-btn.active, .error-dist-var-btn.active, .error-dist-lead-btn.active { background: #1a1a1a; color: #fff; border-color: #1a1a1a; }
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
.obs-history-table {
    width: 100%;
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
    grid-template-columns: 1fr 1fr;
    gap: 16px;
    margin-bottom: 20px;
}
.verification-primary { margin-bottom: 20px; }
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
.ensemble-row th, .ensemble-row td { background: #f8faff; }
.model-runs { display: flex; flex-direction: column; gap: 20px; }
.model-run-card { background: #fff; border: 1px solid #ddd; border-radius: 4px; overflow: hidden; }
.model-run-header { display: flex; align-items: baseline; gap: 10px; padding: 10px 16px; background: #f9f9f9; border-bottom: 1px solid #eee; }
.model-run-header strong { font-size: 14px; }
.base-badge, .ensemble-badge, .baseline-badge { font-size: 11px; padding: 1px 6px; border-radius: 3px; font-weight: 600; letter-spacing: 0.03em; text-transform: uppercase; }
.base-badge { background: #e8f4e8; color: #2d6a2d; }
.ensemble-badge { background: #eff4ff; color: #3b5bdb; }
.baseline-badge { background: #ece9e0; color: #aaa; }
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
.weights-section { display: grid; grid-template-columns: repeat(auto-fill, minmax(240px, 1fr)); gap: 16px; margin-top: 12px; align-items: start; }
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
@media (max-width: 600px) {
    .conditions-grid, .charts-grid, .verification-windows { grid-template-columns: 1fr; }
}
.forecast-cards { display:flex; gap:12px; overflow-x:auto; padding-bottom:4px; }
.forecast-card { flex:0 0 auto; min-width:110px; background:#fff; border:1px solid #ddd; border-radius:8px; padding:14px 16px; text-align:center; }
.forecast-card.now-card { border-color:#b0c4de; background:#f5f8fc; }
.fcst-label { font-size:11px; font-weight:700; text-transform:uppercase; letter-spacing:.06em; color:#888; margin-bottom:8px; }
.fcst-temp { font-size:32px; font-weight:700; color:#1a1a1a; line-height:1; }
.fcst-temp-spread { font-size:11px; color:#aaa; margin-top:2px; margin-bottom:10px; }
.fcst-details { font-size:12px; color:#555; line-height:1.8; text-align:left; }
.fcst-details .detail-label { color:#999; }
.fcst-no-data { color:#bbb; font-size:13px; }
.fcst-ref { font-size:11px; color:#999; margin-top:8px; padding-top:6px; border-top:1px solid #f0f0f0; text-align:left; line-height:1.8; }
.fcst-ref-lbl { font-size:10px; font-weight:700; text-transform:uppercase; letter-spacing:.05em; color:#ccc; display:block; line-height:1.4; }
.fcst-ref .detail-label { color:#bbb; }
"""


def _weights_section_html(rows: list, all_members: list | None = None) -> str:
    from collections import defaultdict

    # build tuned weights: model_id -> member_id -> avg weight across all (var, lead) cells
    sums: dict = defaultdict(lambda: defaultdict(list))
    model_names: dict = {}
    member_names: dict = {}
    for r in rows:
        sums[r["model_id"]][r["member_id"]].append(r["weight"])
        model_names[r["model_id"]] = r["model_name"]
        member_names[(r["model_id"], r["member_id"])] = r["member_name"] or str(r["member_id"])

    avg_weights: dict = {
        mid: {mem_id: sum(ws) / len(ws) for mem_id, ws in members.items()}
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
                avg_weights[model_id] = {r["member_id"]: 1.0 / n for r in members}
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
        weights = avg_weights[model_id]
        n = len(weights)
        equal_w = 1.0 / n
        max_w = max(weights.values())
        spread = max_w - equal_w
        tuned = model_id in tuned_ids

        table_rows = []
        prev_group = None
        for mem_id in sorted(weights):
            w = weights[mem_id]
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

        untrained_note = (
            '' if tuned
            else ' <span style="color:#aaa;font-style:italic;font-weight:400">(not tuned)</span>'
        )
        blocks.append(
            f'<div class="weights-model-block">'
            f'<h3>{model_names[model_id]}{untrained_note}'
            f' <span class="model-id-cell">(model {model_id})</span></h3>'
            f'<p class="window-label">equal weight: {equal_w:.1%} per member</p>'
            f'<table class="weight-table">'
            f'<thead><tr><th>Member</th><th>Avg weight</th></tr></thead>'
            f'<tbody>{"".join(table_rows)}</tbody>'
            f'</table>'
            f'</div>'
        )

    return (
        '<details class="score-details" style="margin-top:16px">'
        '<summary>Member weights</summary>'
        f'<div class="weights-section">{"".join(blocks)}</div>'
        '</details>'
    )


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
    """variable -> model -> {x: [human timestamps], y: [values]}"""
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
        elif var == "wind_speed":
            v = _to_mph(v)
        data[var][model]["x"].append(fmt.short_ts(row["valid_at"]))
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
        f'<h3>{label}: {name} <span class="station-id">({station_id})</span></h3>'
        f'<p class="obs-time">{timestamp}</p>'
        f'<table class="obs-table"><tbody>{rows_html}</tbody></table>'
        f'</div>'
    )


def _forecast_table_html(table: dict, lead_times: list, slp_offset: float = 0.0) -> str:
    header_cells = "".join(f"<th>+{h}h</th>" for h in lead_times)
    rows = []
    for var in VARIABLES:
        label = _VARIABLE_LABEL.get(var, var)
        unit = _UNIT.get(var, "")
        if var == "pressure" and slp_offset != 0.0:
            label = "Station P"
        cells = []
        for h in lead_times:
            v = table.get(var, {}).get(h)
            if var == "temperature" or var == "dewpoint":
                v = _to_f(v)
            elif var == "wind_speed":
                v = _to_mph(v)
            cells.append(f"<td>{fmt.val(v, '.1f', unit)}</td>")
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
        type_badge = (
            f'<span class="ensemble-badge">{mtype}</span>' if mtype == "ensemble" else ""
        )
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
        name = r["name"] or r["station_id"]
        return f'{label}: {name} <span class="station-id">({r["station_id"]})</span>'

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
        '<section class="section">'
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
            badge = f'<span class="ensemble-badge">{m["type"]}</span>'
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
        hdr_class = "ensemble-header" if model_type == "ensemble" else "model-header"
        detail_rows.append(
            f'<tr class="{hdr_class}"><th colspan="{len(leads) + 1}">{model_id} — {model_name}</th></tr>'
        )
        var_data = by_model[(model_id, model_name, model_type)]
        row_class = ' class="ensemble-row"' if model_type == "ensemble" else ""
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
                    elif var == "wind_speed":
                        mae = _to_mph(mae)
                        bias = _to_mph(bias)
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
        f'{summary_table}'
        f'<details class="score-details">'
        f'<summary>Variable breakdown</summary>'
        f'{detail_table}'
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
                        else _to_mph(mae) if var == "wind_speed"
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
                        else _to_mph(bias) if var == "wind_speed"
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
                "points": {},
            }
        mae = row["avg_mae"]
        if mae is not None:
            if var in ("temperature", "dewpoint"):
                mae = _diff_to_f(mae)
            elif var == "wind_speed":
                mae = _to_mph(mae)
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
            elif var == "wind_speed":
                mae = _to_mph(mae)
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
                elif var == "wind_speed":
                    b = _to_mph(b)
                    m = _to_mph(m)
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
            elif var == "wind_speed":
                err = _to_mph(err)
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
        "wind_speed": "Wind Speed MAE (mph)",
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
            margin: {{ t: 40, b: 60, l: 50, r: 16 }},
            xaxis: {{ tickangle: -30, tickfont: {{ size: 11 }} }},
            yaxis: Object.assign({{ tickfont: {{ size: 11 }} }}, yRange),
            height: 380,
            showlegend: false,
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
        elif row["variable"] == "wind_speed":
            v = _to_mph(v)
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
        elif row["variable"] == "wind_speed":
            mae = _to_mph(mae)
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
        "wind_speed": "Wind Speed (mph)",
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

let fcstActiveVar = fcstVariables[0];

function drawFcstChart() {{
    const varData = fcstData[fcstActiveVar] || {{}};
    const traces = Object.entries(varData).map(function([model, d]) {{
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
        margin: {{ t: 40, b: 60, l: 50, r: 16 }},
        xaxis: {{ tickangle: -30, tickfont: {{ size: 11 }} }},
        yaxis: {{ tickfont: {{ size: 11 }} }},
        height: 420,
        showlegend: false,
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
        "wind_speed": "Wind Speed Bias (mph)",
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
            xaxis: {{ tickangle: -30, tickfont: {{ size: 11 }} }},
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
        "wind_speed": "Wind Speed MAE (mph)",
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
        "wind_speed": "Wind Speed (mph)",
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
        temperature: '\u00b0F', dewpoint: '\u00b0F', pressure: 'hPa', wind_speed: 'mph'
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

    def _fmt_value(variable, value, spread=None) -> str:
        """Format a forecast value (SI units) with optional spread as an HTML snippet."""
        if value is None:
            return "&mdash;"
        unit = _UNIT[variable]
        if variable in ("temperature", "dewpoint"):
            disp = _to_f(value)
            s = f"{disp:.0f}{unit}"
            spread_disp = _diff_to_f(spread) if spread is not None else None
        elif variable == "wind_speed":
            disp = _to_mph(value)
            s = f"{disp:.1f} {unit}"
            spread_disp = _to_mph(spread) if spread is not None else None
        else:
            s = f"{value:.1f} {unit}"
            spread_disp = spread
        if spread_disp is not None and spread_disp > 0:
            s += f'<small class="fcst-spread">&pm;{spread_disp:.1f}</small>'
        return s

    def _nws_at(target_ts: int) -> dict | None:
        """Return NWS forecast entry nearest to target_ts, within 90 min."""
        if not nws_forecast:
            return None
        best = min(nws_forecast, key=lambda t: abs(t - target_ts))
        if abs(best - target_ts) > 5400:
            return None
        return nws_forecast[best]

    def _card(label: str, is_now: bool, temp_val, dew_val, pres_val, wind_val,
              temp_spread=None, nws=None) -> str:
        cls = 'forecast-card now-card' if is_now else 'forecast-card'
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
        details_html = (
            '<div class="fcst-details">' + "<br>".join(details) + "</div>"
            if details else ""
        )

        nws_html = ""
        if nws:
            nws_lines = []
            if nws.get("temperature") is not None:
                nws_lines.append(
                    f'<span class="detail-label">Temp</span> {_to_f(nws["temperature"]):.0f}\u00b0F'
                )
            if nws.get("dewpoint") is not None:
                nws_lines.append(
                    f'<span class="detail-label">Dew</span> {_to_f(nws["dewpoint"]):.0f}\u00b0F'
                )
            if nws.get("wind_speed") is not None:
                nws_lines.append(
                    f'<span class="detail-label">Wind</span> {_to_mph(nws["wind_speed"]):.0f} mph'
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
            f'<div class="fcst-label">{label}</div>'
            f'{temp_html}'
            f'{details_html}'
            f'{nws_html}'
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
        w_cell = table.get("wind_speed", {}).get(lead)
        t_val = t_cell[0] if t_cell else None
        t_spread = t_cell[1] if t_cell else None
        d_val = d_cell[0] if d_cell else None
        p_raw = p_cell[0] if p_cell else None
        p_val = p_raw + slp_offset if p_raw is not None else None
        w_val = w_cell[0] if w_cell else None
        nws_entry = _nws_at(vat) if vat else None
        cards_html += _card(label, False, t_val, d_val, p_val, w_val, t_spread, nws_entry)

    issued_str = fmt.ts(issued_at) if issued_at else "&mdash;"
    return (
        '<section class="section">\n'
        '  <h2>Ensemble Forecast</h2>\n'
        f'  <div class="obs-time">issued {issued_str}</div>\n'
        f'  <div class="forecast-cards">{cards_html}</div>\n'
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

    # structure Hyp A MAE data by lead then member
    mae_by_lead: dict[int, dict[int, dict]] = {}
    for row in mae_rows:
        lead = row["lead_hours"]
        mid = row["member_id"]
        mae_by_lead.setdefault(lead, {}).setdefault(mid, {"x": [], "y": []})
        mae_by_lead[lead][mid]["x"].append(fmt.short_ts(row["issued_at"]))
        mae_by_lead[lead][mid]["y"].append(_diff_to_f(row["mae"]))

    # structure Hyp C: per-run MAE keyed by (variable, lead) → {model: {x, y}}
    hyp_c: dict[tuple, dict[str, dict]] = {}
    for row in all_model_mae_rows:
        key = (row["variable"], row["lead_hours"])
        model = row["model"]
        hyp_c.setdefault(key, {}).setdefault(model, {"x": [], "y": []})
        mae_val = row["mae"]
        if row["variable"] in ("temperature", "dewpoint"):
            mae_val = _diff_to_f(mae_val)
        hyp_c[key][model]["x"].append(fmt.short_ts(row["issued_at"]))
        hyp_c[key][model]["y"].append(mae_val)

    # structure Hyp D: {(variable, lead): {x, y}}
    hyp_d: dict[tuple, dict] = {}
    for row in pt_mae_rows:
        key = (row["variable"], row["lead_hours"])
        hyp_d.setdefault(key, {"x": [], "y": []})
        mae_val = row["mae"]
        if row["variable"] in ("temperature", "dewpoint"):
            mae_val = _diff_to_f(mae_val)
        hyp_d[key]["x"].append(fmt.short_ts(row["issued_at"]))
        hyp_d[key]["y"].append(mae_val)

    # structure Hyp E: {lead: {model: {x, y}}}
    hyp_e: dict[int, dict[str, dict]] = {}
    for row in decay_mae_rows:
        lead = row["lead_hours"]
        model = row["model"]
        hyp_e.setdefault(lead, {}).setdefault(model, {"x": [], "y": []})
        hyp_e[lead][model]["x"].append(fmt.short_ts(row["issued_at"]))
        hyp_e[lead][model]["y"].append(_diff_to_f(row["mae"]))

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

    # structure Hyp G: {lead: {model: {x, y}}}
    hyp_g: dict[int, dict[str, dict]] = {}
    for row in diurnal_climo_rows:
        lead = row["lead_hours"]
        model = row["model"]
        hyp_g.setdefault(lead, {}).setdefault(model, {"x": [], "y": []})
        hyp_g[lead][model]["x"].append(fmt.short_ts(row["issued_at"]))
        hyp_g[lead][model]["y"].append(_diff_to_f(row["mae"]))

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
        "clearness_pts": clearness_pts,
        "sky_pts": sky_pts,
        "hyp_c": hyp_c,
        "hyp_d": hyp_d,
        "hyp_e": hyp_e,
        "hyp_f": hyp_f,
        "spec_all": spec_all,
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
    has_clearness = bool(data["clearness_pts"])
    has_hyp_c = any(data["hyp_c"])
    has_hyp_d = any(data["hyp_d"])
    has_hyp_e = any(data["hyp_e"])
    has_hyp_f = bool(data["hyp_f"])
    has_hyp_g = any(data["hyp_g"])
    weights_html = _learnings_weights_table_html(data["weight_rows"])

    no_data = '<p class="no-data">Not enough scored data yet. Check back after several forecast cycles.</p>'

    return (
        '<section class="section">\n'
        "  <h2>Learnings</h2>\n"
        '  <p class="learnings-intro">Tracked hypotheses that accumulate evidence over time.'
        " Thin data is expected early &mdash; the goal is to watch these relationships evolve.</p>\n"
        "\n"
        # --- Hypothesis A ---
        "  <h3>Hypothesis A: Clearness persistence vs. pressure projection</h3>\n"
        '  <p class="learnings-desc">'
        "<strong>Question:</strong> Does projecting the solar clearness index forward "
        "via pressure tendency (airmass_diurnal member 3) reduce temperature MAE compared to "
        "simply persisting it (member 1)? The weights table shows whether "
        "<code>barogram tune</code> tracks the better performer over time."
        "</p>\n"
        + (
            '  <div class="learnings-hyp-grid">'
            '<div class="chart-container"><div id="learnings-mae-6h"></div></div>'
            '<div class="chart-container"><div id="learnings-mae-12h"></div></div>'
            "</div>\n"
            if has_mae
            else no_data + "\n"
        )
        + f"  {weights_html}\n"
        "\n"
        # --- Hypothesis B ---
        '  <h3 class="obs-subhead">Hypothesis B: Solar clearness index vs. NWS sky cover</h3>\n'
        '  <p class="learnings-desc">'
        "<strong>Question:</strong> Does the Tempest-derived clearness index track NWS-reported "
        "sky cover? Persistent divergence would suggest a sensor calibration issue or a local "
        "microclimate difference. NWS sky cover is validation only &mdash; never a model input."
        "</p>\n"
        + (
            '  <div class="chart-container"><div id="learnings-clearness-chart"></div></div>\n'
            if has_clearness
            else no_data + "\n"
        )
        + "\n"
        # --- Hypothesis C ---
        '  <h3 class="obs-subhead">Hypothesis C: Is the ensemble underperforming its best member?</h3>\n'
        '  <p class="learnings-desc">'
        "<strong>Question:</strong> A weighted ensemble should converge toward its best member as "
        "weights improve. Currently the ensemble is worse than <code>climo_deviation</code> on "
        "temperature at every lead. These charts track whether the gap is closing over time. "
        "If it stays wide, the ensemble weighting or composition needs revisiting."
        "</p>\n"
        + (
            '  <div class="learnings-hyp-grid">'
            '<div class="chart-container"><div id="learnings-hyp-c-6h"></div></div>'
            '<div class="chart-container"><div id="learnings-hyp-c-24h"></div></div>'
            "</div>\n"
            if has_hyp_c
            else no_data + "\n"
        )
        + "\n"
        # --- Hypothesis D ---
        '  <h3 class="obs-subhead">Hypothesis D: pressure_tendency &mdash; best and worst simultaneously</h3>\n'
        '  <p class="learnings-desc">'
        "<strong>Question:</strong> <code>pressure_tendency</code> is the best model for dewpoint "
        "at all leads, but its pressure MAE explodes with lead time (reaching 40+ hPa at 24h vs "
        "persistence&rsquo;s 5 hPa). Are these two behaviors correlated within individual runs? "
        "The chart shows both lines on the same axis at 12h lead. If they diverge structurally, "
        "the model may need a pressure-variable guard."
        "</p>\n"
        + (
            '  <div class="chart-container"><div id="learnings-hyp-d-chart"></div></div>\n'
            if has_hyp_d
            else no_data + "\n"
        )
        + "\n"
        # --- Hypothesis E ---
        '  <h3 class="obs-subhead">Hypothesis E: How long does the climo_deviation signal last?</h3>\n'
        '  <p class="learnings-desc">'
        "<strong>Question:</strong> At 6h lead, <code>climo_deviation</code> beats persistence by "
        "~1.9&deg;F on temperature. By 24h the gap is ~0.5&deg;F. These charts track "
        "climo_deviation vs persistence MAE over time at 6h and 24h to show whether the signal "
        "advantage holds or decays as conditions change seasonally."
        "</p>\n"
        + (
            '  <div class="learnings-hyp-grid">'
            '<div class="chart-container"><div id="learnings-hyp-e-6h"></div></div>'
            '<div class="chart-container"><div id="learnings-hyp-e-24h"></div></div>'
            "</div>\n"
            if has_hyp_e
            else no_data + "\n"
        )
        + "\n"
        # --- Hypothesis F ---
        '  <h3 class="obs-subhead">Hypothesis F: Model specialization map</h3>\n'
        '  <p class="learnings-desc">'
        "<strong>Question:</strong> Which model is best for each variable and lead hour? "
        "This heatmap shows the best-performing base model (excluding the ensemble) per cell. "
        "Watch for whether the ensemble weights actually reflect this specialization over time."
        "</p>\n"
        + (
            '  <div class="chart-container"><div id="learnings-hyp-f-chart"></div></div>\n'
            if has_hyp_f
            else no_data + "\n"
        )
        + "\n"
        # --- Hypothesis G ---
        '  <h3 class="obs-subhead">Hypothesis G: Does diurnal_curve add skill over climo_deviation?</h3>\n'
        '  <p class="learnings-desc">'
        "<strong>Question:</strong> <code>diurnal_curve</code> was designed to improve on "
        "<code>climo_deviation</code> by explicitly modeling the daily temperature cycle. "
        "Currently <code>climo_deviation</code> wins at every temperature lead. These charts "
        "track whether <code>diurnal_curve</code> closes the gap over time, "
        "or whether the recency weighting in <code>climo_deviation</code> is the real advantage."
        "</p>\n"
        + (
            '  <div class="learnings-hyp-grid">'
            '<div class="chart-container"><div id="learnings-hyp-g-6h"></div></div>'
            '<div class="chart-container"><div id="learnings-hyp-g-24h"></div></div>'
            "</div>\n"
            if has_hyp_g
            else no_data + "\n"
        )
        + "</section>\n"
    )


def _learnings_js(data: dict) -> str:
    _font = "'-apple-system, sans-serif'"
    _base_layout = (
        f"margin:{{t:40,b:60,l:50,r:16}},"
        f"xaxis:{{tickangle:-30,tickfont:{{size:11}}}},"
        f"yaxis:{{rangemode:'tozero',tickfont:{{size:11}}}},"
        f"height:320,showlegend:true,"
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

    # --- Hypothesis B: clearness index vs sky cover ---
    clearness_traces = []
    if data["clearness_pts"]:
        k_x = json.dumps([fmt.short_ts(ts) for ts, _ in data["clearness_pts"]])
        k_y = json.dumps([round(k, 3) for _, k in data["clearness_pts"]])
        clearness_traces.append(
            f"{{type:'scatter',mode:'lines',name:'clearness index (Tempest)',"
            f"x:{k_x},y:{k_y},"
            f"line:{{color:'#f6a623',width:1.5}},yaxis:'y'}}"
        )
    if data["sky_pts"]:
        s_x = json.dumps([fmt.short_ts(ts) for ts, _ in data["sky_pts"]])
        s_y = json.dumps([frac for _, frac in data["sky_pts"]])
        clearness_traces.append(
            f"{{type:'scatter',mode:'markers',name:'sky cover fraction (NWS)',"
            f"x:{s_x},y:{s_y},"
            f"marker:{{color:'#4a90d9',size:5,opacity:0.7}},yaxis:'y2'}}"
        )
    if clearness_traces:
        cl_layout = (
            f"{{title:{{text:'Solar clearness index vs. NWS sky cover (last 30 days)',"
            f"font:{{size:13,family:{_font}}}}},"
            f"margin:{{t:40,b:60,l:50,r:60}},"
            f"xaxis:{{tickangle:-30,tickfont:{{size:11}}}},"
            f"yaxis:{{title:'clearness index k',range:[0,1.05],tickfont:{{size:11}}}},"
            f"yaxis2:{{title:'sky cover fraction',range:[0,1.05],"
            f"overlaying:'y',side:'right',tickfont:{{size:11}}}},"
            f"height:360,showlegend:true,"
            f"paper_bgcolor:'white',plot_bgcolor:'#fafafa'}}"
        )
        lines.append(
            f"if(document.getElementById('learnings-clearness-chart'))"
            f"{{Plotly.react('learnings-clearness-chart',"
            f"[{','.join(clearness_traces)}],{cl_layout},{{responsive:true}});}}"
        )

    # --- Hypothesis C: ensemble vs best member over time (temperature, 6h and 24h) ---
    _model_colors = {
        "climo_deviation": "#2ca02c",
        "persistence": "#7f7f7f",
        "barogram_ensemble": "#9467bd",
        "diurnal_curve": "#17becf",
        "pressure_tendency": "#d62728",
        "weighted_climatological_mean": "#8c564b",
        "climatological_mean": "#bcbd22",
        "airmass_diurnal": "#e377c2",
    }
    _model_dash = {
        "barogram_ensemble": "dash",
        "climo_deviation": "solid",
        "persistence": "dot",
    }
    for lead in [6, 24]:
        key = ("temperature", lead)
        model_series = data["hyp_c"].get(key, {})
        traces = []
        # show ensemble and the two key competitors
        for m in ["barogram_ensemble", "climo_deviation", "persistence"]:
            if m in model_series:
                traces.append(_line_trace(
                    m, model_series[m],
                    _model_colors.get(m, "#333"),
                    _model_dash.get(m, "solid"),
                ))
        if traces:
            lines.append(_react(
                f"learnings-hyp-c-{lead}h",
                "[" + ",".join(traces) + "]",
                f"+{lead}h \u2014 Temperature MAE (\u00b0F): ensemble vs best members",
            ))

    # --- Hypothesis D: pressure_tendency paradox at 12h ---
    pt_colors = {"dewpoint": "#1f77b4", "pressure": "#d62728"}
    pt_dash_map = {"dewpoint": "solid", "pressure": "dot"}
    pt_traces = []
    for var in ["dewpoint", "pressure"]:
        key = (var, 12)
        series = data["hyp_d"].get(key, {})
        if series.get("x"):
            unit = "\u00b0F" if var == "dewpoint" else "hPa"
            pt_traces.append(_line_trace(
                f"{var} MAE ({unit})", series,
                pt_colors[var], pt_dash_map[var],
            ))
    if pt_traces:
        lines.append(_react(
            "learnings-hyp-d-chart",
            "[" + ",".join(pt_traces) + "]",
            "+12h \u2014 pressure_tendency MAE: dewpoint vs pressure",
            "yaxis:{rangemode:'tozero',tickfont:{size:11},title:'MAE'}",
        ))

    # --- Hypothesis E: climo_deviation vs persistence signal decay at 6h and 24h ---
    e_colors = {"climo_deviation": "#2ca02c", "persistence": "#7f7f7f"}
    e_dash = {"climo_deviation": "solid", "persistence": "dot"}
    for lead in [6, 24]:
        lead_data = data["hyp_e"].get(lead, {})
        traces = []
        for m in ["climo_deviation", "persistence"]:
            if m in lead_data and lead_data[m].get("x"):
                traces.append(_line_trace(
                    m, lead_data[m], e_colors[m], e_dash[m],
                ))
        if traces:
            lines.append(_react(
                f"learnings-hyp-e-{lead}h",
                "[" + ",".join(traces) + "]",
                f"+{lead}h \u2014 Temperature MAE (\u00b0F): signal decay",
            ))

    # --- Hypothesis F: model specialization heatmap ---
    variables = ["temperature", "dewpoint", "pressure", "wind_speed"]
    leads = [6, 12, 18, 24]
    var_labels = {"temperature": "temp", "dewpoint": "dewpt", "pressure": "pressure", "wind_speed": "wind"}

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
        # add model name annotations
        annotations_js = "["
        for ri, lt in enumerate(leads):
            for ci, var in enumerate(variables):
                key = (var, lt)
                best = data["hyp_f"].get(key)
                model_short = best["model"].replace("_", "\u200b_") if best else ""
                annotations_js += (
                    f"{{x:{ci},y:{ri},text:{json.dumps(model_short)},"
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
            f"height:280,paper_bgcolor:'white',plot_bgcolor:'#fafafa'}}"
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
            if m in lead_data and lead_data[m].get("x"):
                traces.append(_line_trace(
                    m, lead_data[m], g_colors[m], g_dash[m],
                ))
        if traces:
            lines.append(_react(
                f"learnings-hyp-g-{lead}h",
                "[" + ",".join(traces) + "]",
                f"+{lead}h \u2014 Temperature MAE (\u00b0F): diurnal_curve vs climo_deviation",
            ))

    return "\n".join(lines)


def generate(
    conn_in: sqlite3.Connection,
    conn_out: sqlite3.Connection,
    output_path: Path,
) -> None:
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
    all_scores_30 = db.score_summary_last_n_runs(conn_out, 30)
    summary_30 = [r for r in all_scores_30 if r["member_id"] == 0]
    all_scores_10 = db.score_summary_last_n_runs(conn_out, 10)
    summary_10 = [r for r in all_scores_10 if r["member_id"] == 0]
    members_10 = [r for r in all_scores_10 if r["member_id"] > 0]
    member_models = {r["model"] for r in members_10}
    summary_7d = [r for r in db.score_summary_since(conn_out, now - 7 * 86400) if r["member_id"] == 0]
    timeseries = [r for r in db.score_timeseries(conn_out, since=midnight_7d_ago) if r["member_id"] == 0]
    all_time_summary = [r for r in db.score_summary(conn_out) if r["member_id"] == 0]
    bias_ts_rows = [r for r in db.bias_timeseries(conn_out, since=midnight_7d_ago) if r["member_id"] == 0]
    diurnal_rows = [r for r in db.diurnal_errors(conn_out) if r["member_id"] == 0]
    error_dist_rows = db.error_distribution(conn_out)
    weight_rows = db.all_weights_with_members(conn_out)
    all_members = db.all_members_for_ensemble_models(conn_out)

    lead_times = sorted({row["lead_hours"] for row in mean_rows})
    charts = _chart_data(mean_rows)
    mae_ts = _mae_timeseries_data(timeseries)
    bias_ts = _bias_timeseries_data(bias_ts_rows)
    lead_skill = _lead_skill_data(all_time_summary)
    heatmap = _heatmap_data(all_time_summary)
    diurnal = _diurnal_data(diurnal_rows)
    error_dist = _error_dist_data(error_dist_rows)
    generated_at = fmt.ts(now)
    _lf = db.get_metadata(conn_out, "last_forecast")
    _lt = db.get_metadata(conn_out, "last_tune")
    last_forecast_str = fmt.ts(int(_lf)) if _lf else "\u2014"
    last_tune_str = fmt.ts(int(_lt)) if _lt else "\u2014"

    learnings = _learnings_data(conn_in, conn_out)
    learnings_section = _learnings_section_html(learnings)

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
            ("dewpoint", "Dew Point"), ("pressure", "Pressure"), ("wind_speed", "Wind Speed"),
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
            ("pressure", "Pressure"), ("wind_speed", "Wind Speed"),
        ])
    )

    _var_btns = [
        ("temperature", "Temperature"), ("dewpoint", "Dew Point"),
        ("pressure", "Pressure"), ("wind_speed", "Wind Speed"),
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
  <h1>barogram</h1>
  <div class="generated">
    <span>generated {generated_at}</span>
    <span>last forecast: {last_forecast_str}</span>
    <span>last tune: {last_tune_str}</span>
  </div>
</header>

<section class="section">
  <h2>Latest Conditions</h2>
  <div class="conditions-grid">
    {tempest_card}
    {nws_card}
  </div>
  {zambretti_panel}
</section>

{ensemble_section}

<section class="section">
  <h2>Verification</h2>
  <div class="verification-primary">
    {table_30}
  </div>
  <div class="verification-windows">
    {table_10}
    {table_7d}
  </div>
  {weights_section}
  <h3 class="obs-subhead">MAE over time</h3>
  <div class="mae-filter-bar">{filter_btns}<button id="smooth-toggle" class="mae-raw-btn">Per-run detail</button><button id="raw-toggle" class="mae-raw-btn">Raw values</button></div>
  <p class="chart-legend-note">Grey: reference lines (climo = long-dash, persistence = dotted) &nbsp;·&nbsp; Per-run detail: solid with dash-dot rolling avg overlay</p>
  <div class="mae-charts-grid">
    {mae_chart_divs}
  </div>
</section>

<section class="section">
  <h2>Model Analysis</h2>

  <h3>Bias Over Time</h3>
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

  <h3 class="obs-subhead">Diurnal Stratification</h3>
  <div class="mae-filter-bar">
    {diurnal_filter_btns}
    <button id="diurnal-mode-btn" class="mae-raw-btn">Show MAE</button>
  </div>
  <div class="chart-container"><div id="diurnal-chart"></div></div>

  <h3 class="obs-subhead">Error Distribution</h3>
  <div class="mae-filter-bar">{error_dist_var_btns}</div>
  <div class="mae-filter-bar">{error_dist_lead_btns}</div>
  <div class="chart-container"><div id="error-dist-chart"></div></div>
</section>

{learnings_section}

<section class="section">
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
{_diurnal_js(diurnal)}
{_error_dist_js(error_dist)}
{_learnings_js(learnings)}
</script>
</body>
</html>
"""

    output_path.write_text(html, encoding="utf-8")
