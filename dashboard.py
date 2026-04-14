import json
import sqlite3
import time
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
    "pressure": "mb",
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
.weights-section { display: flex; flex-direction: column; gap: 16px; margin-top: 12px; }
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
"""


def _weights_section_html(rows: list) -> str:
    if not rows:
        return ""

    # average weight per (model_id, member_id) across all (variable, lead_hours) groups
    from collections import defaultdict
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

    def _group_label(name: str) -> str:
        if name.startswith("s-"):
            return "static"
        if name.startswith("d03-"):
            return "decay k=0.03"
        if name.startswith("d05-"):
            return "decay k=0.05"
        if name.startswith("d10-"):
            return "decay k=0.10"
        return ""

    blocks = []
    for model_id in sorted(avg_weights):
        weights = avg_weights[model_id]
        n = len(weights)
        equal_w = 1.0 / n
        max_w = max(weights.values())
        spread = max_w - equal_w

        table_rows = []
        prev_group = None
        for mem_id in sorted(weights):
            w = weights[mem_id]
            name = member_names[(model_id, mem_id)]

            # group separator for models with named prefixes (model 4)
            group = _group_label(name)
            if group and group != prev_group:
                table_rows.append(
                    f'<tr class="weight-group-hdr"><th colspan="2">{group}</th></tr>'
                )
                prev_group = group

            # color: blue tint proportional to how far above equal weight
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

        blocks.append(
            f'<div class="weights-model-block">'
            f'<h3>{model_names[model_id]} <span class="model-id-cell">(model {model_id})</span></h3>'
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
        f'Zambretti algorithm &mdash; station pressure, not altitude-corrected'
        f'</p>'
        f'</div>'
    )


def _conditions_card(label: str, obs) -> str:
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
        rows_html = (
            f'<tr><th>Temperature</th><td>{fmt.temp(obs["air_temp"])}</td></tr>'
            f'<tr><th>Dew Point</th><td>{fmt.temp(obs["dew_point"])}</td></tr>'
            f'<tr><th>Pressure</th><td>{fmt.val(obs["station_pressure"], ".1f", " mb")} (station)</td></tr>'
            f'<tr><th>Wind</th><td>{fmt.wind_dir(obs["wind_direction"])} {fmt.val(_to_mph(obs["wind_avg"]), ".1f", " mph")}{gust_str}</td></tr>'
            f'<tr><th>Precip today</th><td>{fmt.val(_to_in(obs["precip_accum_day"]), ".2f", " in")}</td></tr>'
            f'<tr><th>UV Index</th><td>{fmt.val(obs["uv_index"], ".1f")}</td></tr>'
            f'<tr><th>Solar</th><td>{fmt.val(obs["solar_radiation"], ".0f", " W/m\u00b2")}</td></tr>'
            f'<tr><th>Lightning</th><td>{lc if lc is not None else 0} strikes</td></tr>'
        )
    else:
        rows_html = (
            f'<tr><th>Temperature</th><td>{fmt.temp(obs["air_temp"])}</td></tr>'
            f'<tr><th>Dew Point</th><td>{fmt.temp(obs["dew_point"])}</td></tr>'
            f'<tr><th>Wind</th><td>{fmt.wind_dir(obs["wind_direction"])} {fmt.val(_to_mph(obs["wind_speed"]), ".1f", " mph")}</td></tr>'
            f'<tr><th>Pressure</th><td>{fmt.val(obs["sea_level_pressure"], ".1f", " mb")}</td></tr>'
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


def _forecast_table_html(table: dict, lead_times: list) -> str:
    header_cells = "".join(f"<th>+{h}h</th>" for h in lead_times)
    rows = []
    for var in VARIABLES:
        label = _VARIABLE_LABEL.get(var, var)
        unit = _UNIT.get(var, "")
        cells = []
        for h in lead_times:
            v = table.get(var, {}).get(h)
            if var == "temperature" or var == "dewpoint":
                v = _to_f(v)
            elif var == "wind_speed":
                v = _to_mph(v)
            cells.append(f"<td>{fmt.val(v, '.1f', unit)}</td>")
        rows.append(f'<tr><th>{label}</th>{"".join(cells)}</tr>')

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
        table_html = _forecast_table_html(table, lead_times)
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


def _tempest_obs_row(row) -> str:
    gust = row["wind_gust"]
    wind = fmt.wind_dir(row["wind_direction"]) + " " + fmt.val(_to_mph(row["wind_avg"]), ".1f", " mph")
    if gust is not None:
        wind += f" g{fmt.val(_to_mph(gust), '.1f')}"
    lc = row["lightning_count"]
    return (
        "<tr>"
        f"<td>{fmt.ts(row['timestamp'])}</td>"
        f"<td>{fmt.temp(row['air_temp'])}</td>"
        f"<td>{fmt.temp(row['dew_point'])}</td>"
        f"<td>{fmt.val(row['station_pressure'], '.1f', ' mb')}</td>"
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
        f"<td>{fmt.val(row['sea_level_pressure'], '.1f', ' mb')}</td>"
        f"<td>{row['sky_cover'] or '\u2014'}</td>"
        "</tr>"
    )


def _obs_history_section(tempest_obs: list, nws_obs: list) -> str:
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

    tempest_block = table_block(
        "Tempest", tempest_obs, "tempest-obs-tbody", "tempest-more-btn",
        ["Time", "Temperature", "Dew Point", "Pressure", "Wind", "Precip (day)", "Lightning"],
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

    persistence = model_summary.get("persistence", {})
    p_avg = persistence.get("avg_mae")
    p_24h = persistence.get("mae_24h")

    total = sum(row["n"] for row in summary_rows)
    sorted_names = sorted(model_summary.keys(), key=lambda k: model_summary[k]["model_id"])

    # level 1: at-a-glance summary rows
    summary_tbody = []
    for name in sorted_names:
        m = model_summary[name]
        if name == "persistence":
            badge = '<span class="baseline-badge">baseline</span>'
        elif m["type"] == "ensemble":
            badge = f'<span class="ensemble-badge">{m["type"]}</span>'
        else:
            badge = ""

        if name == "persistence":
            avg_ratio = 1.0 if m["avg_mae"] is not None else None
            h24_ratio = 1.0 if m["mae_24h"] is not None else None
        else:
            avg_ratio = m["avg_mae"] / p_avg if (m["avg_mae"] is not None and p_avg) else None
            h24_ratio = m["mae_24h"] / p_24h if (m["mae_24h"] is not None and p_24h) else None
        avg_cls = _mae_color_class(avg_ratio, 1.0) if name != "persistence" and avg_ratio is not None else ""
        h24_cls = _mae_color_class(h24_ratio, 1.0) if name != "persistence" and h24_ratio is not None else ""
        def _cell(ratio, raw, cls, is_baseline=False):
            if ratio is None:
                return "\u2014"
            raw_str = f"{raw:.2f}" if raw is not None else "\u2014"
            effective_cls = ' class="mae-baseline-val"' if is_baseline else cls
            return f'<span{effective_cls} data-raw="{raw_str}" data-ratio="{ratio:.2f}">{ratio:.2f}</span>'
        is_baseline = name == "persistence"
        avg_str = _cell(avg_ratio, m["avg_mae"], avg_cls, is_baseline)
        h24_str = _cell(h24_ratio, m["mae_24h"], h24_cls, is_baseline)

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
        row_cls = ' class="baseline-row"' if name == 'persistence' else ''
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
        '<thead><tr><th>ID</th><th>Model</th><th class="col-avg-hdr">Avg skill ratio</th><th class="col-24h-hdr">+24h skill ratio</th><th></th></tr></thead>'
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


def _mae_timeseries_data(timeseries_rows: list) -> dict:
    """lead (str) -> model -> {is_persistence, is_ensemble, series: {var|avg -> {x, y_raw, y_ratio}}}

    y_ratio = MAE / persistence_MAE for the same (var, lead, issued_at).
    Persistence series always has y_ratio = 1.0. Average series ratios are
    the mean ratio across variables (dimensionless, comparable across vars).
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
        # persistence MAE for this lead: var -> issued_at -> mae
        p_ts: dict = raw[lead].get("persistence", {})
        result[str(lead)] = {}
        for model, vars_ in raw[lead].items():
            is_persistence = model == "persistence"
            is_ensemble = model_meta[model]["is_ensemble"]
            series: dict = {}
            for var, ts in vars_.items():
                p_var = p_ts.get(var, {})
                x, y_raw, y_ratio = [], [], []
                for issued in sorted(ts):
                    mae = ts[issued]
                    mae_display = (
                        _diff_to_f(mae) if var in ("temperature", "dewpoint")
                        else _to_mph(mae) if var == "wind_speed"
                        else mae
                    )
                    p = p_var.get(issued)
                    ratio = 1.0 if is_persistence else (mae / p if p else None)
                    x.append(fmt.short_ts(issued))
                    y_raw.append(mae_display)
                    y_ratio.append(ratio)
                series[var] = {"x": x, "y_raw": y_raw, "y_ratio": y_ratio}
            # average series
            all_issued = sorted(set().union(*[set(ts) for ts in vars_.values()]))
            ax, ay_raw, ay_ratio = [], [], []
            for issued in all_issued:
                ratios, raws = [], []
                for var, ts in vars_.items():
                    if issued not in ts:
                        continue
                    mae = ts[issued]
                    p_var = p_ts.get(var, {})
                    p = p_var.get(issued)
                    if is_persistence or p:
                        ratios.append(1.0 if is_persistence else mae / p)
                    raws.append(mae)
                if ratios:
                    ax.append(fmt.short_ts(issued))
                    ay_ratio.append(sum(ratios) / len(ratios))
                    ay_raw.append(sum(raws) / len(raws) if raws else None)
            series["avg"] = {"x": ax, "y_raw": ay_raw, "y_ratio": ay_ratio}
            result[str(lead)][model] = {
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
        "pressure": "Pressure MAE (mb)",
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

function drawMaeCharts() {{
    maeLeads.forEach(function(lead) {{
        const leadData = maeLeadData[String(lead)] || {{}};
        const traces = Object.entries(leadData).map(function([model, info]) {{
            const s = (info.series || {{}})[maeActiveVar] || {{}};
            const y = verifMode === 'ratio' ? (s.y_ratio || []) : (s.y_raw || []);
            const isPersistence = info.is_persistence;
            const isEns = info.is_ensemble;
            const color = isPersistence ? '#aaaaaa' : maeModelColors[model];
            return {{
                type: 'scatter',
                mode: 'lines+markers',
                name: String(info.model_id),
                x: s.x || [],
                y: y,
                line: {{
                    width: 2,
                    dash: isPersistence ? 'dot' : (isEns ? 'dash' : 'solid'),
                    color: color
                }},
                marker: {{ size: isPersistence ? 5 : 6, color: color }}
            }};
        }});
        const isRatio = verifMode === 'ratio';
        const varLabel = isRatio
            ? (maeActiveVar === 'avg' ? 'average skill ratio' : maeFilterLabels[maeActiveVar].replace(' MAE', ' skill ratio'))
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
        el.textContent = isRatio ? 'Avg skill ratio' : 'Avg MAE';
    }});
    document.querySelectorAll('.col-24h-hdr').forEach(function(el) {{
        el.textContent = isRatio ? '+24h skill ratio' : '+24h MAE';
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
        "pressure": "Pressure (mb)",
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
        "pressure": "Pressure Bias (mb)",
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
        "pressure": "Pressure MAE (mb)",
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
        "pressure": "Pressure (mb)",
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
        temperature: '\u00b0F', dewpoint: '\u00b0F', pressure: 'mb', wind_speed: 'mph'
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


def generate(
    conn_in: sqlite3.Connection,
    conn_out: sqlite3.Connection,
    output_path: Path,
) -> None:
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
    member_counts = {mid: len(mids) for mid, mids in model_member_ids.items()}

    tempest = db.latest_tempest_obs(conn_in)
    nws = db.latest_nws_obs(conn_in)
    tempest_history = db.recent_tempest_obs(conn_in)
    nws_history = db.recent_nws_obs(conn_in)

    now = int(time.time())
    all_scores_10 = db.score_summary_last_n_runs(conn_out, 10)
    summary_10 = [r for r in all_scores_10 if r["member_id"] == 0]
    members_10 = [r for r in all_scores_10 if r["member_id"] > 0]
    member_models = {r["model"] for r in members_10}
    summary_7d = [r for r in db.score_summary_since(conn_out, now - 7 * 86400) if r["member_id"] == 0]
    timeseries = [r for r in db.score_timeseries(conn_out) if r["member_id"] == 0]
    all_time_summary = [r for r in db.score_summary(conn_out) if r["member_id"] == 0]
    bias_ts_rows = [r for r in db.bias_timeseries(conn_out) if r["member_id"] == 0]
    diurnal_rows = [r for r in db.diurnal_errors(conn_out) if r["member_id"] == 0]
    error_dist_rows = db.error_distribution(conn_out)
    weight_rows = db.all_weights_with_members(conn_out)

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

    zambretti = pressure_tendency.zambretti_text(tempest, conn_in) if tempest else None
    zambretti_panel = _zambretti_panel_html(zambretti)
    tempest_card = _conditions_card("Tempest", tempest)
    nws_card = _conditions_card("NWS", nws)
    model_runs = _model_runs_html(mean_rows, lead_times, member_counts, member_forecast_rows)
    obs_section = _obs_history_section(tempest_history, nws_history)
    tempest_rows = [_tempest_obs_row(r) for r in tempest_history]
    nws_rows = [_nws_obs_row(r) for r in nws_history]

    all_models = {r["model"]: {"model_id": r["model_id"], "type": r["type"]} for r in mean_rows}
    table_10 = _score_summary_table(summary_10, "last 10 runs", member_models, all_models)
    table_7d = _score_summary_table(summary_7d, "last 7 days", member_models, all_models)
    weights_section = _weights_section_html(weight_rows)
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

<section class="section">
  <h2>Ensemble Forecast</h2>
  <p class="muted">Ensemble model not yet available &mdash; in development.</p>
</section>

<section class="section">
  <h2>Verification</h2>
  <div class="verification-windows">
    {table_10}
    {table_7d}
  </div>
  {weights_section}
  <h3 class="obs-subhead">MAE over time</h3>
  <div class="mae-filter-bar">{filter_btns}<button id="raw-toggle" class="mae-raw-btn">Raw values</button></div>
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
<script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
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
</script>
</body>
</html>
"""

    output_path.write_text(html, encoding="utf-8")
