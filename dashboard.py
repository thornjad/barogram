import json
import sqlite3
import time
from pathlib import Path

import db
import fmt

VARIABLES = ["temperature", "humidity", "pressure", "wind_speed"]

_VARIABLE_LABEL = {
    "temperature": "Temperature",
    "humidity": "Humidity",
    "pressure": "Pressure",
    "wind_speed": "Wind Speed",
}

_UNIT = {
    "temperature": "\u00b0C",
    "humidity": "%",
    "pressure": "mb",
    "wind_speed": "m/s",
}

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
.generated { font-size: 12px; color: #666; }
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
.chart-container {
    background: #fff;
    border: 1px solid #ddd;
    border-radius: 4px;
    overflow: hidden;
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
.base-badge, .ensemble-badge { font-size: 11px; padding: 1px 6px; border-radius: 3px; font-weight: 600; letter-spacing: 0.03em; text-transform: uppercase; }
.base-badge { background: #e8f4e8; color: #2d6a2d; }
.ensemble-badge { background: #eff4ff; color: #3b5bdb; }
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
@media (max-width: 600px) {
    .conditions-grid, .charts-grid, .verification-windows { grid-template-columns: 1fr; }
}
"""


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
            data[var][model] = {"x": [], "y": []}
        data[var][model]["x"].append(fmt.ts(row["valid_at"]))
        data[var][model]["y"].append(row["value"])
    return data


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
        gust_str = f", gusts to {fmt.val(gust, '.1f', ' m/s')}" if gust is not None else ""
        lc = obs["lightning_count"]
        rows_html = (
            f'<tr><th>Temperature</th><td>{fmt.temp(obs["air_temp"])}</td></tr>'
            f'<tr><th>Humidity</th><td>{fmt.val(obs["relative_humidity"], ".0f", "%")}</td></tr>'
            f'<tr><th>Pressure</th><td>{fmt.val(obs["station_pressure"], ".1f", " mb")} (station)</td></tr>'
            f'<tr><th>Wind</th><td>{fmt.wind_dir(obs["wind_direction"])} {fmt.val(obs["wind_avg"], ".1f", " m/s")}{gust_str}</td></tr>'
            f'<tr><th>Precip today</th><td>{fmt.val(obs["precip_accum_day"], ".1f", " mm")}</td></tr>'
            f'<tr><th>UV Index</th><td>{fmt.val(obs["uv_index"], ".1f")}</td></tr>'
            f'<tr><th>Solar</th><td>{fmt.val(obs["solar_radiation"], ".0f", " W/m\u00b2")}</td></tr>'
            f'<tr><th>Lightning</th><td>{lc if lc is not None else 0} strikes</td></tr>'
        )
    else:
        rows_html = (
            f'<tr><th>Temperature</th><td>{fmt.temp(obs["air_temp"])}</td></tr>'
            f'<tr><th>Dewpoint</th><td>{fmt.temp(obs["dew_point"])}</td></tr>'
            f'<tr><th>Humidity</th><td>{fmt.val(obs["relative_humidity"], ".0f", "%")}</td></tr>'
            f'<tr><th>Wind</th><td>{fmt.wind_dir(obs["wind_direction"])} {fmt.val(obs["wind_speed"], ".1f", " m/s")}</td></tr>'
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
            cells.append(f"<td>{fmt.val(v, '.1f', unit)}</td>")
        rows.append(f'<tr><th>{label}</th>{"".join(cells)}</tr>')

    return (
        '<table class="forecast-table">'
        f'<thead><tr><th>Variable</th>{header_cells}</tr></thead>'
        f'<tbody>{"".join(rows)}</tbody>'
        '</table>'
    )


def _model_runs_html(rows: list, lead_times: list, member_counts: dict | None = None) -> str:
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
        member_badge = (
            f'<span class="member-badge">{n_members} members</span>' if n_members else ""
        )
        cards.append(
            f'<div class="model-run-card">'
            f'<div class="model-run-header">'
            f'<strong><span class="model-id-cell">{model_id}</span> {model}</strong>'
            f'{type_badge}'
            f'{member_badge}'
            f'<span class="run-detail">issued {fmt.ts(issued_at)} &mdash; {len(model_rows)} rows</span>'
            f'</div>'
            f'{table_html}'
            f'</div>'
        )
    return "\n".join(cards)


def _tempest_obs_row(row) -> str:
    gust = row["wind_gust"]
    wind = fmt.wind_dir(row["wind_direction"]) + " " + fmt.val(row["wind_avg"], ".1f", " m/s")
    if gust is not None:
        wind += f" g{fmt.val(gust, '.1f')}"
    lc = row["lightning_count"]
    return (
        "<tr>"
        f"<td>{fmt.ts(row['timestamp'])}</td>"
        f"<td>{fmt.temp(row['air_temp'])}</td>"
        f"<td>{fmt.val(row['relative_humidity'], '.0f', '%')}</td>"
        f"<td>{fmt.val(row['station_pressure'], '.1f', ' mb')}</td>"
        f"<td>{wind}</td>"
        f"<td>{fmt.val(row['precip_accum_day'], '.1f', ' mm')}</td>"
        f"<td>{lc if lc is not None else 0}</td>"
        "</tr>"
    )


def _nws_obs_row(row) -> str:
    return (
        "<tr>"
        f"<td>{fmt.ts(row['timestamp'])}</td>"
        f"<td>{fmt.temp(row['air_temp'])}</td>"
        f"<td>{fmt.temp(row['dew_point'])}</td>"
        f"<td>{fmt.val(row['relative_humidity'], '.0f', '%')}</td>"
        f"<td>{fmt.wind_dir(row['wind_direction'])} {fmt.val(row['wind_speed'], '.1f', ' m/s')}</td>"
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
        ["Time", "Temperature", "Humidity", "Pressure", "Wind", "Precip (day)", "Lightning"],
    )
    nws_block = table_block(
        "NWS", nws_obs, "nws-obs-tbody", "nws-more-btn",
        ["Time", "Temperature", "Dewpoint", "Humidity", "Wind", "Pressure", "Sky"],
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
        badge = (
            f'<span class="ensemble-badge">{m["type"]}</span>'
            if m["type"] == "ensemble" else ""
        )

        avg_cls = _mae_color_class(m["avg_mae"], p_avg) if name != "persistence" else ""
        h24_cls = _mae_color_class(m["mae_24h"], p_24h) if name != "persistence" else ""
        avg_str = f'<span{avg_cls}>{m["avg_mae"]:.2f}</span>' if m["avg_mae"] is not None else "\u2014"
        h24_str = f'<span{h24_cls}>{m["mae_24h"]:.2f}</span>' if m["mae_24h"] is not None else "\u2014"

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
        summary_tbody.append(
            f'<tr>'
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
        '<thead><tr><th>ID</th><th>Model</th><th>Avg MAE</th><th>+24h MAE</th><th></th></tr></thead>'
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
                    sign = "+" if bias >= 0 else ""
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
    """variable -> 'model +Nh' -> {x, y, is_ensemble}"""
    data: dict = {}
    for row in timeseries_rows:
        var = row["variable"]
        key = f"{row['model']} +{row['lead_hours']}h"
        is_ensemble = row["type"] == "ensemble"
        data.setdefault(var, {}).setdefault(key, {"x": [], "y": [], "is_ensemble": is_ensemble})
        data[var][key]["x"].append(fmt.ts(row["issued_at"]))
        data[var][key]["y"].append(row["avg_mae"])
    return data


def _mae_timeseries_js(timeseries_data: dict) -> str:
    data_json = json.dumps(timeseries_data)
    var_labels_json = json.dumps({
        "temperature": "Temperature MAE (\u00b0C)",
        "humidity": "Humidity MAE (%)",
        "pressure": "Pressure MAE (mb)",
        "wind_speed": "Wind Speed MAE (m/s)",
    })
    vars_json = json.dumps(VARIABLES)
    return f"""\
const maeData = {data_json};
const maeVarLabels = {var_labels_json};
const maeVariables = {vars_json};

maeVariables.forEach(function(variable) {{
    const varData = maeData[variable] || {{}};
    const traces = Object.entries(varData).map(function([lead, d]) {{
        const ens = d.is_ensemble;
        return {{
            type: 'scatter',
            mode: 'lines+markers',
            name: lead,
            x: d.x,
            y: d.y,
            line: {{ width: ens ? 3 : 2, dash: ens ? 'dash' : 'solid' }},
            marker: {{ size: ens ? 8 : 6 }}
        }};
    }});
    Plotly.newPlot('mae-chart-' + variable, traces, {{
        title: {{ text: maeVarLabels[variable], font: {{ size: 13, family: '-apple-system, sans-serif' }} }},
        margin: {{ t: 40, b: 24, l: 50, r: 16 }},
        xaxis: {{ tickangle: -40, tickfont: {{ size: 11 }} }},
        yaxis: {{ tickfont: {{ size: 11 }}, rangemode: 'tozero' }},
        height: 320,
        showlegend: true,
        legend: {{ orientation: 'h', x: 0, y: -0.35, font: {{ size: 11 }} }},
        paper_bgcolor: 'white',
        plot_bgcolor: '#fafafa'
    }}, {{responsive: true}});
}});
"""


def _member_detail_js(member_rows: list) -> str:
    if not member_rows:
        return "const memberData = {};"

    data: dict = {}
    for row in member_rows:
        data.setdefault(row["model"], []).append({
            "member_id": row["member_id"],
            "member_name": row["member_name"],
            "variable": row["variable"],
            "lead_hours": row["lead_hours"],
            "avg_mae": row["avg_mae"],
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
        return '<th>' + (memberVarLabels[v] || v) + '</th>';
    }}).join('');
    const bodyRows = Object.entries(members).map(function([key, varData]) {{
        const label = key.split(':')[1];
        const cells = varCols.map(function(v) {{
            const d = varData[v];
            if (!d || d.n === 0) return '<td>\u2014</td>';
            return '<td>' + (d.sum / d.n).toFixed(2) + ' ' + (memberUnits[v] || '') + '</td>';
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
        "temperature": "Temperature (\u00b0C)",
        "humidity": "Humidity (%)",
        "pressure": "Pressure (mb)",
        "wind_speed": "Wind Speed (m/s)",
    })
    vars_json = json.dumps(VARIABLES)
    return f"""\
const chartData = {data_json};
const varLabels = {var_labels_json};
const variables = {vars_json};

variables.forEach(function(variable) {{
    const varData = chartData[variable] || {{}};
    const traces = Object.entries(varData).map(function([model, d]) {{
        return {{
            type: 'scatter',
            mode: 'lines+markers',
            name: model,
            x: d.x,
            y: d.y,
            line: {{ width: 2 }},
            marker: {{ size: 6 }}
        }};
    }});
    Plotly.newPlot('chart-' + variable, traces, {{
        title: {{ text: varLabels[variable], font: {{ size: 13, family: '-apple-system, sans-serif' }} }},
        margin: {{ t: 40, b: 24, l: 50, r: 16 }},
        xaxis: {{ tickangle: -40, tickfont: {{ size: 11 }} }},
        yaxis: {{ tickfont: {{ size: 11 }} }},
        height: 320,
        showlegend: traces.length > 1,
        legend: {{ orientation: 'h', x: 0, y: -0.35, font: {{ size: 11 }} }},
        paper_bgcolor: 'white',
        plot_bgcolor: '#fafafa'
    }}, {{responsive: true}});
}});
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

    # count named members per model for the member badge
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

    lead_times = sorted({row["lead_hours"] for row in mean_rows})
    charts = _chart_data(mean_rows)
    mae_ts = _mae_timeseries_data(timeseries)
    generated_at = fmt.ts(now)

    tempest_card = _conditions_card("Tempest", tempest)
    nws_card = _conditions_card("NWS", nws)
    model_runs = _model_runs_html(mean_rows, lead_times, member_counts)
    obs_section = _obs_history_section(tempest_history, nws_history)
    tempest_rows = [_tempest_obs_row(r) for r in tempest_history]
    nws_rows = [_nws_obs_row(r) for r in nws_history]

    all_models = {r["model"]: {"model_id": r["model_id"], "type": r["type"]} for r in mean_rows}
    table_10 = _score_summary_table(summary_10, "last 10 runs", member_models, all_models)
    table_7d = _score_summary_table(summary_7d, "last 7 days", member_models, all_models)
    mae_chart_divs = "".join(
        f'<div class="chart-container"><div id="mae-chart-{v}"></div></div>'
        for v in VARIABLES
    )

    chart_divs = "".join(
        f'<div class="chart-container"><div id="chart-{v}"></div></div>'
        for v in VARIABLES
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
  <span class="generated">generated {generated_at}</span>
</header>

<section class="section">
  <h2>Latest Conditions</h2>
  <div class="conditions-grid">
    {tempest_card}
    {nws_card}
  </div>
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
  <h3 class="obs-subhead">MAE over time</h3>
  <div class="charts-grid">
    {mae_chart_divs}
  </div>
</section>

<section class="section">
  <h2>Latest Forecast Run</h2>
  <div class="model-runs">
    {model_runs}
  </div>
  <h3 class="obs-subhead">Forecast Charts</h3>
  <div class="charts-grid">
    {chart_divs}
  </div>
</section>

{obs_section}

</div>
<script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
<script>
{_chart_js(charts)}
{_obs_history_js(tempest_rows, nws_rows)}
{_mae_timeseries_js(mae_ts)}
{_member_detail_js(members_10)}
</script>
</body>
</html>
"""

    output_path.write_text(html, encoding="utf-8")
