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
@media (max-width: 600px) {
    .conditions-grid, .charts-grid { grid-template-columns: 1fr; }
}
"""


def _latest_run(conn_out: sqlite3.Connection) -> int | None:
    row = conn_out.execute("SELECT MAX(issued_at) FROM forecasts").fetchone()
    return row[0] if row and row[0] is not None else None


def _run_rows(conn_out: sqlite3.Connection, issued_at: int) -> list:
    return conn_out.execute(
        """
        SELECT variable, lead_hours, value, valid_at, model_id, model
        FROM forecasts WHERE issued_at = ?
        ORDER BY variable, model_id, lead_hours
        """,
        (issued_at,),
    ).fetchall()


def _model_name(conn_out: sqlite3.Connection, issued_at: int) -> str:
    row = conn_out.execute(
        """
        SELECT m.name FROM models m
        JOIN forecasts f ON f.model_id = m.id
        WHERE f.issued_at = ?
        LIMIT 1
        """,
        (issued_at,),
    ).fetchone()
    return row[0] if row else "unknown"


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
        margin: {{ t: 40, b: 64, l: 50, r: 16 }},
        xaxis: {{ tickangle: -40, tickfont: {{ size: 11 }} }},
        yaxis: {{ tickfont: {{ size: 11 }} }},
        height: 280,
        showlegend: traces.length > 1,
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
    issued_at = _latest_run(conn_out)
    if issued_at is None:
        raise ValueError(
            "no forecasts in output database \u2014 run barogram.py forecast first"
        )

    rows = _run_rows(conn_out, issued_at)
    model_name = _model_name(conn_out, issued_at)
    tempest = db.latest_tempest_obs(conn_in)
    nws = db.latest_nws_obs(conn_in)
    tempest_history = db.recent_tempest_obs(conn_in)
    nws_history = db.recent_nws_obs(conn_in)

    lead_times = sorted({row["lead_hours"] for row in rows})
    table = _table_data(rows)
    charts = _chart_data(rows)
    generated_at = fmt.ts(int(time.time()))

    tempest_card = _conditions_card("Tempest", tempest)
    nws_card = _conditions_card("NWS", nws)
    forecast_table = _forecast_table_html(table, lead_times)
    obs_section = _obs_history_section(tempest_history, nws_history)
    tempest_rows = [_tempest_obs_row(r) for r in tempest_history]
    nws_rows = [_nws_obs_row(r) for r in nws_history]

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
  <h2>Current Conditions</h2>
  <div class="conditions-grid">
    {tempest_card}
    {nws_card}
  </div>
</section>

{obs_section}

<section class="section">
  <h2>Latest Forecast Run</h2>
  <div class="run-meta">
    <strong>{model_name}</strong> &mdash;
    issued {fmt.ts(issued_at)}
  </div>
</section>

<section class="section">
  <h2>Forecast Table</h2>
  {forecast_table}
</section>

<section class="section">
  <h2>Forecast Charts</h2>
  <div class="charts-grid">
    {chart_divs}
  </div>
</section>

</div>
<script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
<script>
{_chart_js(charts)}
{_obs_history_js(tempest_rows, nws_rows)}
</script>
</body>
</html>
"""

    output_path.write_text(html, encoding="utf-8")
