# Barogram

A personal weather forecasting and ensemble verification system. Reads
observation data collected by a personal weather station and produces
short-range forecasts using a ladder of increasingly sophisticated methods,
combining them into a weighted ensemble scored against real observations.

My goal in building this project is explicitly about reinventing the metaphorical wheel, in order to better understand how the wheel works. It's not about making the best wheel, or even a reliable wheel. If you're interested in getting started with a forecasting model that really tries to get things right, [WRF](https://github.com/wrf-model/WRF) is a good starting point.

Caution: this project is open-source in hopes that is of interest to a few people, but not as a system which is meant to be generally runnable by anyone. The input data schema is tightly tied to an arbitrary system I invented to log data from my Tempest personal weather station, and the output format is guided by whatever I'm interested in knowing at the moment, and changes without warning, versioning or a changelog. If you want to run these models, you probably want to make a fork first to avoid unexpected changes.

Another caution: this is a pet project being built by a hobbyist, the author has a degree in computer science, not meteorology, and there is no claim that the models produce good results. That said, the author is also always looking to learn, so if you see something that could be improved, opening an issue or PR is more than welcome, especially with helpful explanation.

<p align="center">
<img width="911" height="1003" alt="image" src="https://github.com/user-attachments/assets/9f9471d7-59aa-405c-90df-18aed2c6e2f7" />
</p>

## Requirements

Python 3.11+, managed with [uv](https://github.com/astral-sh/uv).

## Setup

Copy the example config and fill in your paths:

```bash
cp barogram.example.toml barogram.toml
# edit barogram.toml
uv sync
```

```toml
[barogram]
input_db = "/path/to/wxlog-read-only.db"
output_db = "/path/to/barogram.db"
```

`input_db` points to the `wxlog-read-only.db` snapshot produced by wxlog.
`output_db` is where barogram stores forecasts and verification scores
(created automatically on first run).

To enable the Tempest forecast model (which fetches Tempest's built-in forecast
as an external reference), add a `[tempest]` section:

```toml
[tempest]
station_id = ""  # numeric Tempest station ID
token = ""       # API token from tempest.earth
```

Without this section the Tempest forecast model silently produces no output.

## Usage

```
uv run barogram [--config PATH] <command>
```

### Commands

```
run           score pending forecasts, run all models, rebuild dashboard
forecast      run forecast models and write to output database
score         score past forecasts against observations
tune          compute skill-score member weights from scoring history
dashboard     generate dashboard.html from latest forecast run
conditions    show latest observed conditions from the input database
query         run a SQL query against barogram.db or the input database
```

`query` accepts `--input` to target the wxlog database instead of barogram.db, and
`--format json` for JSON output instead of a table.

The dashboard requires internet connectivity to load Plotly from CDN.

### Typical workflow

`run` fires automatically (e.g. every 3 hours via a cron job or launchd) and handles the
score → forecast → dashboard pipeline. `tune` is a separate, infrequent step — run it
periodically once enough scoring data has accumulated to meaningfully differentiate ensemble
members. See [docs/tune.md](docs/tune.md) for details.

### Makefile

A `Makefile` wraps the common commands for convenience:

```bash
make          # equivalent to uv run barogram run
make forecast
make score
make tune     # tune weights, then rebuild dashboard
make dashboard
make conditions
make test
```

### Examples

```bash
uv run barogram run
uv run barogram conditions
uv run barogram forecast
uv run barogram score
uv run barogram tune
uv run barogram tune --dry-run
uv run barogram dashboard
uv run barogram query "select model, avg(mae) from forecasts where scored_at is not null group by model"
uv run barogram query --input "select date(timestamp, 'unixepoch', 'localtime') as day, avg(air_temp) from tempest_obs group by day order by day desc limit 7"
uv run barogram query --format json "select * from forecasts order by issued_at desc limit 20"
uv run barogram --config /path/to/barogram.toml conditions
uv run barogram --help
```

## Multi-machine sync

If you run barogram on multiple machines with the output database synced via Syncthing,
write commands (`run`, `forecast`, `score`, `tune`) will check that the local Syncthing
folder is idle before writing, reducing the chance of sync conflicts.

Create `barogram.local.toml` (gitignored and stignored) from the example:

```bash
cp barogram.example.local.toml barogram.local.toml
# edit: add your Syncthing API key and folder ID
```

The API key is in the Syncthing web UI under Actions > Settings, or in the Syncthing
config XML (`<apikey>`). Without this file the sync check is skipped entirely, so the
feature has no effect on single-machine or non-Syncthing setups.

## License

ISC — see [LICENSE](LICENSE).
