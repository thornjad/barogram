# Barogram

A personal weather forecasting and ensemble verification system. Reads
observation data collected by a personal weather station and produces
short-range forecasts using a ladder of increasingly sophisticated methods,
combining them into a weighted ensemble scored against real observations.

My goal in building this project is explicitly about reinventing the metaphorical wheel, in order to better understand how the wheel works. It's not about making the best wheel, or even a reliable wheel. If you're interested in getting started with a forecasting model that really tries to get things right, [WRF](https://github.com/wrf-model/WRF) is a good starting point.

Caution: this project is open-source in hopes that is of interest to a few people, but not as a system which is meant to be generally runnable by anyone. The input data schema is tightly tied to an arbitrary system I invented to log data from my Tempest personal weather station, and the output format is guided by whatever I'm interested in knowing at the moment, and changes without warning, versioning or a changelog. If you want to run these models, you probably want to make a fork first to avoid unexpected changes.

Another caution: this is a pet project being built by a hobbyist, the author has a degree in computer science, not meteorology, and there is no claim that the models produce good results. That said, the author is also always looking to learn, so if you see something that could be improved, opening an issue or PR is more than welcome, especially with helpful explanation.

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

## Usage

```
uv run barogram [--config PATH] <command>
```

### Commands

```
run           score pending forecasts, run all models, rebuild dashboard
forecast      run forecast models and write to output database
score         score past forecasts against observations
dashboard     generate dashboard.html from latest forecast run
conditions    show latest observed conditions from the input database
```

The dashboard requires internet connectivity to load Plotly from CDN.

### Makefile

A `Makefile` wraps the common commands for convenience:

```bash
make          # equivalent to uv run barogram run
make forecast
make score
make dashboard
make conditions
```

### Examples

```bash
uv run barogram run
uv run barogram conditions
uv run barogram forecast
uv run barogram dashboard
uv run barogram --config /path/to/barogram.toml conditions
uv run barogram --help
```

## License

ISC — see [LICENSE](LICENSE).
