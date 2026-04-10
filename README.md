# barogram

A personal weather forecasting and ensemble verification system. Reads
observation data collected by a personal weather station and produces
short-range forecasts using a ladder of increasingly sophisticated methods,
combining them into a weighted ensemble scored against real observations.

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
conditions    show latest observed conditions from the input database
forecast      run forecast models and write to output database
dashboard     generate dashboard.html from latest forecast run
```

The dashboard requires internet connectivity to load Plotly from CDN.

### Examples

```bash
uv run barogram conditions
uv run barogram forecast
uv run barogram dashboard
uv run barogram --config /path/to/barogram.toml conditions
uv run barogram --help
```

## License

ISC — see [LICENSE](LICENSE).
