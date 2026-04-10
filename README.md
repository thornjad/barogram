# barogram

A personal weather forecasting and ensemble verification system. Reads
observation data collected by a personal weather station and produces
short-range forecasts using a ladder of increasingly sophisticated methods,
combining them into a weighted ensemble scored against real observations.

## Requirements

Python 3.11+. No third-party packages.

## Setup

Copy the example config and fill in your paths:

```bash
cp barogram.example.toml barogram.toml
# edit barogram.toml
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
python3 barogram.py [--config PATH] <command>
```

### Commands

```
conditions    show latest observed conditions from the input database
```

### Examples

```bash
python3 barogram.py conditions
python3 barogram.py --config /path/to/barogram.toml conditions
python3 barogram.py --help
```

## License

ISC — see [LICENSE](LICENSE).
