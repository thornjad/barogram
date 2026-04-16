# Barogram

Personal weather forecast ensemble. Reads from wxlog's SQLite DB (read-only,
synced via Syncthing) and writes forecasts to a local barogram.db.

## Running commands

Always use `uv run barogram <command>`. Never invoke Python directly.

| Command      | Description                                          |
|--------------|------------------------------------------------------|
| `run`        | Score pending forecasts, run all models, rebuild dashboard |
| `forecast`   | Run all models, write forecast rows                  |
| `score`      | Score past forecasts against observations            |
| `tune`       | Compute inverse-MAE member weights from scoring history |
| `dashboard`  | Regenerate dashboard.html                            |
| `conditions` | Print latest Tempest and NWS observations            |
| `query`      | Run a SQL query against barogram.db or wxlog         |

### Data investigation with `query`

Use `query` to investigate patterns without writing custom Python. DB paths come from `barogram.toml` automatically.

```bash
# query barogram.db (default)
uv run barogram query "select model, variable, lead_hours, avg(mae) as avg_mae from forecasts where scored_at is not null group by model, variable, lead_hours order by avg_mae"

# query the wxlog input DB
uv run barogram query --input "select date(timestamp, 'unixepoch', 'localtime') as day, avg(air_temp) from tempest_obs group by day order by day desc limit 30"

# JSON output for richer analysis
uv run barogram query --format json "select * from forecasts where scored_at is not null order by issued_at desc limit 20"
```

Flags: `--input` targets wxlog; `--format json` emits JSON instead of a table.

## Adding a model

1. Create `models/<name>.py` with:
   - `MODEL_ID: int` — next unused ID
   - `MODEL_NAME: str`
   - `NEEDS_CONN_IN = True` if the model needs historical DB access, else omit
   - `NEEDS_WEIGHTS = True` if the model accepts inverse-MAE member weights, else omit
   - `run(obs, issued_at, *, conn_in=None, weights=None) -> list[dict]` returning forecast dicts

2. Add an `insert or ignore` for the model row to `migrations/001_baseline.sql` (models
   table) and a row for each member to the members table. Single-member models need one
   members row: `(model_id, 0, null)`.

3. Add the model to `_MODELS` in `barogram.py`.

4. Add a doc page `docs/00N_<name>.md` and a row to `docs/README.md`.

### Forecast dict keys

Every dict returned by `run()` must have these keys:

```python
{
    "model_id": int,
    "model": str,
    "issued_at": int,    # unix epoch
    "valid_at": int,     # unix epoch
    "lead_hours": int,   # one of [6, 12, 18, 24]
    "variable": str,     # "temperature" | "humidity" | "pressure" | "wind_speed"
    "value": float | None,
    # optional — single-member models may omit; insert_forecasts applies defaults
    "member_id": int,    # default 0; 1+ for named members of a multi-member model
    "spread": float | None,  # default None; non-None only on member_id=0 rows
                             # for multi-member models (ensemble spread)
}
```

`value=None` is valid — the scoring engine skips those rows.

### Multi-member models

A multi-member model produces one batch of rows per member (member_id 1+), plus a
member_id=0 row per (lead_hours, variable) holding the ensemble mean as `value` and the
ensemble spread as `spread`. All members share the same `issued_at`.

Register each `(model_id, member_id, name)` pair in the members table in
`migrations/001_baseline.sql`. Member names should be short descriptive labels
(e.g. `"week-heavy"`, `"exponential"`). The member_id=0 row has `name=null`.

## SQL style

All SQL keywords must be lowercase — `select`, `insert`, `create table`, `where`, `join`,
`order by`, etc. This applies to both `.sql` migration files and inline SQL strings in
Python. Data types (`integer`, `text`, `real`) and functions (`avg`, `count`, `cast`) are
lowercase as well.

## Model inventory

| ID  | Name                         | Type     | Status |
|-----|------------------------------|----------|--------|
| 1   | persistence                  | base     | done   |
| 2   | climatological_mean          | base     | done   |
| 3   | weighted_climatological_mean | base     | done   |
| 4   | climo_deviation              | base     | done   |
| 5   | pressure_tendency            | base     | done   |
| 6   | diurnal_curve                | base     | done   |
| 100 | barogram_ensemble            | ensemble | done   |

## Database schemas

### barogram.db (output DB — read/write)

**`models`** — one row per model
| Column | Type | Notes |
|--------|------|-------|
| `id` | integer PK | model ID |
| `name` | text | unique model name |
| `type` | text | `'base'` or `'ensemble'` |

**`forecasts`** — one row per (model, member, variable, lead, run)
| Column | Type | Notes |
|--------|------|-------|
| `id` | integer PK autoincrement | |
| `model_id` | integer FK → models | |
| `model` | text | denormalized name |
| `member_id` | integer | 0 = single/ensemble mean; 1+ = named members |
| `issued_at` | integer | Unix epoch of forecast run |
| `valid_at` | integer | Unix epoch of forecast target time |
| `lead_hours` | integer | one of 6, 12, 18, 24 |
| `variable` | text | `temperature`, `dewpoint`, `pressure`, `wind_speed` |
| `value` | real | forecast value (NULL = model abstained) |
| `spread` | real | std dev across members; non-NULL only on member_id=0 for multi-member models |
| `observed` | real | filled by scorer; actual observed value |
| `error` | real | filled by scorer; signed error (forecast − observed) |
| `mae` | real | filled by scorer; absolute error |
| `scored_at` | integer | Unix epoch when scored; NULL = not yet scored |

**`members`** — registry of valid (model_id, member_id) pairs
| Column | Type | Notes |
|--------|------|-------|
| `model_id` | integer FK → models | |
| `member_id` | integer | 0 = ensemble mean / single member |
| `name` | text | short label (NULL for member_id=0) |

**`weights`** — inverse-MAE weights computed by `tune`
| Column | Type | Notes |
|--------|------|-------|
| `model_id` | integer FK → models | |
| `member_id` | integer | 1+ only |
| `variable` | text | |
| `lead_hours` | integer | |
| `weight` | real | normalized so members in a group sum to 1 |
| `updated_at` | integer | Unix epoch of last `tune` run |

**`metadata`** — key/value store
| Key | Value |
|-----|-------|
| `schema_version` | current migration version (integer as string) |
| `last_forecast` | Unix epoch of most recent `forecast` or `run` |
| `last_tune` | Unix epoch of most recent `tune` |

### wxlog-read-only.db (input DB — read-only)

**`tempest_obs`** — Tempest PWS observations (~5 min cadence)
| Column | Type | Notes |
|--------|------|-------|
| `station_id` | text | Tempest device ID |
| `timestamp` | integer | Unix epoch |
| `air_temp` | real | °C |
| `dew_point` | real | °C |
| `station_pressure` | real | hPa (not sea-level adjusted) |
| `wind_avg` | real | m/s |
| `wind_gust` | real | m/s |
| `wind_direction` | real | degrees |
| `precip_accum_day` | real | mm since midnight local |
| `solar_radiation` | real | W/m² |
| `uv_index` | real | |
| `lightning_count` | integer | |

**`nws_obs`** — NWS ASOS observations (~hourly)
| Column | Type | Notes |
|--------|------|-------|
| `station_id` | text | ICAO station ID |
| `timestamp` | integer | Unix epoch |
| `air_temp` | real | °C |
| `dew_point` | real | °C |
| `wind_speed` | real | m/s |
| `wind_direction` | real | degrees |
| `sea_level_pressure` | real | hPa (SLP) |
| `sky_cover` | text | e.g. `'CLR'`, `'FEW'`, `'OVC'` |
| `raw_metar` | text | |

**`stations`** — station metadata
| Column | Type | Notes |
|--------|------|-------|
| `station_id` | text PK | |
| `source` | text | `'tempest'` or `'nws'` |
| `name` | text | human-readable name |
| `latitude` | real | |
| `longitude` | real | |
| `elevation` | real | m ASL (ground elevation) |
| `agl` | real | m above ground level (sensor height) |

All timestamps are Unix epoch integers (seconds since 1970-01-01 UTC). Use `datetime(timestamp, 'unixepoch', 'localtime')` in SQLite queries to convert to local time.

## Playwright / screenshots

When using Playwright (MCP tools or the scripts in `screenshots/`) always save screenshots
to `/tmp/barogram-screenshots/`. Never save PNGs inside the repo directory.

The `screenshots/` directory contains capture scripts (`capture.js`, `capture-states.js`,
`zoom-*.js`) that are gitignored along with the directory itself. Each script already
creates `/tmp/barogram-screenshots/` and writes there. Run them with:

```bash
node screenshots/capture.js
```

## Key files

- `barogram.py` — CLI entry point; `_MODELS` list controls which models run
- `db.py` — all database access; input DB is read-only, output DB is read-write
- `config.py` — loads `barogram.toml`
- `score.py` — matches forecasts to observations within ±30 min
- `dashboard.py` — generates `dashboard.html`
- `fmt.py` — shared formatting helpers
- `migrations/` — numbered SQL files, run automatically at startup
- `models/` — one file per model
- `docs/` — one Markdown doc per model plus `README.md` index and `database.md` (schema evolution rules)

## Config

`barogram.toml` is gitignored. Copy from `barogram.example.toml` and set:

```toml
[barogram]
input_db = "/path/to/wxlog-read-only.db"
output_db = "/path/to/barogram.db"
```

## Setup on a new machine

```bash
git clone https://github.com/thornjad/barogram
uv sync
cp barogram.example.toml barogram.toml            # then edit paths
cp barogram.example.local.toml barogram.local.toml  # then add Syncthing API key + folder ID
uv run barogram conditions                        # verify
```

`barogram.local.toml` is machine-specific and gitignored/stignored. The Syncthing API key
is at `~/Library/Application Support/Syncthing/config.xml` (`<apikey>`), or in the
Syncthing web UI under Actions > Settings. The folder ID for the thornlog folder is
visible next to the folder name in the web UI.
