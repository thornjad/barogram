# Barogram

Personal weather forecast ensemble. Reads from wxlog's SQLite DB (read-only,
synced via Syncthing) and writes forecasts to a local barogram.db.

## Running commands

Always use `uv run barogram <command>`. Never invoke Python directly.

| Command      | Description                                          |
|--------------|------------------------------------------------------|
| `forecast`   | Run all models, write forecast rows                  |
| `score`      | Score past forecasts against observations            |
| `prune`      | Null out raw value/spread/observed/confidence past a debugging window (default 30 days) |
| `tune`       | Compute skill-score member weights from scoring history |
| `dashboard`  | Regenerate dashboard.html                            |
| `conditions` | Print latest Tempest and NWS observations            |
| `query`      | Run a SQL query against barogram.db or wxlog         |
| `insights`   | Emit forecast + accuracy summary as JSON (or `--format table`) |

There is no `run` subcommand. `make run` composes the full cycle — `score`, then
`forecast`, then `dashboard`, then `prune` — as separate steps, and stops if `forecast`
exits non-zero (it does so only when no forecast rows were written), so a stale or empty
forecast never reaches the dashboard or publish step. `make tune` runs `score` first
(its own weighting math needs freshly scored rows), then `tune`. `make full` runs
`tune` then the full `run` cycle — use it when weights are stale and a forecast is due.

`prune` only nulls `value`/`spread`/`observed`/`confidence` on already-scored rows older
than the cutoff — `error`/`mae`/`scored_at` and everything else stay forever for long-run
accuracy trends. It never deletes rows and never touches an unscored row. Freed pages
aren't reclaimed until `auto_vacuum=incremental` is enabled, which needs a one-time
full `VACUUM` on the existing db (`sqlite3 barogram.db "pragma auto_vacuum=incremental; vacuum;"`,
needs ~1.5GB free disk headroom and briefly locks the db) — `prune` runs
`pragma incremental_vacuum` every time regardless, but it's a no-op until that's done.

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
   - `NEEDS_CONN_IN = True` if the model needs historical input DB access, else omit
   - `NEEDS_CONN_OUT = True` if the model needs output DB access (ensemble, external_corrected, pressure_consensus_transfer, inverse_pressure_transfer), else omit
   - `NEEDS_WEIGHTS = True` if the model accepts skill-score member weights, else omit
   - `NEEDS_CONF = True` if the model needs the barogram config (e.g. external API URLs), else omit
   - `NEEDS_ALL_OBS = True` if the model needs the full Tempest obs history for the run (shared/computed once, not per-model), else omit
   - `NEEDS_LOCATION = True` if the model needs the Tempest station's lat/lon/elevation, else omit
   - `NEEDS_MATCH_HISTORY = True` if the model feeds per-cell confidence — nearly every model does. Adds two kwargs: `member_history` (scored error history per member, for `models/_confidence.py`'s `confidence_for_cell`) and `default_matches` (shared analog-day matches computed once via `_confidence.find_default_matches`)
   - `run(obs, issued_at, *, conn_in=None, conn_out=None, weights=None, conf=None, all_obs=None, location=None, member_history=None, default_matches=None) -> list[dict]` returning forecast dicts — accept only the kwargs your `NEEDS_*` flags request

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
    "lead_hours": int,   # 1 through 24 (hourly); see models/_climo_weights.py's LEAD_HOURS
    "variable": str,     # "temperature" | "dewpoint" | "pressure"
    "value": float | None,
    # optional — single-member models may omit; insert_forecasts applies defaults
    "member_id": int,    # default 0; 1+ for named members of a multi-member model
    "spread": float | None,  # default None; non-None only on member_id=0 rows
                             # for multi-member models (ensemble spread)
    "confidence": float | None,  # default None; per-cell confidence from
                                  # models/_confidence.py's confidence_for_cell,
                                  # written on every row (member rows and the
                                  # member_id=0 aggregate alike) — see docs/confidence.md
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
| 7   | airmass_diurnal              | base     | done   |
| 8   | analog                       | base     | done   |
| 9   | surface_signs                | base     | done   |
| 10  | synoptic_state_machine       | base     | done   |
| 12  | bogo                         | base     | done   |
| 13  | full_state_analog            | base     | done   |
| 14  | multivariate_trend           | base     | done   |
| 15  | dry_airmass_diurnal          | base     | done   |
| 16  | pressure_trend_cascade       | base     | done   |
| 17  | pressure_damped_diurnal      | base     | done   |
| 18  | pressure_consensus_transfer  | base     | done   |
| 19  | inverse_pressure_transfer    | base     | done   |
| 20  | wind_veer_detector           | base     | done   |
| 21  | frontal_trigger              | base     | done   |
| 22  | dewpoint_tendency            | base     | done   |
| 23  | solar_ramp                   | base     | done   |
| 100 | barogram_ensemble            | ensemble | done   |
| 200 | nws                          | external | done   |
| 201 | tempest_forecast             | external | done   |
| 202 | external_corrected           | external | done   |

## Database schemas

### barogram.db (output DB — read/write)

**`models`** — one row per model
| Column | Type | Notes |
|--------|------|-------|
| `id` | integer PK | model ID |
| `name` | text | unique model name |
| `type` | text | `'base'`, `'ensemble'`, or `'external'` |

**`forecasts`** — one row per (model, member, variable, lead, run)
| Column | Type | Notes |
|--------|------|-------|
| `id` | integer PK autoincrement | |
| `model_id` | integer FK → models | |
| `model` | text | denormalized name |
| `member_id` | integer | 0 = single/ensemble mean; 1+ = named members |
| `issued_at` | integer | Unix epoch of forecast run |
| `valid_at` | integer | Unix epoch of forecast target time |
| `lead_hours` | integer | 1 through 24, hourly |
| `variable` | text | `temperature`, `dewpoint`, `pressure` |
| `value` | real | forecast value (NULL = model abstained) |
| `spread` | real | std dev across members; non-NULL only on member_id=0 for multi-member models |
| `confidence` | real | per-cell confidence from `models/_confidence.py`; NULL until enough scored history exists. See [docs/confidence.md](docs/confidence.md) |
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

**`weights`** — skill-score weights computed by `tune`
| Column | Type | Notes |
|--------|------|-------|
| `model_id` | integer FK → models | |
| `member_id` | integer | 1+ only |
| `variable` | text | |
| `lead_hours` | integer | |
| `sector` | integer | 0=night(00-05), 1=morning(06-11), 2=afternoon(12-17), 3=evening(18-23) |
| `weight` | real | normalized so members in a group sum to 1 |
| `updated_at` | integer | Unix epoch of last `tune` run |

**`metadata`** — key/value store
| Key | Value |
|-----|-------|
| `schema_version` | current migration version (integer as string) |
| `last_forecast` | Unix epoch of most recent `forecast` or `run` |
| `last_prune` | Unix epoch of most recent `prune` |
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
- `sync.py` — Syncthing API integration; polls for idle state before each run
- `migrations/` — numbered SQL files, run automatically at startup
- `models/` — one file per model, plus shared helpers `_confidence.py` (per-cell confidence), `_similarity.py` (analog-day matching), `_climo_weights.py`, `_utils.py`
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
