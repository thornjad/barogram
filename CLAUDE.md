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
| `dashboard`  | Regenerate dashboard.html                            |
| `conditions` | Print latest Tempest and NWS observations            |

## Adding a model

1. Create `models/<name>.py` with:
   - `MODEL_ID: int` — next unused ID
   - `MODEL_NAME: str`
   - `NEEDS_CONN_IN = True` if the model needs historical DB access, else omit
   - `run(obs, issued_at, *, conn_in=None) -> list[dict]` returning forecast dicts

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
cp barogram.example.toml barogram.toml  # then edit paths
uv run barogram conditions              # verify
```
