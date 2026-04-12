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
   - `MODEL_ID: int` — next unused ID (base models: 1–99, ensemble: 100)
   - `MODEL_NAME: str`
   - `NEEDS_CONN_IN = True` if the model needs historical DB access, else omit
   - `run(obs, issued_at, *, conn_in=None) -> list[dict]` returning forecast dicts

2. Add a migration `migrations/00N_<name>.sql` seeding the model row:
   ```sql
   INSERT OR IGNORE INTO models (id, name, type) VALUES (N, '<name>', 'base');
   ```

3. Add the model to `_MODELS` in `barogram.py`.

4. Add a doc page `docs/00N_<name>.md` and a row to `docs/README.md`.

### Forecast dict keys

Every dict returned by `run()` must have these keys:

```python
{
    "model_id": int,
    "model": str,
    "issued_at": int,   # unix epoch
    "valid_at": int,    # unix epoch
    "lead_hours": int,  # one of [6, 12, 18, 24]
    "variable": str,    # "temperature" | "humidity" | "pressure" | "wind_speed"
    "value": float | None,
}
```

`value=None` is valid — the scoring engine skips those rows.

## Model inventory

| ID  | Name                  | Type     | Status |
|-----|-----------------------|----------|--------|
| 1   | persistence           | base     | done   |
| 2   | climatological_mean   | base     | done   |
| 100 | ensemble              | ensemble | stub   |

## Key files

- `barogram.py` — CLI entry point; `_MODELS` list controls which models run
- `db.py` — all database access; input DB is read-only, output DB is read-write
- `config.py` — loads `barogram.toml`
- `score.py` — matches forecasts to observations within ±30 min
- `dashboard.py` — generates `dashboard.html`
- `fmt.py` — shared formatting helpers
- `migrations/` — numbered SQL files, run automatically at startup
- `models/` — one file per model
- `docs/` — one Markdown doc per model plus `README.md` index

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
