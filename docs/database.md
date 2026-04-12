# Database Management

## Schema evolution

Barogram uses numbered SQL migration files in `migrations/` applied in sequence at startup via `db.run_migrations()`. The version counter is stored in the `metadata` table.

### Adding a schema change

Write the change as a new migration file: `migrations/00N_<description>.sql`, where N follows the highest existing number. Run the app to apply it. The file stays in the repo so that anyone bootstrapping a fresh DB can replay history.

For seed data (inserting new model rows), prefer adding the `INSERT OR IGNORE` directly to the baseline rather than creating a migration file — see below.

### Squashing migrations

When the file count becomes unwieldy, squash all applied migrations into a new `001_baseline.sql` that creates the full current schema and seeds all current model rows. Delete the old files. New changes resume at `002`.

The baseline is both the bootstrap for fresh installs and the readable schema document — no separate schema file to keep in sync.

**Before deleting old migration files**, validate the baseline against the live DB:

```bash
python3 -c "
import sqlite3, tomllib
with open('barogram.toml', 'rb') as f:
    conf = tomllib.load(f)
conn = sqlite3.connect(conf['barogram']['output_db'])
for row in conn.execute(\"SELECT name, sql FROM sqlite_master WHERE type IN ('table','index') AND name NOT LIKE 'sqlite_%' AND name != 'metadata' ORDER BY name\"):
    print(row[0])
    print(row[1])
    print()
"
```

Compare the output against `001_baseline.sql`. The `metadata` table and `sqlite_*` internals are excluded — they are not part of the baseline. The `models` table may show ALTER TABLE artifacts in the live DB (e.g., `name TEXT NOT NULL UNIQUE\n, type ...`); the baseline's clean `CREATE TABLE` form is the correct target for fresh installs. If the seed data or any column differs, fix the baseline before deleting the old files.

**Constraint**: any DB older than the squash point cannot be upgraded by replaying migrations. Since barogram is a single-user tool synced via Syncthing, coordinate a squash only when all instances are current.

### When to use each migration type

| Change | Approach |
|--------|----------|
| New table or column | New numbered migration file |
| New model row | `INSERT OR IGNORE` in `001_baseline.sql` directly |
| Structural restructure (SQLite can't ALTER) | New table, `INSERT INTO new SELECT FROM old`, drop old, rename |
| Accumulated file count is annoying | Squash into new baseline |

## Fresh install

On a new machine, `db.run_migrations()` starts from version 0 and applies all files in order. The baseline covers the full schema, so a fresh DB is always ready after a single pass.
