CREATE TABLE IF NOT EXISTS models (
    id   INTEGER PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    type TEXT NOT NULL DEFAULT 'base'
);

INSERT OR IGNORE INTO models (id, name, type) VALUES (1, 'persistence', 'base');
INSERT OR IGNORE INTO models (id, name, type) VALUES (2, 'climatological_mean', 'base');
INSERT OR IGNORE INTO models (id, name, type) VALUES (3, 'weighted_climatological_mean', 'base');
INSERT OR IGNORE INTO models (id, name, type) VALUES (100, 'ensemble', 'ensemble');

CREATE TABLE IF NOT EXISTS forecasts (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    model_id    INTEGER NOT NULL REFERENCES models(id),
    model       TEXT    NOT NULL,  -- denormalized for query convenience
    issued_at   INTEGER NOT NULL,
    valid_at    INTEGER NOT NULL,
    lead_hours  INTEGER NOT NULL,
    variable    TEXT    NOT NULL,
    value       REAL,
    observed    REAL,
    scored_at   INTEGER,
    error       REAL,
    mae         REAL
);

CREATE INDEX IF NOT EXISTS idx_forecasts_lookup
    ON forecasts (model_id, variable, valid_at);

CREATE INDEX IF NOT EXISTS idx_forecasts_issued
    ON forecasts (issued_at);

CREATE INDEX IF NOT EXISTS idx_forecasts_scoring
    ON forecasts (valid_at, scored_at);
