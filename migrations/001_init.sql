CREATE TABLE models (
    id   INTEGER PRIMARY KEY,
    name TEXT NOT NULL UNIQUE
);

INSERT INTO models (id, name) VALUES (1, 'persistence');

CREATE TABLE forecasts (
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

CREATE INDEX idx_forecasts_lookup
    ON forecasts (model_id, variable, valid_at);

CREATE INDEX idx_forecasts_issued
    ON forecasts (issued_at);

CREATE INDEX idx_forecasts_scoring
    ON forecasts (valid_at, scored_at);
