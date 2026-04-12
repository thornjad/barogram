CREATE TABLE IF NOT EXISTS members (
    model_id   INTEGER NOT NULL REFERENCES models(id),
    member_id  INTEGER NOT NULL DEFAULT 0,
    name       TEXT,
    PRIMARY KEY (model_id, member_id)
);

INSERT OR IGNORE INTO members (model_id, member_id, name) VALUES (1, 0, NULL);
INSERT OR IGNORE INTO members (model_id, member_id, name) VALUES (2, 0, NULL);
INSERT OR IGNORE INTO members (model_id, member_id, name) VALUES (100, 0, NULL);

ALTER TABLE forecasts ADD COLUMN member_id INTEGER NOT NULL DEFAULT 0;
ALTER TABLE forecasts ADD COLUMN spread REAL;

DROP INDEX IF EXISTS idx_forecasts_lookup;
CREATE INDEX IF NOT EXISTS idx_forecasts_lookup
    ON forecasts (model_id, member_id, variable, valid_at);
