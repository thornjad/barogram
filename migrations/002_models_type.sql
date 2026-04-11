ALTER TABLE models ADD COLUMN type TEXT NOT NULL DEFAULT 'base';

INSERT OR IGNORE INTO models (id, name, type) VALUES (100, 'ensemble', 'ensemble');
