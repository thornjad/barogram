UPDATE forecasts SET variable = 'dewpoint' WHERE variable = 'humidity';

INSERT OR REPLACE INTO metadata (key, value) VALUES ('schema_version', '7');
