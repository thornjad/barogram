update forecasts set variable = 'dewpoint' where variable = 'humidity';

insert or replace into metadata (key, value) values ('schema_version', '7');
