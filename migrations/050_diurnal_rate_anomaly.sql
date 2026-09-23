-- diurnal_rate_anomaly: new model, two members. Is a signal changing faster or
-- slower than its own climatological rate/level for this (month, hour), rather
-- than whether its current level is anomalous.

insert or ignore into models (id, name, type) values (25, 'diurnal_rate_anomaly', 'base');
insert or ignore into members (model_id, member_id, name) values (25, 0, null);
insert or ignore into members (model_id, member_id, name) values (25, 1, 'temp_slope_anomaly');
insert or ignore into members (model_id, member_id, name) values (25, 2, 'wind_rate_anomaly');
