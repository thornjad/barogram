-- storm_trajectory: new model, five members. Migration 004 (wxlog) added
-- lightning_avg_distance plus the station-derived lightning_strike_* batch,
-- making a distance-aware convective-event model buildable.

insert or ignore into models (id, name, type) values (26, 'storm_trajectory', 'base');
insert or ignore into members (model_id, member_id, name) values (26, 0, null);
insert or ignore into members (model_id, member_id, name) values (26, 1, 'lightning_accel');
insert or ignore into members (model_id, member_id, name) values (26, 2, 'precip_onset_lag');
insert or ignore into members (model_id, member_id, name) values (26, 3, 'storm_state');
insert or ignore into members (model_id, member_id, name) values (26, 4, 'precip_pressure_state');
insert or ignore into members (model_id, member_id, name) values (26, 5, 'dry_lightning_flag');
