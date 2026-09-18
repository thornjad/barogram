-- new members on pressure_trend_cascade and wind_veer_detector, plus a new
-- solar_ramp model, all aimed at the 2026-09-16 09:00 run: pressure_trend_cascade
-- and wind_veer_detector both predicted a temp drop on a strong clear-sky heating
-- day because their transfer functions/classifiers pool history without regard
-- to solar heating in progress. See those models' module-level comments.

insert or ignore into members (model_id, member_id, name) values (16, 5, 'sector_conditioned_extrap');
insert or ignore into members (model_id, member_id, name) values (16, 6, 'solar_gated_extrap');

insert or ignore into members (model_id, member_id, name) values (20, 4, 'veer_solar_gated');
insert or ignore into members (model_id, member_id, name) values (20, 5, 'veer_hour_gated');

insert or ignore into models (id, name, type) values (23, 'solar_ramp', 'base');
insert or ignore into members (model_id, member_id, name) values (23, 0, null);
insert or ignore into members (model_id, member_id, name) values (23, 1, 'solar_temp_transfer');
insert or ignore into members (model_id, member_id, name) values (23, 2, 'dewpoint_depression_ramp');
