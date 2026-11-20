-- model 300, meta_forecast: the best forecast for this yard from everything barogram knows
-- (the ensemble, the external forecasts, current conditions). type 'meta' keeps it out of
-- the ensemble. this registers the hand-designed members; run() registers the 100-series
-- pass-throughs of base models itself when it emits them. raw_mix (-1) turns on the
-- self-correction blend. data only: insert or ignore into existing tables.

insert or ignore into models (id, name, type) values (300, 'meta_forecast', 'meta');

insert or ignore into members (model_id, member_id, name) values (300, 0, null);
insert or ignore into members (model_id, member_id, name) values (300, -1, 'raw_mix');

insert or ignore into members (model_id, member_id, name) values (300, 1, 'nws');
insert or ignore into members (model_id, member_id, name) values (300, 2, 'tempest_forecast');
insert or ignore into members (model_id, member_id, name) values (300, 3, 'external_corrected');
insert or ignore into members (model_id, member_id, name) values (300, 4, 'barogram_ensemble');

insert or ignore into members (model_id, member_id, name) values (300, 11, 'median_sources');
insert or ignore into members (model_id, member_id, name) values (300, 12, 'hot_hand');
insert or ignore into members (model_id, member_id, name) values (300, 13, 'spread_gated_ensemble');
insert or ignore into members (model_id, member_id, name) values (300, 14, 'ensemble_residual');
insert or ignore into members (model_id, member_id, name) values (300, 15, 'obs_anchor');
insert or ignore into members (model_id, member_id, name) values (300, 16, 'lagged_consensus');
insert or ignore into members (model_id, member_id, name) values (300, 17, 'ensemble_gated');
insert or ignore into members (model_id, member_id, name) values (300, 18, 'confidence_argmax');

insert or ignore into members (model_id, member_id, name) values (300, 91, 'self_correction');
insert or ignore into members (model_id, member_id, name) values (300, 92, 'self_correction_fast');
