-- standard self-correction member (models/_self_correction.py), rollout batch A:
-- weighted_climatological_mean, pressure_tendency, diurnal_curve, analog,
-- dry_airmass_diurnal, full_state_analog, multivariate_trend, surface_signs,
-- pressure_trend_cascade, pressure_damped_diurnal.

insert or ignore into members (model_id, member_id, name) values (3, 13, 'self_correction');
insert or ignore into members (model_id, member_id, name) values (5, 12, 'self_correction');
insert or ignore into members (model_id, member_id, name) values (6, 42, 'self_correction');
insert or ignore into members (model_id, member_id, name) values (8, 9, 'self_correction');
insert or ignore into members (model_id, member_id, name) values (15, 8, 'self_correction');
insert or ignore into members (model_id, member_id, name) values (13, 18, 'self_correction');
insert or ignore into members (model_id, member_id, name) values (14, 17, 'self_correction');
insert or ignore into members (model_id, member_id, name) values (9, 5, 'self_correction');
insert or ignore into members (model_id, member_id, name) values (16, 7, 'self_correction');
insert or ignore into members (model_id, member_id, name) values (17, 4, 'self_correction');
