insert or ignore into models (id, name, type) values (16, 'pressure_trend_cascade', 'base');
insert or ignore into members (model_id, member_id, name) values (16, 0, null);
insert or ignore into members (model_id, member_id, name) values (16, 1, 'linear_extrap');
insert or ignore into members (model_id, member_id, name) values (16, 2, 'quad_extrap');
insert or ignore into members (model_id, member_id, name) values (16, 3, 'damped_extrap');

insert or ignore into models (id, name, type) values (17, 'pressure_damped_diurnal', 'base');
insert or ignore into members (model_id, member_id, name) values (17, 0, null);
insert or ignore into members (model_id, member_id, name) values (17, 1, 'linear_damp');
insert or ignore into members (model_id, member_id, name) values (17, 2, 'threshold_damp');
insert or ignore into members (model_id, member_id, name) values (17, 3, 'airmass_pressure_joint');

insert or ignore into models (id, name, type) values (18, 'pressure_consensus_transfer', 'base');
insert or ignore into members (model_id, member_id, name) values (18, 0, null);
insert or ignore into members (model_id, member_id, name) values (18, 1, 'simple_mean_consensus');
insert or ignore into members (model_id, member_id, name) values (18, 2, 'spread_aware');
insert or ignore into members (model_id, member_id, name) values (18, 3, 'best_model_only');

insert or ignore into models (id, name, type) values (19, 'inverse_pressure_transfer', 'base');
insert or ignore into members (model_id, member_id, name) values (19, 0, null);
insert or ignore into members (model_id, member_id, name) values (19, 1, 'temp_only_inverse');
insert or ignore into members (model_id, member_id, name) values (19, 2, 'dewpoint_only_inverse');
insert or ignore into members (model_id, member_id, name) values (19, 3, 'joint_inverse');
