-- new members on existing models, plus three new models, all aimed at the
-- 2026-09-12 dry-airmass-intrusion precursors (pressure tendency, wind veer,
-- dewpoint's own trailing trend) that the ensemble missed that run.

insert or ignore into members (model_id, member_id, name) values (1, 1, 'trend_persistence');

insert or ignore into members (model_id, member_id, name) values (15, 7, '24h-amp-damped');

insert or ignore into members (model_id, member_id, name) values (16, 4, 'fast_damped_extrap');

insert or ignore into models (id, name, type) values (20, 'wind_veer_detector', 'base');
insert or ignore into members (model_id, member_id, name) values (20, 0, null);
insert or ignore into members (model_id, member_id, name) values (20, 1, 'veer_nogate');
insert or ignore into members (model_id, member_id, name) values (20, 2, 'veer_lowgate');
insert or ignore into members (model_id, member_id, name) values (20, 3, 'veer_gust_confirmed');

insert or ignore into models (id, name, type) values (21, 'frontal_trigger', 'base');
insert or ignore into members (model_id, member_id, name) values (21, 0, null);
insert or ignore into members (model_id, member_id, name) values (21, 1, 'ptend_veer_strict');
insert or ignore into members (model_id, member_id, name) values (21, 2, 'ptend_veer_loose');
insert or ignore into members (model_id, member_id, name) values (21, 3, 'ptend_veer_weighted');

insert or ignore into models (id, name, type) values (22, 'dewpoint_tendency', 'base');
insert or ignore into members (model_id, member_id, name) values (22, 0, null);
insert or ignore into members (model_id, member_id, name) values (22, 1, 'linear_1h');
insert or ignore into members (model_id, member_id, name) values (22, 2, 'linear_3h');
insert or ignore into members (model_id, member_id, name) values (22, 3, 'linear_3h_hl45');
