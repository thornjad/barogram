-- moisture_trajectory: new model, five members. Reads relative_humidity and
-- delta_t (both station-derived, partial coverage on the latter) as their own
-- moisture-trend dimensions, distinct from the dewpoint-depression level every
-- other model already scales on.

insert or ignore into models (id, name, type) values (29, 'moisture_trajectory', 'base');
insert or ignore into members (model_id, member_id, name) values (29, 0, null);
insert or ignore into members (model_id, member_id, name) values (29, 1, 'rh_dd_divergence');
insert or ignore into members (model_id, member_id, name) values (29, 2, 'moisture_convergence');
insert or ignore into members (model_id, member_id, name) values (29, 3, 'dd_closing_rate');
insert or ignore into members (model_id, member_id, name) values (29, 4, 'saturation_countdown');
insert or ignore into members (model_id, member_id, name) values (29, 5, 'delta_t_trend');
