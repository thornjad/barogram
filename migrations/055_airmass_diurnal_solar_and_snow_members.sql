-- airmass_diurnal members 17-20: clear-sky-index trend against the station's own
-- 30d hourly solar envelope (distinct from the astronomical clear-sky formula
-- members 1-16 already use), UV-vs-solar-radiation divergence as a speculative
-- haze/smoke proxy, early post-sunrise solar-ramp steepness, and an inferred
-- snow-cover proxy from a sub-freezing run plus precip in that run.

insert or ignore into members (model_id, member_id, name) values (7, 17, 'clearsky-envelope-trend');
insert or ignore into members (model_id, member_id, name) values (7, 18, 'uv-solar-divergence');
insert or ignore into members (model_id, member_id, name) values (7, 19, 'early-ramp-steepness');
insert or ignore into members (model_id, member_id, name) values (7, 20, 'snow-cover-proxy');
