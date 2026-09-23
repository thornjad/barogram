-- pressure_trajectory: new model, five members. Pressure classified/extrapolated
-- as an intermediate signal, transferred to temperature/dewpoint, same philosophy
-- as pressure_tendency/pressure_trend_cascade/pressure_consensus_transfer.

insert or ignore into models (id, name, type) values (27, 'pressure_trajectory', 'base');
insert or ignore into members (model_id, member_id, name) values (27, 0, null);
insert or ignore into members (model_id, member_id, name) values (27, 1, 'pressure_jerk');
insert or ignore into members (model_id, member_id, name) values (27, 2, 'trend_agreement');
insert or ignore into members (model_id, member_id, name) values (27, 3, 'post_frontal_ringing');
insert or ignore into members (model_id, member_id, name) values (27, 4, 'days_since_front');
insert or ignore into members (model_id, member_id, name) values (27, 5, 'front_phase_state');
