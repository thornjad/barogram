-- standard self-correction member (models/_self_correction.py), rollout batch B:
-- wind_veer_detector, frontal_trigger, regime_stability, diurnal_rate_anomaly,
-- storm_trajectory, pressure_trajectory, pressure_consensus_transfer,
-- inverse_pressure_transfer, radiational_cooling, moisture_trajectory,
-- synoptic_state_machine, airmass_diurnal.

insert or ignore into members (model_id, member_id, name) values (20, 6, 'self_correction');
insert or ignore into members (model_id, member_id, name) values (21, 5, 'self_correction');
insert or ignore into members (model_id, member_id, name) values (24, 2, 'self_correction');
insert or ignore into members (model_id, member_id, name) values (25, 3, 'self_correction');
insert or ignore into members (model_id, member_id, name) values (26, 6, 'self_correction');
insert or ignore into members (model_id, member_id, name) values (27, 6, 'self_correction');
insert or ignore into members (model_id, member_id, name) values (18, 4, 'self_correction');
insert or ignore into members (model_id, member_id, name) values (19, 4, 'self_correction');
insert or ignore into members (model_id, member_id, name) values (28, 4, 'self_correction');
insert or ignore into members (model_id, member_id, name) values (29, 6, 'self_correction');
insert or ignore into members (model_id, member_id, name) values (10, 97, 'self_correction');
insert or ignore into members (model_id, member_id, name) values (7, 21, 'self_correction');
