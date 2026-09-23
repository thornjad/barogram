-- full_state_analog members 14-17: candidate-pool variants (seasonal window,
-- pressure-regime gating, trend-vector matching, full snapshot+trend lookup),
-- distinct from the plain K-value/feature-subset members registered by
-- migrations/033_full_state_analog.sql and 047_full_state_analog_k_members.sql.

insert or ignore into members (model_id, member_id, name) values (13, 14, 'seasonal-window');
insert or ignore into members (model_id, member_id, name) values (13, 15, 'regime-gated');
insert or ignore into members (model_id, member_id, name) values (13, 16, 'trajectory-analog');
insert or ignore into members (model_id, member_id, name) values (13, 17, 'full-fingerprint-lookup');
