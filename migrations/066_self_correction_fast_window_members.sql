-- fast-window variant of the standard self-correction member
-- (models/_self_correction.py, window_days=14 instead of the default 60),
-- added to climo_deviation and weighted_climatological_mean only -- the two
-- models where the 60-day window's seasonal-transition lag was the measured
-- finding, not a blanket rollout to every model.

insert or ignore into members (model_id, member_id, name) values (3, 14, 'self_correction_fast');
insert or ignore into members (model_id, member_id, name) values (4, 56, 'self_correction_fast');
