-- standard self-correction member (models/_self_correction.py), first rollout:
-- climo_deviation member 55, dewpoint_tendency member 4, solar_ramp member 3.

insert or ignore into members (model_id, member_id, name) values (4, 55, 'self_correction');
insert or ignore into members (model_id, member_id, name) values (22, 4, 'self_correction');
insert or ignore into members (model_id, member_id, name) values (23, 3, 'self_correction');
