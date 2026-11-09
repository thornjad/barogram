-- store each self-correcting model's raw mix (its output without self-correction)
-- as its own scored member, raw_mix at member id -1, and fold the ensemble's own
-- self-correction (member 101 of model 100, formerly model 101) into the same pattern.
--
-- raw_mix is what a model's self-correction learns its bias from. every member 0 row
-- existing now is a raw mix, because the self-correction blend (sc_blend.py) has not run
-- yet, so the last 60 days of member 0 (self-correction's lookback) are copied as the
-- raw_mix history. model 100 is the exception: its member 0 already includes member
-- 101's contribution, so it is not a raw mix and its raw_mix history starts empty.
-- non-positive id keeps raw_mix out of tune and the dashboard member lists.

insert or ignore into members (model_id, member_id, name) values (100, 101, 'self_correction');
update members set name = 'self_correction' where model_id = 100 and member_id = 101;

insert or ignore into members (model_id, member_id, name)
select distinct model_id, -1, 'raw_mix' from members where name = 'self_correction';

insert or ignore into forecasts
    (model_id, model, member_id, issued_at, valid_at, lead_hours, variable,
     value, spread, confidence, observed, scored_at, error, mae)
select model_id, model, -1, issued_at, valid_at, lead_hours, variable,
       value, spread, confidence, observed, scored_at, error, mae
from forecasts
where member_id = 0
  and model_id <> 100
  and issued_at >= cast(strftime('%s', 'now', '-60 days') as integer)
  and model_id in (select model_id from members where member_id = -1 and name = 'raw_mix');
