-- add window_h to members table for hypothesis H tracking
alter table members add column window_h integer;

-- backfill existing multivariate_trend members
update members set window_h = 1  where model_id = 14 and member_id = 1;
update members set window_h = 3  where model_id = 14 and member_id = 2;
update members set window_h = 6  where model_id = 14 and member_id = 3;
update members set window_h = 12 where model_id = 14 and member_id = 4;
update members set window_h = 3  where model_id = 14 and member_id = 5;
update members set window_h = 6  where model_id = 14 and member_id = 6;
update members set window_h = 6  where model_id = 14 and member_id = 7;
update members set window_h = 3  where model_id = 14 and member_id = 8;
update members set window_h = 6  where model_id = 14 and member_id = 9;
update members set window_h = 6  where model_id = 14 and member_id = 10;

-- new longer-window members
insert or ignore into members (model_id, member_id, name, window_h) values (14, 11, 'linear-18h',    18);
insert or ignore into members (model_id, member_id, name, window_h) values (14, 12, 'linear-24h',    24);
insert or ignore into members (model_id, member_id, name, window_h) values (14, 13, 'linear-36h',    36);
insert or ignore into members (model_id, member_id, name, window_h) values (14, 14, 'linear-48h',    48);
insert or ignore into members (model_id, member_id, name, window_h) values (14, 15, 'wls-18h-hl240', 18);
insert or ignore into members (model_id, member_id, name, window_h) values (14, 16, 'wls-24h-hl360', 24);
