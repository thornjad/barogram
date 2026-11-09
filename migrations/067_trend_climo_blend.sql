-- trend_climo_blend: trend-persistence basis (own trailing slope, flat
-- current value, or K-nearest-analog mean) blended toward climatology by a
-- lead-decaying weight, exploring three decay families (linear, exponential,
-- logarithmic) at three rates. Temperature and dewpoint only.

insert or ignore into models (id, name, type) values (30, 'trend_climo_blend', 'base');

insert or ignore into members (model_id, member_id, name) values (30, 0, null);
insert or ignore into members (model_id, member_id, name) values (30, 1, 'trend3_lin');
insert or ignore into members (model_id, member_id, name) values (30, 2, 'trend3_exp');
insert or ignore into members (model_id, member_id, name) values (30, 3, 'trend3_log');
insert or ignore into members (model_id, member_id, name) values (30, 4, 'trend6_lin');
insert or ignore into members (model_id, member_id, name) values (30, 5, 'trend6_exp');
insert or ignore into members (model_id, member_id, name) values (30, 6, 'trend6_log');
insert or ignore into members (model_id, member_id, name) values (30, 7, 'trend12_lin');
insert or ignore into members (model_id, member_id, name) values (30, 8, 'trend12_exp');
insert or ignore into members (model_id, member_id, name) values (30, 9, 'trend12_log');
insert or ignore into members (model_id, member_id, name) values (30, 10, 'flat_lin');
insert or ignore into members (model_id, member_id, name) values (30, 11, 'flat_exp');
insert or ignore into members (model_id, member_id, name) values (30, 12, 'flat_log');
insert or ignore into members (model_id, member_id, name) values (30, 13, 'analogk5_lin');
insert or ignore into members (model_id, member_id, name) values (30, 14, 'analogk5_exp');
insert or ignore into members (model_id, member_id, name) values (30, 15, 'analogk5_log');
insert or ignore into members (model_id, member_id, name) values (30, 16, 'analogk10_lin');
insert or ignore into members (model_id, member_id, name) values (30, 17, 'analogk10_exp');
insert or ignore into members (model_id, member_id, name) values (30, 18, 'analogk10_log');
insert or ignore into members (model_id, member_id, name) values (30, 19, 'analogk20_lin');
insert or ignore into members (model_id, member_id, name) values (30, 20, 'analogk20_exp');
insert or ignore into members (model_id, member_id, name) values (30, 21, 'analogk20_log');
insert or ignore into members (model_id, member_id, name) values (30, 22, 'flat_lin_fast');
insert or ignore into members (model_id, member_id, name) values (30, 23, 'flat_lin_slow');
insert or ignore into members (model_id, member_id, name) values (30, 24, 'flat_exp_fast');
insert or ignore into members (model_id, member_id, name) values (30, 25, 'flat_exp_slow');
insert or ignore into members (model_id, member_id, name) values (30, 26, 'flat_log_fast');
insert or ignore into members (model_id, member_id, name) values (30, 27, 'flat_log_slow');
insert or ignore into members (model_id, member_id, name) values (30, 28, 'climo_only');
insert or ignore into members (model_id, member_id, name) values (30, 29, 'self_correction');
