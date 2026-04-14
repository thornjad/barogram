insert or ignore into models (id, name, type) values (3, 'weighted_climatological_mean', 'base');

insert or ignore into members (model_id, member_id, name) values (3, 0, null);
insert or ignore into members (model_id, member_id, name) values (3, 1, 'today-only');
insert or ignore into members (model_id, member_id, name) values (3, 2, 'week-only');
insert or ignore into members (model_id, member_id, name) values (3, 3, 'month-only');
insert or ignore into members (model_id, member_id, name) values (3, 4, 'week+month');
insert or ignore into members (model_id, member_id, name) values (3, 5, 'today+week+month');
insert or ignore into members (model_id, member_id, name) values (3, 6, 'exp-steep');
insert or ignore into members (model_id, member_id, name) values (3, 7, 'exp-fast');
insert or ignore into members (model_id, member_id, name) values (3, 8, 'exp-moderate');
insert or ignore into members (model_id, member_id, name) values (3, 9, 'exp-gentle');
