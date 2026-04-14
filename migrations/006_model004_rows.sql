insert or ignore into models (id, name, type) values (4, 'climo_deviation', 'base');

insert or ignore into members (model_id, member_id, name) values (4, 0, null);
-- static group (IDs 1-9)
insert or ignore into members (model_id, member_id, name) values (4, 1,  's-today-only');
insert or ignore into members (model_id, member_id, name) values (4, 2,  's-week-only');
insert or ignore into members (model_id, member_id, name) values (4, 3,  's-month-only');
insert or ignore into members (model_id, member_id, name) values (4, 4,  's-week+month');
insert or ignore into members (model_id, member_id, name) values (4, 5,  's-today+week+month');
insert or ignore into members (model_id, member_id, name) values (4, 6,  's-exp-steep');
insert or ignore into members (model_id, member_id, name) values (4, 7,  's-exp-fast');
insert or ignore into members (model_id, member_id, name) values (4, 8,  's-exp-moderate');
insert or ignore into members (model_id, member_id, name) values (4, 9,  's-exp-gentle');
-- decay k=0.03 group (IDs 10-18)
insert or ignore into members (model_id, member_id, name) values (4, 10, 'd03-today-only');
insert or ignore into members (model_id, member_id, name) values (4, 11, 'd03-week-only');
insert or ignore into members (model_id, member_id, name) values (4, 12, 'd03-month-only');
insert or ignore into members (model_id, member_id, name) values (4, 13, 'd03-week+month');
insert or ignore into members (model_id, member_id, name) values (4, 14, 'd03-today+week+month');
insert or ignore into members (model_id, member_id, name) values (4, 15, 'd03-exp-steep');
insert or ignore into members (model_id, member_id, name) values (4, 16, 'd03-exp-fast');
insert or ignore into members (model_id, member_id, name) values (4, 17, 'd03-exp-moderate');
insert or ignore into members (model_id, member_id, name) values (4, 18, 'd03-exp-gentle');
-- decay k=0.05 group (IDs 19-27)
insert or ignore into members (model_id, member_id, name) values (4, 19, 'd05-today-only');
insert or ignore into members (model_id, member_id, name) values (4, 20, 'd05-week-only');
insert or ignore into members (model_id, member_id, name) values (4, 21, 'd05-month-only');
insert or ignore into members (model_id, member_id, name) values (4, 22, 'd05-week+month');
insert or ignore into members (model_id, member_id, name) values (4, 23, 'd05-today+week+month');
insert or ignore into members (model_id, member_id, name) values (4, 24, 'd05-exp-steep');
insert or ignore into members (model_id, member_id, name) values (4, 25, 'd05-exp-fast');
insert or ignore into members (model_id, member_id, name) values (4, 26, 'd05-exp-moderate');
insert or ignore into members (model_id, member_id, name) values (4, 27, 'd05-exp-gentle');
-- decay k=0.10 group (IDs 28-36)
insert or ignore into members (model_id, member_id, name) values (4, 28, 'd10-today-only');
insert or ignore into members (model_id, member_id, name) values (4, 29, 'd10-week-only');
insert or ignore into members (model_id, member_id, name) values (4, 30, 'd10-month-only');
insert or ignore into members (model_id, member_id, name) values (4, 31, 'd10-week+month');
insert or ignore into members (model_id, member_id, name) values (4, 32, 'd10-today+week+month');
insert or ignore into members (model_id, member_id, name) values (4, 33, 'd10-exp-steep');
insert or ignore into members (model_id, member_id, name) values (4, 34, 'd10-exp-fast');
insert or ignore into members (model_id, member_id, name) values (4, 35, 'd10-exp-moderate');
insert or ignore into members (model_id, member_id, name) values (4, 36, 'd10-exp-gentle');
