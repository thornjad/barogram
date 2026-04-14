insert or ignore into models (id, name, type) values (5, 'pressure_tendency', 'base');

insert or ignore into members (model_id, member_id, name) values (5, 0,  null);
insert or ignore into members (model_id, member_id, name) values (5, 1,  'zambretti');
insert or ignore into members (model_id, member_id, name) values (5, 2,  'linear_1h');
insert or ignore into members (model_id, member_id, name) values (5, 3,  'linear_3h');
insert or ignore into members (model_id, member_id, name) values (5, 4,  'linear_6h');
insert or ignore into members (model_id, member_id, name) values (5, 5,  'linear_3h_hl45');
insert or ignore into members (model_id, member_id, name) values (5, 6,  'quad_3h');
insert or ignore into members (model_id, member_id, name) values (5, 7,  'quad_6h');
insert or ignore into members (model_id, member_id, name) values (5, 8,  'quad_3h_hl20');
insert or ignore into members (model_id, member_id, name) values (5, 9,  'quad_3h_hl45');
insert or ignore into members (model_id, member_id, name) values (5, 10, 'quad_6h_hl20');
insert or ignore into members (model_id, member_id, name) values (5, 11, 'quad_6h_hl45');
