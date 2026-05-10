insert or ignore into models (id, name, type) values (14, 'multivariate_trend', 'base');

insert or ignore into members (model_id, member_id, name) values (14, 0,  null);
insert or ignore into members (model_id, member_id, name) values (14, 1,  'linear-1h');
insert or ignore into members (model_id, member_id, name) values (14, 2,  'linear-3h');
insert or ignore into members (model_id, member_id, name) values (14, 3,  'linear-6h');
insert or ignore into members (model_id, member_id, name) values (14, 4,  'linear-12h');
insert or ignore into members (model_id, member_id, name) values (14, 5,  'wls-3h-hl20');
insert or ignore into members (model_id, member_id, name) values (14, 6,  'wls-6h-hl45');
insert or ignore into members (model_id, member_id, name) values (14, 7,  'wls-6h-hl120');
insert or ignore into members (model_id, member_id, name) values (14, 8,  'quad-3h');
insert or ignore into members (model_id, member_id, name) values (14, 9,  'quad-6h');
insert or ignore into members (model_id, member_id, name) values (14, 10, 'ridge-6h');
