insert or ignore into models (id, name, type) values (9, 'surface_signs', 'base');

insert or ignore into members (model_id, member_id, name) values (9, 0, null);
insert or ignore into members (model_id, member_id, name) values (9, 1, 'wind-rotation');
insert or ignore into members (model_id, member_id, name) values (9, 2, 'dp-trend');
insert or ignore into members (model_id, member_id, name) values (9, 3, 'solar-cloud');
insert or ignore into members (model_id, member_id, name) values (9, 4, 'convective');
