insert or ignore into models (id, name, type) values (200, 'nws', 'external');
insert or ignore into models (id, name, type) values (201, 'tempest_forecast', 'external');
insert or ignore into members (model_id, member_id, name) values (200, 0, null);
insert or ignore into members (model_id, member_id, name) values (201, 0, null);
