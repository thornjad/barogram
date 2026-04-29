insert or ignore into models (id, name, type) values (10, 'synoptic_state_machine', 'base');

insert or ignore into members (model_id, member_id, name) values (10, 0, null);
insert or ignore into members (model_id, member_id, name) values (10, 1, 'full-4');
insert or ignore into members (model_id, member_id, name) values (10, 2, 'no-cloud');
insert or ignore into members (model_id, member_id, name) values (10, 3, 'wind-moisture');
insert or ignore into members (model_id, member_id, name) values (10, 4, 'moisture-convective');
insert or ignore into members (model_id, member_id, name) values (10, 5, 'coarse-4');
