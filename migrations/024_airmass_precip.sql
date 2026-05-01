insert or ignore into models (id, name, type) values (11, 'airmass_precip', 'base');

insert or ignore into members (model_id, member_id, name) values (11, 0, null);
insert or ignore into members (model_id, member_id, name) values (11, 1, 'dewpoint-moisture');
insert or ignore into members (model_id, member_id, name) values (11, 2, 'pressure-tendency');
insert or ignore into members (model_id, member_id, name) values (11, 3, 'cloud-cover');
insert or ignore into members (model_id, member_id, name) values (11, 4, 'wind-sector');
insert or ignore into members (model_id, member_id, name) values (11, 5, 'active-precip');
insert or ignore into members (model_id, member_id, name) values (11, 6, 'moisture+pressure');
insert or ignore into members (model_id, member_id, name) values (11, 7, 'wind-rotation');
insert or ignore into members (model_id, member_id, name) values (11, 8, 'cloud+moisture');
