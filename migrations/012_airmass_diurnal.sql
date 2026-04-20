insert or ignore into models (id, name, type) values (7, 'airmass_diurnal', 'base');

insert or ignore into members (model_id, member_id, name) values (7, 0, null);
insert or ignore into members (model_id, member_id, name) values (7, 1, 'clearness-only');
insert or ignore into members (model_id, member_id, name) values (7, 2, 'clearness+dewpoint');
insert or ignore into members (model_id, member_id, name) values (7, 3, 'clearness-pressure-projected');
insert or ignore into members (model_id, member_id, name) values (7, 4, 'wind-sector-only');
insert or ignore into members (model_id, member_id, name) values (7, 5, 'wind+clearness');
insert or ignore into members (model_id, member_id, name) values (7, 6, 'morning-warmup-rate');
insert or ignore into members (model_id, member_id, name) values (7, 7, 'dewpoint-only');
insert or ignore into members (model_id, member_id, name) values (7, 8, 'combined-full');
