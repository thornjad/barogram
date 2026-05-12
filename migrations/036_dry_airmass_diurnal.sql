insert or ignore into models (id, name, type) values (15, 'dry_airmass_diurnal', 'base');

insert or ignore into members (model_id, member_id, name) values (15, 0, null);
insert or ignore into members (model_id, member_id, name) values (15, 1, '24h-amp');
insert or ignore into members (model_id, member_id, name) values (15, 2, '48h-amp');
insert or ignore into members (model_id, member_id, name) values (15, 3, '72h-amp');
insert or ignore into members (model_id, member_id, name) values (15, 4, '24h-amp-ridge');
insert or ignore into members (model_id, member_id, name) values (15, 5, '48h-amp-ridge');
insert or ignore into members (model_id, member_id, name) values (15, 6, '72h-amp-ridge');
