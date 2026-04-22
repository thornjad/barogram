-- airmass_diurnal v2: clearness-trend, pressure-departure, and combined members
insert or ignore into members (model_id, member_id, name) values (7, 9,  'clearness-trend');
insert or ignore into members (model_id, member_id, name) values (7, 10, 'clearness-trend+dewpoint');
insert or ignore into members (model_id, member_id, name) values (7, 11, 'clearness-trend+pressure-proj');
insert or ignore into members (model_id, member_id, name) values (7, 12, 'pressure-departure');
insert or ignore into members (model_id, member_id, name) values (7, 13, 'pressure-dep+clearness-trend');
