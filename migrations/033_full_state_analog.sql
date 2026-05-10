insert or ignore into models (id, name, type) values (13, 'full_state_analog', 'base');

insert or ignore into members (model_id, member_id, name) values (13, 0, null);
insert or ignore into members (model_id, member_id, name) values (13, 1, 'full-k5');
insert or ignore into members (model_id, member_id, name) values (13, 2, 'full-k10');
insert or ignore into members (model_id, member_id, name) values (13, 3, 'thermo-wind');
insert or ignore into members (model_id, member_id, name) values (13, 4, 'solar-thermo');
insert or ignore into members (model_id, member_id, name) values (13, 5, 'synoptic');
insert or ignore into members (model_id, member_id, name) values (13, 6, 'precip-signal');
insert or ignore into members (model_id, member_id, name) values (13, 7, 'full-seasonal');
insert or ignore into members (model_id, member_id, name) values (13, 8, 'full-dist-weighted');
