update models set name = 'barogram_ensemble' where id = 100;

insert or ignore into members (model_id, member_id, name) values (100, 0, null);
insert or ignore into members (model_id, member_id, name) values (100, 1, 'persistence');
insert or ignore into members (model_id, member_id, name) values (100, 2, 'climatological_mean');
insert or ignore into members (model_id, member_id, name) values (100, 3, 'weighted_climatological_mean');
insert or ignore into members (model_id, member_id, name) values (100, 4, 'climo_deviation');
insert or ignore into members (model_id, member_id, name) values (100, 5, 'pressure_tendency');
insert or ignore into members (model_id, member_id, name) values (100, 6, 'diurnal_curve');
