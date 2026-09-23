-- radiational_cooling: new model, 3 members. Clear+calm+dry combined into one
-- joint overnight cooling trigger, a wind_lull-frequency stability proxy, and a
-- learned metro-heat-retention correction on top of the joint-trigger member.

insert or ignore into models (id, name, type) values (28, 'radiational_cooling', 'base');
insert or ignore into members (model_id, member_id, name) values (28, 0, null);
insert or ignore into members (model_id, member_id, name) values (28, 1, 'cooling_potential_index');
insert or ignore into members (model_id, member_id, name) values (28, 2, 'wind_lull_frequency');
insert or ignore into members (model_id, member_id, name) values (28, 3, 'metro_heat_retention_correction');
