-- frontal_trigger member 4: front_type_archetype. Classifies pressure rate, wind
-- veer, temp trend, and precip duration jointly into a cold_frontal/warm_frontal
-- state, richer than members 1-3's plain pressure-tendency+veer joint.

insert or ignore into members (model_id, member_id, name) values (21, 4, 'front_type_archetype');
