-- synoptic_state_machine members 95-96: gust-ratio trend (leading indicator of
-- approaching mechanical mixing vs a calming trend) and relative humidity as its
-- own joint dimension with wind rotation and pressure tendency, orthogonal to the
-- existing dewpoint-only moisture members.

insert or ignore into members (model_id, member_id, name) values (10, 95, 'gust-ratio-trend');
insert or ignore into members (model_id, member_id, name) values (10, 96, 'rh-wind-pressure');
