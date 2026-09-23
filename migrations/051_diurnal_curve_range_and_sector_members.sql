-- diurnal_curve members 40-41: diurnal-range compression/expansion (yesterday's
-- range scaled by how today's morning trajectory compares to the same-window
-- climatological rate, then current-anchored) and a wind-sector-conditioned
-- piecewise curve (30d hour-of-day means filtered to the current prevailing
-- 8-point wind sector). See docs/006_diurnal_curve.md.

insert or ignore into members (model_id, member_id, name) values (6, 40, 'range_scaled');
insert or ignore into members (model_id, member_id, name) values (6, 41, 'wind_sector_conditioned');
