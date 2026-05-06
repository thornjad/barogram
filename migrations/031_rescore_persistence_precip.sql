-- persistence._precip_prob returned 100.0 (percentage scale) instead of 1.0 (probability scale).
-- reset all scored persistence precip_prob rows so they are re-scored on the next score run.
update forecasts
set scored_at = null, observed = null, error = null, mae = null
where model_id = 1
  and variable = 'precip_prob'
  and scored_at is not null;
