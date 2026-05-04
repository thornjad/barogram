-- reset precip_prob scores for climatological_mean so it is re-scored
-- using squared error (brier score) on the next run, consistent with
-- the external model reset in migration 027.
update forecasts
set scored_at = null, observed = null, error = null, mae = null
where variable = 'precip_prob'
  and model_id = 2;
