-- reset precip_prob scores for external models so they are re-scored
-- using squared error (brier score) on the next run. base model historical
-- rows are intentionally left as-is.
update forecasts
set scored_at = null, observed = null, error = null, mae = null
where variable = 'precip_prob'
  and model_id in (200, 201);
