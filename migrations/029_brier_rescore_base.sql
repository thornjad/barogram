-- reset precip_prob scores for all base/ensemble models so they are
-- re-scored using brier score (squared error), consistent with the
-- climo reference reset in migration 028 and external models in 027.
-- models 2, 200, 201 were already handled by those migrations.
update forecasts
set scored_at = null, observed = null, error = null, mae = null
where variable = 'precip_prob'
  and model_id not in (2, 200, 201);
