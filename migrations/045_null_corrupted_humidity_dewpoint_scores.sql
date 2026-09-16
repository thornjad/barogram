-- migration 007 renamed variable='humidity' to 'dewpoint' with no value
-- recompute, so any dewpoint row from before that rename took effect still
-- holds an old relative-humidity value (0-100 scale) scored against a real
-- dew_point observation (celsius). only models 1-4 existed before the
-- rename (commit 39dc1f8, 2026-04-14); the shared cutoff, verified against
-- the actual mae distribution rather than the commit timestamp, is the last
-- pre-rename forecast run at issued_at=1776179035 (2026-04-14 10:03:55
-- local). the original value/observed pairs are long gone via prune, so the
-- only sound fix is nulling the misleading error/mae/scored_at.
update forecasts
set error = null, mae = null, scored_at = null
where variable = 'dewpoint'
  and model_id in (1, 2, 3, 4)
  and issued_at <= 1776179035;
