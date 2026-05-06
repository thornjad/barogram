-- prevent duplicate forecast rows from re-running barogram forecast with the same issued_at
create unique index if not exists idx_forecasts_dedup
    on forecasts (model_id, member_id, issued_at, valid_at, lead_hours, variable);
