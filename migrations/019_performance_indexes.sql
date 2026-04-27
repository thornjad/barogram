create index if not exists idx_forecasts_model_run
    on forecasts (model_id, member_id, issued_at);

create index if not exists idx_forecasts_scored_lead
    on forecasts (scored_at, lead_hours, issued_at);
