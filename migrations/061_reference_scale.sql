create table if not exists reference_scale (
    variable    text not null,
    lead_hours  integer not null,
    scale       real not null,
    updated_at  integer not null,
    primary key (variable, lead_hours)
);
