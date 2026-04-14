create table if not exists models (
    id   integer primary key,
    name text not null unique,
    type text not null default 'base'
);

insert or ignore into models (id, name, type) values (1, 'persistence', 'base');
insert or ignore into models (id, name, type) values (2, 'climatological_mean', 'base');
insert or ignore into models (id, name, type) values (3, 'weighted_climatological_mean', 'base');
insert or ignore into models (id, name, type) values (100, 'ensemble', 'ensemble');

create table if not exists forecasts (
    id          integer primary key autoincrement,
    model_id    integer not null references models(id),
    model       text    not null,  -- denormalized for query convenience
    issued_at   integer not null,
    valid_at    integer not null,
    lead_hours  integer not null,
    variable    text    not null,
    value       real,
    observed    real,
    scored_at   integer,
    error       real,
    mae         real
);

create index if not exists idx_forecasts_lookup
    on forecasts (model_id, variable, valid_at);

create index if not exists idx_forecasts_issued
    on forecasts (issued_at);

create index if not exists idx_forecasts_scoring
    on forecasts (valid_at, scored_at);
