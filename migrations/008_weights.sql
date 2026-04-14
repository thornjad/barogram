create table if not exists weights (
    model_id    integer not null references models(id),
    member_id   integer not null,
    variable    text    not null,
    lead_hours  integer not null,
    weight      real    not null,
    updated_at  integer not null,
    primary key (model_id, member_id, variable, lead_hours)
);
