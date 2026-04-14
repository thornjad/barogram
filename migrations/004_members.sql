create table if not exists members (
    model_id   integer not null references models(id),
    member_id  integer not null default 0,
    name       text,
    primary key (model_id, member_id)
);

insert or ignore into members (model_id, member_id, name) values (1, 0, null);
insert or ignore into members (model_id, member_id, name) values (2, 0, null);
insert or ignore into members (model_id, member_id, name) values (100, 0, null);

alter table forecasts add column member_id integer not null default 0;
alter table forecasts add column spread real;

drop index if exists idx_forecasts_lookup;
create index if not exists idx_forecasts_lookup
    on forecasts (model_id, member_id, variable, valid_at);
