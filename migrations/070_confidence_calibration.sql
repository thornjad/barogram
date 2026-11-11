-- confidence calibration: each leaf forecast row keeps the evidence score it had at issue
-- time (packed into conf_bin) so that, once the row is scored, its hit or miss can be
-- tallied against that score. calib_counts is the tally: one row per
-- (variable, lead, evidence bins, issue-day), summed into measured hit rates by
-- models/_calibration.py. additive only; nothing existing is altered or dropped.

alter table forecasts add column conf_bin integer;

create table if not exists calib_counts (
    variable   text    not null,
    lead_hours integer not null,
    n_bin      integer not null,
    form_bin   integer not null,
    score_bin  integer not null,
    day        integer not null,
    hits       integer not null,
    total      integer not null,
    primary key (variable, lead_hours, n_bin, form_bin, score_bin, day)
);
