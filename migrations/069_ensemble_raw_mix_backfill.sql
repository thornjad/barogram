-- rebuild barogram_ensemble's raw_mix history (its blend without the self-correction
-- member 101) so its self-correction can learn from it immediately instead of waiting
-- for new runs to score. model 100's stored member 0 includes member 101's contribution,
-- so the raw blend is recomputed from the per-model member rows (ids 1..100) the way
-- models.ensemble.blend_cells does: weighted by tune weight times confidence, members
-- without a weight dropped, equal mean when none has a weight.
--
-- approximation: the tune weights are today's, not the weights in force when each run
-- was made. only the last ~30 days have member values (older ones are pruned to null).
-- observed, scored_at and error come from the same run's member 0 row. dewpoint is not
-- clamped to temperature here, unlike a live insert.

insert or ignore into forecasts
    (model_id, model, member_id, issued_at, valid_at, lead_hours, variable,
     value, spread, confidence, observed, scored_at, error, mae)
with member_inputs as (
    select f.issued_at, f.valid_at, f.lead_hours, f.variable, f.value,
           f.confidence,
           w.weight,
           max(coalesce(f.confidence, 0.0), 0.001) as eff
    from forecasts f
    left join weights w
      on w.model_id = 100 and w.member_id = f.member_id
     and w.variable = f.variable and w.lead_hours = f.lead_hours
     and w.sector = case
            when cast(strftime('%H', f.valid_at, 'unixepoch', 'localtime') as integer) < 6 then 0
            when cast(strftime('%H', f.valid_at, 'unixepoch', 'localtime') as integer) < 12 then 1
            when cast(strftime('%H', f.valid_at, 'unixepoch', 'localtime') as integer) < 18 then 2
            else 3 end
    where f.model_id = 100 and f.member_id between 1 and 100 and f.value is not null
),
blend as (
    select issued_at, valid_at, lead_hours, variable,
           case when sum(case when weight is not null then weight * eff end) > 0
                then sum(case when weight is not null then weight * eff * value end)
                     / sum(case when weight is not null then weight * eff end)
                else avg(value) end as value,
           case when sum(case when weight is not null then weight * eff end) > 0
                then sum(case when weight is not null then weight * eff * coalesce(confidence, 0.0) end)
                     / sum(case when weight is not null then weight * eff end)
                else avg(confidence) end as confidence
    from member_inputs
    group by issued_at, valid_at, lead_hours, variable
)
select 100, 'barogram_ensemble', -1, b.issued_at, b.valid_at, b.lead_hours, b.variable,
       b.value, null, b.confidence, z.observed, z.scored_at,
       case when z.observed is not null then b.value - z.observed end,
       case when z.observed is not null then abs(b.value - z.observed) end
from blend b
join forecasts z
  on z.model_id = 100 and z.member_id = 0 and z.issued_at = b.issued_at
 and z.valid_at = b.valid_at and z.lead_hours = b.lead_hours and z.variable = b.variable
where exists (select 1 from members where model_id = 100 and member_id = -1 and name = 'raw_mix');
