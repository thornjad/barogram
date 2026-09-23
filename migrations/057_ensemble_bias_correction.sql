-- ensemble_bias_correction: new model, single member, applies a learned bias
-- correction to a recomputation of barogram_ensemble's own raw blend, then
-- re-enters the ensemble next cycle as an ordinary member.

insert or ignore into models (id, name, type) values (101, 'ensemble_bias_correction', 'base');
insert or ignore into members (model_id, member_id, name) values (101, 0, null);
