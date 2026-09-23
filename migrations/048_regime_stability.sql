-- regime_stability: new model, single member, tests whether a calm/transitional/
-- active volatility meta-state beats any single physical signal at short range.

insert or ignore into models (id, name, type) values (24, 'regime_stability', 'base');
insert or ignore into members (model_id, member_id, name) values (24, 0, null);
insert or ignore into members (model_id, member_id, name) values (24, 1, 'regime_meta_state');
