-- model-level confidence is no longer an averaged, recalibrated aggregate: every member 0 now
-- gets its own confidence from its own scored history, like any member. clear what the
-- aggregate approach stamped (conf_bin 100 and up, written by two runs on 2026-11-10) and the
-- aggregate tally cells (n_bin 3), so their outcomes are never counted. the issued_at bound
-- keeps this off the full table scan; no aggregate stamp is older than it.

update forecasts set conf_bin = null where issued_at >= 1762700000 and conf_bin >= 100;

delete from calib_counts where n_bin = 3;
