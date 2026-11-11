-- confidence is evidence-based again (docs/confidence.md): the calibration tally and the
-- per-row bins it needed are gone. calib_counts only held derived counts and conf_bin was a
-- label on each row; neither is a forecast or an outcome, so nothing recorded is lost.
drop table if exists calib_counts;
alter table forecasts drop column conf_bin;
