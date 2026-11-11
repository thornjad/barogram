-- the tally's evidence bin is now called raw_bin (the member's raw confidence, before
-- calibration corrects it). a rename only; no rows or semantics change.

alter table calib_counts rename column score_bin to raw_bin;
