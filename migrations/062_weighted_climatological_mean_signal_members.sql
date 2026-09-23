-- weighted_climatological_mean members 10-12: sector climatology (bucket mean
-- conditioned on current 8-point wind sector), harmonic regression (Fourier fit
-- to full air_temp history), and continuous day-of-year phase (von Mises kernel
-- replacing the hard month match). See docs/003_weighted_climatological_mean.md.

insert or ignore into members (model_id, member_id, name) values (3, 10, 'sector_climatology');
insert or ignore into members (model_id, member_id, name) values (3, 11, 'harmonic_regression');
insert or ignore into members (model_id, member_id, name) values (3, 12, 'continuous_doy_phase');
