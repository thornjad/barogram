insert or ignore into models (id, name, type) values (6, 'diurnal_curve', 'base');

insert or ignore into members (model_id, member_id, name) values (6, 0, null);

-- sine members (1-12): 4 lookbacks × 3 anchors
insert or ignore into members (model_id, member_id, name) values (6, 1,  'sine-7d-current');
insert or ignore into members (model_id, member_id, name) values (6, 2,  'sine-7d-midnight');
insert or ignore into members (model_id, member_id, name) values (6, 3,  'sine-7d-none');
insert or ignore into members (model_id, member_id, name) values (6, 4,  'sine-14d-current');
insert or ignore into members (model_id, member_id, name) values (6, 5,  'sine-14d-midnight');
insert or ignore into members (model_id, member_id, name) values (6, 6,  'sine-14d-none');
insert or ignore into members (model_id, member_id, name) values (6, 7,  'sine-30d-current');
insert or ignore into members (model_id, member_id, name) values (6, 8,  'sine-30d-midnight');
insert or ignore into members (model_id, member_id, name) values (6, 9,  'sine-30d-none');
insert or ignore into members (model_id, member_id, name) values (6, 10, 'sine-yr-current');
insert or ignore into members (model_id, member_id, name) values (6, 11, 'sine-yr-midnight');
insert or ignore into members (model_id, member_id, name) values (6, 12, 'sine-yr-none');

-- piecewise members (13-24): 4 lookbacks × 3 anchors
insert or ignore into members (model_id, member_id, name) values (6, 13, 'piecewise-7d-current');
insert or ignore into members (model_id, member_id, name) values (6, 14, 'piecewise-7d-midnight');
insert or ignore into members (model_id, member_id, name) values (6, 15, 'piecewise-7d-none');
insert or ignore into members (model_id, member_id, name) values (6, 16, 'piecewise-14d-current');
insert or ignore into members (model_id, member_id, name) values (6, 17, 'piecewise-14d-midnight');
insert or ignore into members (model_id, member_id, name) values (6, 18, 'piecewise-14d-none');
insert or ignore into members (model_id, member_id, name) values (6, 19, 'piecewise-30d-current');
insert or ignore into members (model_id, member_id, name) values (6, 20, 'piecewise-30d-midnight');
insert or ignore into members (model_id, member_id, name) values (6, 21, 'piecewise-30d-none');
insert or ignore into members (model_id, member_id, name) values (6, 22, 'piecewise-yr-current');
insert or ignore into members (model_id, member_id, name) values (6, 23, 'piecewise-yr-midnight');
insert or ignore into members (model_id, member_id, name) values (6, 24, 'piecewise-yr-none');

-- asymmetric members (25-36): 4 lookbacks × 3 anchors
insert or ignore into members (model_id, member_id, name) values (6, 25, 'asymmetric-7d-current');
insert or ignore into members (model_id, member_id, name) values (6, 26, 'asymmetric-7d-midnight');
insert or ignore into members (model_id, member_id, name) values (6, 27, 'asymmetric-7d-none');
insert or ignore into members (model_id, member_id, name) values (6, 28, 'asymmetric-14d-current');
insert or ignore into members (model_id, member_id, name) values (6, 29, 'asymmetric-14d-midnight');
insert or ignore into members (model_id, member_id, name) values (6, 30, 'asymmetric-14d-none');
insert or ignore into members (model_id, member_id, name) values (6, 31, 'asymmetric-30d-current');
insert or ignore into members (model_id, member_id, name) values (6, 32, 'asymmetric-30d-midnight');
insert or ignore into members (model_id, member_id, name) values (6, 33, 'asymmetric-30d-none');
insert or ignore into members (model_id, member_id, name) values (6, 34, 'asymmetric-yr-current');
insert or ignore into members (model_id, member_id, name) values (6, 35, 'asymmetric-yr-midnight');
insert or ignore into members (model_id, member_id, name) values (6, 36, 'asymmetric-yr-none');

-- solar-parameterized members (37-39): physics-derived phase, 3 anchors
insert or ignore into members (model_id, member_id, name) values (6, 37, 'solar-current');
insert or ignore into members (model_id, member_id, name) values (6, 38, 'solar-midnight');
insert or ignore into members (model_id, member_id, name) values (6, 39, 'solar-none');
