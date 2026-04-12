INSERT OR IGNORE INTO models (id, name, type) VALUES (3, 'weighted_climatological_mean', 'base');

INSERT OR IGNORE INTO members (model_id, member_id, name) VALUES (3, 0, NULL);
INSERT OR IGNORE INTO members (model_id, member_id, name) VALUES (3, 1, 'today-only');
INSERT OR IGNORE INTO members (model_id, member_id, name) VALUES (3, 2, 'week-only');
INSERT OR IGNORE INTO members (model_id, member_id, name) VALUES (3, 3, 'month-only');
INSERT OR IGNORE INTO members (model_id, member_id, name) VALUES (3, 4, 'week+month');
INSERT OR IGNORE INTO members (model_id, member_id, name) VALUES (3, 5, 'today+week+month');
INSERT OR IGNORE INTO members (model_id, member_id, name) VALUES (3, 6, 'exp-steep');
INSERT OR IGNORE INTO members (model_id, member_id, name) VALUES (3, 7, 'exp-fast');
INSERT OR IGNORE INTO members (model_id, member_id, name) VALUES (3, 8, 'exp-moderate');
INSERT OR IGNORE INTO members (model_id, member_id, name) VALUES (3, 9, 'exp-gentle');
