INSERT OR IGNORE INTO models (id, name, type) VALUES (4, 'climo_deviation', 'base');

INSERT OR IGNORE INTO members (model_id, member_id, name) VALUES (4, 0, NULL);
-- static group (IDs 1-9)
INSERT OR IGNORE INTO members (model_id, member_id, name) VALUES (4, 1,  's-today-only');
INSERT OR IGNORE INTO members (model_id, member_id, name) VALUES (4, 2,  's-week-only');
INSERT OR IGNORE INTO members (model_id, member_id, name) VALUES (4, 3,  's-month-only');
INSERT OR IGNORE INTO members (model_id, member_id, name) VALUES (4, 4,  's-week+month');
INSERT OR IGNORE INTO members (model_id, member_id, name) VALUES (4, 5,  's-today+week+month');
INSERT OR IGNORE INTO members (model_id, member_id, name) VALUES (4, 6,  's-exp-steep');
INSERT OR IGNORE INTO members (model_id, member_id, name) VALUES (4, 7,  's-exp-fast');
INSERT OR IGNORE INTO members (model_id, member_id, name) VALUES (4, 8,  's-exp-moderate');
INSERT OR IGNORE INTO members (model_id, member_id, name) VALUES (4, 9,  's-exp-gentle');
-- decay k=0.03 group (IDs 10-18)
INSERT OR IGNORE INTO members (model_id, member_id, name) VALUES (4, 10, 'd03-today-only');
INSERT OR IGNORE INTO members (model_id, member_id, name) VALUES (4, 11, 'd03-week-only');
INSERT OR IGNORE INTO members (model_id, member_id, name) VALUES (4, 12, 'd03-month-only');
INSERT OR IGNORE INTO members (model_id, member_id, name) VALUES (4, 13, 'd03-week+month');
INSERT OR IGNORE INTO members (model_id, member_id, name) VALUES (4, 14, 'd03-today+week+month');
INSERT OR IGNORE INTO members (model_id, member_id, name) VALUES (4, 15, 'd03-exp-steep');
INSERT OR IGNORE INTO members (model_id, member_id, name) VALUES (4, 16, 'd03-exp-fast');
INSERT OR IGNORE INTO members (model_id, member_id, name) VALUES (4, 17, 'd03-exp-moderate');
INSERT OR IGNORE INTO members (model_id, member_id, name) VALUES (4, 18, 'd03-exp-gentle');
-- decay k=0.05 group (IDs 19-27)
INSERT OR IGNORE INTO members (model_id, member_id, name) VALUES (4, 19, 'd05-today-only');
INSERT OR IGNORE INTO members (model_id, member_id, name) VALUES (4, 20, 'd05-week-only');
INSERT OR IGNORE INTO members (model_id, member_id, name) VALUES (4, 21, 'd05-month-only');
INSERT OR IGNORE INTO members (model_id, member_id, name) VALUES (4, 22, 'd05-week+month');
INSERT OR IGNORE INTO members (model_id, member_id, name) VALUES (4, 23, 'd05-today+week+month');
INSERT OR IGNORE INTO members (model_id, member_id, name) VALUES (4, 24, 'd05-exp-steep');
INSERT OR IGNORE INTO members (model_id, member_id, name) VALUES (4, 25, 'd05-exp-fast');
INSERT OR IGNORE INTO members (model_id, member_id, name) VALUES (4, 26, 'd05-exp-moderate');
INSERT OR IGNORE INTO members (model_id, member_id, name) VALUES (4, 27, 'd05-exp-gentle');
-- decay k=0.10 group (IDs 28-36)
INSERT OR IGNORE INTO members (model_id, member_id, name) VALUES (4, 28, 'd10-today-only');
INSERT OR IGNORE INTO members (model_id, member_id, name) VALUES (4, 29, 'd10-week-only');
INSERT OR IGNORE INTO members (model_id, member_id, name) VALUES (4, 30, 'd10-month-only');
INSERT OR IGNORE INTO members (model_id, member_id, name) VALUES (4, 31, 'd10-week+month');
INSERT OR IGNORE INTO members (model_id, member_id, name) VALUES (4, 32, 'd10-today+week+month');
INSERT OR IGNORE INTO members (model_id, member_id, name) VALUES (4, 33, 'd10-exp-steep');
INSERT OR IGNORE INTO members (model_id, member_id, name) VALUES (4, 34, 'd10-exp-fast');
INSERT OR IGNORE INTO members (model_id, member_id, name) VALUES (4, 35, 'd10-exp-moderate');
INSERT OR IGNORE INTO members (model_id, member_id, name) VALUES (4, 36, 'd10-exp-gentle');
