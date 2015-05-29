CREATE TABLE kv_rename_test(a int, b int, c int);
DESCRIBE kv_rename_test;

ALTER TABLE kv_rename_test CHANGE a a STRING;
DESCRIBE kv_rename_test;

ALTER TABLE kv_rename_test CHANGE a a1 INT;
DESCRIBE kv_rename_test;

ALTER TABLE kv_rename_test CHANGE a1 a2 INT FIRST;
DESCRIBE kv_rename_test;

ALTER TABLE kv_rename_test CHANGE a2 a INT AFTER b;
DESCRIBE kv_rename_test;

ALTER TABLE kv_rename_test CHANGE a a1 INT COMMENT 'test comment1';
DESCRIBE kv_rename_test;

ALTER TABLE kv_rename_test CHANGE a1 a2 INT COMMENT 'test comment2' FIRST;
DESCRIBE kv_rename_test;

ALTER TABLE kv_rename_test CHANGE COLUMN a2 a INT AFTER b;
DESCRIBE kv_rename_test;

DROP TABLE kv_rename_test;
SHOW TABLES;

-- Using non-default Database
CREATE DATABASE kv_rename_test_db;
USE kv_rename_test_db;

CREATE TABLE kv_rename_test(a int, b int, c int);
DESCRIBE kv_rename_test;

ALTER TABLE kv_rename_test CHANGE a a STRING;
DESCRIBE kv_rename_test;

ALTER TABLE kv_rename_test CHANGE a a1 INT;
DESCRIBE kv_rename_test;

ALTER TABLE kv_rename_test CHANGE a1 a2 INT FIRST;
DESCRIBE kv_rename_test;

ALTER TABLE kv_rename_test CHANGE a2 a INT AFTER b;
DESCRIBE kv_rename_test;

ALTER TABLE kv_rename_test CHANGE a a1 INT COMMENT 'test comment1';
DESCRIBE kv_rename_test;

ALTER TABLE kv_rename_test CHANGE a1 a2 INT COMMENT 'test comment2' FIRST;
DESCRIBE kv_rename_test;

ALTER TABLE kv_rename_test CHANGE COLUMN a2 a INT AFTER b;
DESCRIBE kv_rename_test;

DROP TABLE kv_rename_test;
SHOW TABLES;
