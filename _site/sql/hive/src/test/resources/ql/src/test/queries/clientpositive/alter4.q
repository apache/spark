CREATE TABLE set_bucketing_test (key INT, value STRING) CLUSTERED BY (key) INTO 10 BUCKETS;
DESCRIBE EXTENDED set_bucketing_test;

ALTER TABLE set_bucketing_test NOT CLUSTERED;
DESCRIBE EXTENDED set_bucketing_test;

-- Cleanup
DROP TABLE set_bucketing_test;
SHOW TABLES;

-- with non-default Database

CREATE DATABASE alter4_db;
USE alter4_db;
SHOW TABLES;

CREATE TABLE set_bucketing_test (key INT, value STRING) CLUSTERED BY (key) INTO 10 BUCKETS;
DESCRIBE EXTENDED set_bucketing_test;

ALTER TABLE set_bucketing_test NOT CLUSTERED;
DESCRIBE EXTENDED set_bucketing_test;

DROP TABLE set_bucketing_test;
USE default;
DROP DATABASE alter4_db;
SHOW DATABASES;
