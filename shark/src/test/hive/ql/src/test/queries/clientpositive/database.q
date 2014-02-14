set hive.support.concurrency = true;

SHOW DATABASES;

-- CREATE with comment
CREATE DATABASE test_db COMMENT 'Hive test database';
SHOW DATABASES;

-- CREATE INE already exists
CREATE DATABASE IF NOT EXISTS test_db;
SHOW DATABASES;

-- SHOW DATABASES synonym
SHOW SCHEMAS;

-- DROP
DROP DATABASE test_db;
SHOW DATABASES;

-- CREATE INE doesn't exist
CREATE DATABASE IF NOT EXISTS test_db COMMENT 'Hive test database';
SHOW DATABASES;

-- DROP IE exists
DROP DATABASE IF EXISTS test_db;
SHOW DATABASES;

-- DROP IE doesn't exist
DROP DATABASE IF EXISTS test_db;

-- SHOW
CREATE DATABASE test_db;
SHOW DATABASES;

-- SHOW pattern
SHOW DATABASES LIKE 'test*';

-- SHOW pattern
SHOW DATABASES LIKE '*ef*';


USE test_db;
SHOW DATABASES;

-- CREATE table in non-default DB
CREATE TABLE test_table (col1 STRING) STORED AS TEXTFILE;
SHOW TABLES;

-- DESCRIBE table in non-default DB
DESCRIBE test_table;

-- DESCRIBE EXTENDED in non-default DB
DESCRIBE EXTENDED test_table;

-- CREATE LIKE in non-default DB
CREATE TABLE test_table_like LIKE test_table;
SHOW TABLES;
DESCRIBE EXTENDED test_table_like;

-- LOAD and SELECT
LOAD DATA LOCAL INPATH '../data/files/test.dat'
OVERWRITE INTO TABLE test_table;
SELECT * FROM test_table;

-- DROP and CREATE w/o LOAD
DROP TABLE test_table;
SHOW TABLES;

CREATE TABLE test_table (col1 STRING) STORED AS TEXTFILE;
SHOW TABLES;

SELECT * FROM test_table;

-- CREATE table that already exists in DEFAULT
USE test_db;
CREATE TABLE src (col1 STRING) STORED AS TEXTFILE;
SHOW TABLES;

SELECT * FROM src LIMIT 10;

USE default;
SELECT * FROM src LIMIT 10;

-- DROP DATABASE
USE test_db;

DROP TABLE src;
DROP TABLE test_table;
DROP TABLE test_table_like;
SHOW TABLES;

USE default;
DROP DATABASE test_db;
SHOW DATABASES;

-- DROP EMPTY DATABASE CASCADE
CREATE DATABASE to_drop_db1;
SHOW DATABASES;
USE default;
DROP DATABASE to_drop_db1 CASCADE;
SHOW DATABASES;

-- DROP NON-EMPTY DATABASE CASCADE
CREATE DATABASE to_drop_db2;
SHOW DATABASES;
USE to_drop_db2;
CREATE TABLE temp_tbl (c STRING);
CREATE TABLE temp_tbl2 LIKE temp_tbl;
INSERT OVERWRITE TABLE temp_tbl2 SELECT COUNT(*) FROM temp_tbl;
USE default;
DROP DATABASE to_drop_db2 CASCADE;
SHOW DATABASES;

-- DROP NON-EMPTY DATABASE CASCADE IF EXISTS
CREATE DATABASE to_drop_db3;
SHOW DATABASES;
USE to_drop_db3;
CREATE TABLE temp_tbl (c STRING);
USE default;
DROP DATABASE IF EXISTS to_drop_db3 CASCADE;
SHOW DATABASES;

-- DROP NON-EXISTING DATABASE CASCADE IF EXISTS
DROP DATABASE IF EXISTS non_exists_db3 CASCADE;
SHOW DATABASES;

-- DROP NON-EXISTING DATABASE RESTRICT IF EXISTS
DROP DATABASE IF EXISTS non_exists_db3 RESTRICT;

-- DROP EMPTY DATABASE RESTRICT
CREATE DATABASE to_drop_db4;
SHOW DATABASES;
DROP DATABASE to_drop_db4 RESTRICT;
SHOW DATABASES;


--
-- Canonical Name Tests
--

CREATE DATABASE db1;
CREATE DATABASE db2;

-- CREATE foreign table
CREATE TABLE db1.src(key STRING, value STRING)
STORED AS TEXTFILE;

-- LOAD into foreign table
LOAD DATA LOCAL INPATH '../data/files/kv1.txt'
OVERWRITE INTO TABLE db1.src;

-- SELECT from foreign table
SELECT * FROM db1.src;

-- CREATE Partitioned foreign table
CREATE TABLE db1.srcpart(key STRING, value STRING)
PARTITIONED BY (ds STRING, hr STRING)
STORED AS TEXTFILE;

-- LOAD data into Partitioned foreign table
LOAD DATA LOCAL INPATH '../data/files/kv1.txt'
OVERWRITE INTO TABLE db1.srcpart
PARTITION (ds='2008-04-08', hr='11');

-- SELECT from Partitioned foreign table
SELECT key, value FROM db1.srcpart
WHERE key < 100 AND ds='2008-04-08' AND hr='11';

-- SELECT JOINed product of two foreign tables
USE db2;
SELECT a.* FROM db1.src a JOIN default.src1 b
ON (a.key = b.key);

-- CREATE TABLE AS SELECT from foreign table
CREATE TABLE conflict_name AS
SELECT value FROM default.src WHERE key = 66;

-- CREATE foreign table
CREATE TABLE db1.conflict_name AS
SELECT value FROM db1.src WHERE key = 8;

-- query tables with the same names in different DBs
SELECT * FROM (
  SELECT value FROM db1.conflict_name
UNION ALL
  SELECT value FROM conflict_name
) subq ORDER BY value;

USE default;
SELECT * FROM (
  SELECT value FROM db1.conflict_name
UNION ALL
  SELECT value FROM db2.conflict_name
) subq ORDER BY value;

-- TABLESAMPLES
CREATE TABLE bucketized_src (key INT, value STRING)
CLUSTERED BY (key) SORTED BY (key) INTO 1 BUCKETS;

INSERT OVERWRITE TABLE bucketized_src
SELECT key, value FROM src WHERE key=66;

SELECT key FROM bucketized_src TABLESAMPLE(BUCKET 1 out of 1);

-- CREATE TABLE LIKE
CREATE TABLE db2.src1 LIKE default.src;

USE db2;
DESC EXTENDED src1;

-- character escaping
SELECT key FROM `default`.src ORDER BY key LIMIT 1;
SELECT key FROM `default`.`src` ORDER BY key LIMIT 1;
SELECT key FROM default.`src` ORDER BY key LIMIT 1;

USE default;
