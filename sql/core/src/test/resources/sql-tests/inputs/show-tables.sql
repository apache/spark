-- Test data.
CREATE DATABASE showdb;
USE showdb;
CREATE TABLE show_t1(a String, b Int, c String, d String) USING parquet PARTITIONED BY (c, d);
ALTER TABLE show_t1 ADD PARTITION (c='Us', d=1);
CREATE TABLE show_t2(b String, d Int) USING parquet;
CREATE TEMPORARY VIEW show_t3(e int) USING parquet;
CREATE GLOBAL TEMP VIEW show_t4 AS SELECT 1 as col1;

-- SHOW TABLES
SHOW TABLES;
SHOW TABLES IN showdb;

-- SHOW TABLES WITH wildcard match
SHOW TABLES 'show_t*';
SHOW TABLES LIKE 'show_t1*|show_t2*';
SHOW TABLES IN showdb 'show_t*';

-- SHOW TABLE EXTENDED
SHOW TABLE EXTENDED LIKE 'show_t*';
SHOW TABLE EXTENDED;

-- SHOW TABLE EXTENDED ... PARTITION
SHOW TABLE EXTENDED LIKE 'show_t1' PARTITION(c='Us', d=1);
-- Throw a ParseException if table name is not specified.
SHOW TABLE EXTENDED PARTITION(c='Us', d=1);
-- Don't support regular expression for table name if a partition specification is present.
SHOW TABLE EXTENDED LIKE 'show_t*' PARTITION(c='Us', d=1);
-- Partition specification is not complete.
SHOW TABLE EXTENDED LIKE 'show_t1' PARTITION(c='Us');
-- Partition specification is invalid.
SHOW TABLE EXTENDED LIKE 'show_t1' PARTITION(a='Us', d=1);
-- Partition specification doesn't exist.
SHOW TABLE EXTENDED LIKE 'show_t1' PARTITION(c='Ch', d=1);

-- Clean Up
DROP TABLE show_t1;
DROP TABLE show_t2;
DROP VIEW  show_t3;
DROP VIEW  global_temp.show_t4;
USE default;
DROP DATABASE showdb;
