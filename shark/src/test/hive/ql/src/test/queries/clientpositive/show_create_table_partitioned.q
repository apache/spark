-- Test SHOW CREATE TABLE on a table with partitions and column comments.

CREATE EXTERNAL TABLE tmp_showcrt1 (key string, newvalue boolean COMMENT 'a new value')
COMMENT 'temporary table'
PARTITIONED BY (value bigint COMMENT 'some value');
SHOW CREATE TABLE tmp_showcrt1;
DROP TABLE tmp_showcrt1;

