-- This test verifies that if a partition exists outside a table's current location when the
-- database is dropped the partition's location is dropped as well.

CREATE DATABASE test_database;

USE test_database;

CREATE TABLE test_table (key STRING, value STRING)
PARTITIONED BY (part STRING)
STORED AS RCFILE
LOCATION 'file:${system:test.tmp.dir}/drop_database_removes_partition_dirs_table';

ALTER TABLE test_table ADD PARTITION (part = '1')
LOCATION 'file:${system:test.tmp.dir}/drop_database_removes_partition_dirs_table2/part=1';

INSERT OVERWRITE TABLE test_table PARTITION (part = '1')
SELECT * FROM default.src;

dfs -ls ${system:test.tmp.dir}/drop_database_removes_partition_dirs_table2;

USE default;

DROP DATABASE test_database CASCADE;

dfs -ls ${system:test.tmp.dir}/drop_database_removes_partition_dirs_table2;

dfs -rmr ${system:test.tmp.dir}/drop_database_removes_partition_dirs_table2;
