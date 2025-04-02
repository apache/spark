-- This test verifies that if a partition exists outside an index table's current location when the
-- index is dropped the partition's location is dropped as well.

CREATE TABLE test_table (key STRING, value STRING)
PARTITIONED BY (part STRING)
STORED AS RCFILE
LOCATION 'file:${system:test.tmp.dir}/drop_database_removes_partition_dirs_table';

CREATE INDEX test_index ON 
TABLE test_table(key) AS 'compact' WITH DEFERRED REBUILD
IN TABLE test_index_table;

ALTER TABLE test_index_table ADD PARTITION (part = '1')
LOCATION 'file:${system:test.tmp.dir}/drop_index_removes_partition_dirs_index_table2/part=1';

dfs -ls ${system:test.tmp.dir}/drop_index_removes_partition_dirs_index_table2;

DROP INDEX test_index ON test_table;

dfs -ls ${system:test.tmp.dir}/drop_index_removes_partition_dirs_index_table2;

dfs -rmr ${system:test.tmp.dir}/drop_index_removes_partition_dirs_index_table2;
