set hive.exec.infer.bucket.sort=true;

-- This tests inferring how data is bucketed/sorted from the operators in the reducer
-- and populating that information in partitions' metadata, in particular, this tests
-- the grouping operators rollup/cube/grouping sets

CREATE TABLE test_table_out (key STRING, value STRING, agg STRING) PARTITIONED BY (part STRING);

CREATE TABLE test_table_out_2 (key STRING, value STRING, grouping_key STRING, agg STRING) PARTITIONED BY (part STRING);

-- Test rollup, should not be bucketed or sorted because its missing the grouping ID
EXPLAIN INSERT OVERWRITE TABLE test_table_out PARTITION (part = '1') 
SELECT key, value, count(1) FROM src GROUP BY key, value WITH ROLLUP;

INSERT OVERWRITE TABLE test_table_out PARTITION (part = '1') 
SELECT key, value, count(1) FROM src GROUP BY key, value WITH ROLLUP;

DESCRIBE FORMATTED test_table_out PARTITION (part = '1');

-- Test rollup, should be bucketed and sorted on key, value, grouping_key

INSERT OVERWRITE TABLE test_table_out_2 PARTITION (part = '1') 
SELECT key, value, GROUPING__ID, count(1) FROM src GROUP BY key, value WITH ROLLUP;

DESCRIBE FORMATTED test_table_out_2 PARTITION (part = '1');

-- Test cube, should not be bucketed or sorted because its missing the grouping ID
EXPLAIN INSERT OVERWRITE TABLE test_table_out PARTITION (part = '1') 
SELECT key, value, count(1) FROM src GROUP BY key, value WITH CUBE;

INSERT OVERWRITE TABLE test_table_out PARTITION (part = '1') 
SELECT key, value, count(1) FROM src GROUP BY key, value WITH CUBE;

DESCRIBE FORMATTED test_table_out PARTITION (part = '1');

-- Test cube, should be bucketed and sorted on key, value, grouping_key

INSERT OVERWRITE TABLE test_table_out_2 PARTITION (part = '1') 
SELECT key, value, GROUPING__ID, count(1) FROM src GROUP BY key, value WITH CUBE;

DESCRIBE FORMATTED test_table_out_2 PARTITION (part = '1');

-- Test grouping sets, should not be bucketed or sorted because its missing the grouping ID
EXPLAIN INSERT OVERWRITE TABLE test_table_out PARTITION (part = '1') 
SELECT key, value, count(1) FROM src GROUP BY key, value GROUPING SETS (key, value);

INSERT OVERWRITE TABLE test_table_out PARTITION (part = '1') 
SELECT key, value, count(1) FROM src GROUP BY key, value GROUPING SETS (key, value);

DESCRIBE FORMATTED test_table_out PARTITION (part = '1');

-- Test grouping sets, should be bucketed and sorted on key, value, grouping_key

INSERT OVERWRITE TABLE test_table_out_2 PARTITION (part = '1') 
SELECT key, value, GROUPING__ID, count(1) FROM src GROUP BY key, value GROUPING SETS (key, value);

DESCRIBE FORMATTED test_table_out_2 PARTITION (part = '1');
