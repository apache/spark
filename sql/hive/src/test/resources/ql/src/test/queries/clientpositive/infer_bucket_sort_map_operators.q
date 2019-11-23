set hive.exec.infer.bucket.sort=true;
set hive.enforce.bucketing = true;
set hive.enforce.sorting = true;

-- This tests inferring how data is bucketed/sorted from the operators in the reducer
-- and populating that information in partitions' metadata, in particular, this tests
-- that operators in the mapper have no effect

CREATE TABLE test_table1 (key STRING, value STRING)
CLUSTERED BY (key) SORTED BY (key DESC) INTO 2 BUCKETS;

CREATE TABLE test_table2 (key STRING, value STRING)
CLUSTERED BY (key) SORTED BY (key DESC) INTO 2 BUCKETS;

INSERT OVERWRITE TABLE test_table1 SELECT key, value FROM src;

INSERT OVERWRITE TABLE test_table2 SELECT key, value FROM src;

CREATE TABLE test_table_out (key STRING, value STRING) PARTITIONED BY (part STRING);

set hive.map.groupby.sorted=true;

-- Test map group by doesn't affect inference, should not be bucketed or sorted
EXPLAIN INSERT OVERWRITE TABLE test_table_out PARTITION (part = '1') 
SELECT key, count(*) FROM test_table1 GROUP BY key;

INSERT OVERWRITE TABLE test_table_out PARTITION (part = '1') 
SELECT key, count(*) FROM test_table1 GROUP BY key;

DESCRIBE FORMATTED test_table_out PARTITION (part = '1');

-- Test map group by doesn't affect inference, should be bucketed and sorted by value
EXPLAIN INSERT OVERWRITE TABLE test_table_out PARTITION (part = '1') 
SELECT a.key, a.value FROM (
	SELECT key, count(*) AS value FROM test_table1 GROUP BY key
) a JOIN (
 	SELECT key, value FROM src
) b
ON (a.value = b.value);

INSERT OVERWRITE TABLE test_table_out PARTITION (part = '1') 
SELECT a.key, a.value FROM (
	SELECT key, cast(count(*) AS STRING) AS value FROM test_table1 GROUP BY key
) a JOIN (
 	SELECT key, value FROM src
) b
ON (a.value = b.value);

DESCRIBE FORMATTED test_table_out PARTITION (part = '1');

set hive.map.groupby.sorted=false;
set hive.optimize.bucketmapjoin = true;
set hive.optimize.bucketmapjoin.sortedmerge = true;

-- Test SMB join doesn't affect inference, should not be bucketed or sorted
EXPLAIN INSERT OVERWRITE TABLE test_table_out PARTITION (part = '1')
SELECT /*+ MAPJOIN(a) */ a.key, b.value FROM test_table1 a JOIN test_table2 b ON a.key = b.key;

INSERT OVERWRITE TABLE test_table_out PARTITION (part = '1')
SELECT /*+ MAPJOIN(a) */ a.key, b.value FROM test_table1 a JOIN test_table2 b ON a.key = b.key;

DESCRIBE FORMATTED test_table_out PARTITION (part = '1');

-- Test SMB join doesn't affect inference, should be bucketed and sorted by key
EXPLAIN INSERT OVERWRITE TABLE test_table_out PARTITION (part = '1')
SELECT /*+ MAPJOIN(a) */ b.value, count(*) FROM test_table1 a JOIN test_table2 b ON a.key = b.key
GROUP BY b.value;

INSERT OVERWRITE TABLE test_table_out PARTITION (part = '1')
SELECT /*+ MAPJOIN(a) */ b.value, count(*) FROM test_table1 a JOIN test_table2 b ON a.key = b.key
GROUP BY b.value;

DESCRIBE FORMATTED test_table_out PARTITION (part = '1');

