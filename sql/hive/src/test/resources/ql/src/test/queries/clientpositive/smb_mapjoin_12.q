set hive.optimize.bucketmapjoin = true;
set hive.optimize.bucketmapjoin.sortedmerge = true;
set hive.input.format = org.apache.hadoop.hive.ql.io.BucketizedHiveInputFormat;
set hive.enforce.bucketing=true;
set hive.enforce.sorting=true;
set hive.exec.reducers.max = 1;
set hive.merge.mapfiles=false;
set hive.merge.mapredfiles=false; 

-- This test verifies that the output of a sort merge join on 1 big partition with multiple small partitions is bucketed and sorted

-- Create two bucketed and sorted tables
CREATE TABLE test_table1 (key INT, value STRING) PARTITIONED BY (ds STRING) CLUSTERED BY (key) SORTED BY (key) INTO 16 BUCKETS;
CREATE TABLE test_table2 (key INT, value STRING) PARTITIONED BY (ds STRING) CLUSTERED BY (key) SORTED BY (key) INTO 16 BUCKETS;

FROM src
INSERT OVERWRITE TABLE test_table1 PARTITION (ds = '1') SELECT *
INSERT OVERWRITE TABLE test_table2 PARTITION (ds = '1') SELECT *
INSERT OVERWRITE TABLE test_table2 PARTITION (ds = '2') SELECT *
INSERT OVERWRITE TABLE test_table2 PARTITION (ds = '3') SELECT *;

set hive.enforce.bucketing=false;
set hive.enforce.sorting=false;

-- Create a bucketed table
CREATE TABLE test_table3 (key INT, value STRING) PARTITIONED BY (ds STRING) CLUSTERED BY (key) SORTED BY (key) INTO 16 BUCKETS;

-- Insert data into the bucketed table by joining the two bucketed and sorted tables, bucketing is not enforced
EXPLAIN EXTENDED
INSERT OVERWRITE TABLE test_table3 PARTITION (ds = '1') SELECT /*+ MAPJOIN(b) */ a.key, b.value FROM test_table1 a JOIN test_table2 b ON a.key = b.key AND a.ds = '1' AND b.ds >= '1';

INSERT OVERWRITE TABLE test_table3 PARTITION (ds = '1') SELECT /*+ MAPJOIN(b) */ a.key, b.value FROM test_table1 a JOIN test_table2 b ON a.key = b.key AND a.ds = '1' AND b.ds >= '1';

-- Join data from a sampled bucket to verify the data is bucketed
SELECT COUNT(*) FROM test_table3 TABLESAMPLE(BUCKET 2 OUT OF 16) a JOIN test_table1 TABLESAMPLE(BUCKET 2 OUT OF 16) b ON a.key = b.key AND a.ds = '1' AND b.ds='1';

set hive.optimize.bucketmapjoin = true;
set hive.optimize.bucketmapjoin.sortedmerge = true;
set hive.input.format = org.apache.hadoop.hive.ql.io.BucketizedHiveInputFormat;

-- Join data from the sampled buckets of 2 tables to verify the data is bucketed and sorted
explain extended
INSERT OVERWRITE TABLE test_table3 PARTITION (ds = '2') 
SELECT /*+mapjoin(b)*/ a.key, concat(a.value, b.value) FROM test_table3 a JOIN test_table1 b ON a.key = b.key AND a.ds = '1' AND b.ds='1';

INSERT OVERWRITE TABLE test_table3 PARTITION (ds = '2') 
SELECT /*+mapjoin(b)*/ a.key, concat(a.value, b.value) FROM test_table3 a JOIN test_table1 b ON a.key = b.key AND a.ds = '1' AND b.ds='1';

SELECT count(*) from test_table3 tablesample (bucket 2 out of 16) a where ds = '2';