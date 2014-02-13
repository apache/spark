set hive.optimize.bucketmapjoin = true;
set hive.optimize.bucketmapjoin.sortedmerge = true;
set hive.input.format = org.apache.hadoop.hive.ql.io.BucketizedHiveInputFormat;
set hive.enforce.bucketing=true;
set hive.enforce.sorting=true;
set hive.exec.reducers.max = 1;
set hive.merge.mapfiles=false;
set hive.merge.mapredfiles=false; 

-- This test verifies that the sort merge join optimizer works when the tables are sorted on columns which is a superset
-- of join columns

-- Create bucketed and sorted tables
CREATE TABLE test_table1 (key INT, value STRING) CLUSTERED BY (key) SORTED BY (key ASC, value ASC) INTO 16 BUCKETS;
CREATE TABLE test_table2 (key INT, value STRING) CLUSTERED BY (key) SORTED BY (key ASC, value ASC) INTO 16 BUCKETS;

FROM src
INSERT OVERWRITE TABLE test_table1 SELECT *
INSERT OVERWRITE TABLE test_table2 SELECT *;

-- it should be converted to a sort-merge join, since the first sort column (#join columns = 1) contains the join columns
EXPLAIN EXTENDED
SELECT /*+mapjoin(b)*/ * FROM test_table1 a JOIN test_table2 b ON a.key = b.key ORDER BY a.key LIMIT 10;
SELECT /*+mapjoin(b)*/ * FROM test_table1 a JOIN test_table2 b ON a.key = b.key ORDER BY a.key LIMIT 10;

DROP TABLE test_table1;
DROP TABLE test_table2;

-- Create bucketed and sorted tables
CREATE TABLE test_table1 (key INT, key2 INT, value STRING) CLUSTERED BY (key) SORTED BY (key ASC, key2 ASC, value ASC) INTO 16 BUCKETS;
CREATE TABLE test_table2 (key INT, key2 INT, value STRING) CLUSTERED BY (key) SORTED BY (key ASC, key2 ASC, value ASC) INTO 16 BUCKETS;

FROM src
INSERT OVERWRITE TABLE test_table1 SELECT key, key, value
INSERT OVERWRITE TABLE test_table2 SELECT key, key, value;

-- it should be converted to a sort-merge join, since the first 2 sort columns (#join columns = 2) contain the join columns
EXPLAIN EXTENDED
SELECT /*+mapjoin(b)*/ * FROM test_table1 a JOIN test_table2 b ON a.key = b.key and a.key2 = b.key2 ORDER BY a.key LIMIT 10;
SELECT /*+mapjoin(b)*/ * FROM test_table1 a JOIN test_table2 b ON a.key = b.key and a.key2 = b.key2 ORDER BY a.key LIMIT 10;

-- it should be converted to a sort-merge join, since the first 2 sort columns (#join columns = 2) contain the join columns
-- even if the order is not the same
EXPLAIN EXTENDED
SELECT /*+mapjoin(b)*/ * FROM test_table1 a JOIN test_table2 b ON a.key2 = b.key2 and a.key = b.key ORDER BY a.key LIMIT 10;
SELECT /*+mapjoin(b)*/ * FROM test_table1 a JOIN test_table2 b ON a.key2 = b.key2 and a.key = b.key ORDER BY a.key LIMIT 10;

-- it should not be converted to a sort-merge join, since the first 2 sort columns (#join columns = 2) do not contain all 
-- the join columns
EXPLAIN EXTENDED
SELECT /*+mapjoin(b)*/ * FROM test_table1 a JOIN test_table2 b ON a.key = b.key and a.value = b.value ORDER BY a.key LIMIT 10;
SELECT /*+mapjoin(b)*/ * FROM test_table1 a JOIN test_table2 b ON a.key = b.key and a.value = b.value ORDER BY a.key LIMIT 10;

DROP TABLE test_table1;
DROP TABLE test_table2;
