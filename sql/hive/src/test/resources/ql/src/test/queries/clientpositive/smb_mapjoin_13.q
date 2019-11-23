set hive.optimize.bucketmapjoin = true;
set hive.optimize.bucketmapjoin.sortedmerge = true;
set hive.input.format = org.apache.hadoop.hive.ql.io.BucketizedHiveInputFormat;
set hive.enforce.bucketing=true;
set hive.enforce.sorting=true;
set hive.exec.reducers.max = 1;
set hive.merge.mapfiles=false;
set hive.merge.mapredfiles=false; 

-- This test verifies that the sort merge join optimizer works when the tables are joined on columns with different names

-- Create bucketed and sorted tables
CREATE TABLE test_table1 (key INT, value STRING) CLUSTERED BY (key) SORTED BY (key ASC) INTO 16 BUCKETS;
CREATE TABLE test_table2 (value INT, key STRING) CLUSTERED BY (value) SORTED BY (value ASC) INTO 16 BUCKETS;
CREATE TABLE test_table3 (key INT, value STRING) CLUSTERED BY (key, value) SORTED BY (key ASC, value ASC) INTO 16 BUCKETS;
CREATE TABLE test_table4 (key INT, value STRING) CLUSTERED BY (key, value) SORTED BY (value ASC, key ASC) INTO 16 BUCKETS;

FROM src
INSERT OVERWRITE TABLE test_table1 SELECT *
INSERT OVERWRITE TABLE test_table2 SELECT *
INSERT OVERWRITE TABLE test_table3 SELECT *
INSERT OVERWRITE TABLE test_table4 SELECT *;

-- Join data from 2 tables on their respective sorted columns (one each, with different names) and
-- verify sort merge join is used
EXPLAIN EXTENDED
SELECT /*+mapjoin(b)*/ * FROM test_table1 a JOIN test_table2 b ON a.key = b.value ORDER BY a.key LIMIT 10;

SELECT /*+mapjoin(b)*/ * FROM test_table1 a JOIN test_table2 b ON a.key = b.value ORDER BY a.key LIMIT 10;

-- Join data from 2 tables on their respective columns (two each, with the same names but sorted
-- with different priorities) and verify sort merge join is not used
EXPLAIN EXTENDED
SELECT /*+mapjoin(b)*/ * FROM test_table3 a JOIN test_table4 b ON a.key = b.value ORDER BY a.key LIMIT 10;

SELECT /*+mapjoin(b)*/ * FROM test_table3 a JOIN test_table4 b ON a.key = b.value ORDER BY a.key LIMIT 10;
