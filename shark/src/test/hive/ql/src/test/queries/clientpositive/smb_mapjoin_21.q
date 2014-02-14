set hive.optimize.bucketmapjoin = true;
set hive.optimize.bucketmapjoin.sortedmerge = true;
set hive.enforce.bucketing=true;
set hive.enforce.sorting=true;
set hive.exec.reducers.max = 1;
set hive.merge.mapfiles=false;
set hive.merge.mapredfiles=false; 

-- Create two bucketed and sorted tables
CREATE TABLE test_table1 (key INT, value STRING) PARTITIONED BY (ds STRING)
CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS;
CREATE TABLE test_table2 (key INT, value STRING) PARTITIONED BY (ds STRING)
CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS;

FROM src
INSERT OVERWRITE TABLE test_table1 PARTITION (ds = '1') SELECT *;

-- Insert data into the bucketed table by selecting from another bucketed table
-- This should be a map-only operation
EXPLAIN
INSERT OVERWRITE TABLE test_table2 PARTITION (ds = '1')
SELECT a.key, a.value FROM test_table1 a WHERE a.ds = '1';

drop table test_table2;

CREATE TABLE test_table2 (key INT, value STRING) PARTITIONED BY (ds STRING)
CLUSTERED BY (key) SORTED BY (key desc) INTO 2 BUCKETS;

-- Insert data into the bucketed table by selecting from another bucketed table
-- This should be a map-reduce operation since the sort orders does not match
EXPLAIN
INSERT OVERWRITE TABLE test_table2 PARTITION (ds = '1')
SELECT a.key, a.value FROM test_table1 a WHERE a.ds = '1';

drop table test_table2;

CREATE TABLE test_table2 (key INT, value STRING) PARTITIONED BY (ds STRING)
CLUSTERED BY (key) SORTED BY (key, value) INTO 2 BUCKETS;

-- Insert data into the bucketed table by selecting from another bucketed table
-- This should be a map-reduce operation since the sort columns do not match
EXPLAIN
INSERT OVERWRITE TABLE test_table2 PARTITION (ds = '1')
SELECT a.key, a.value FROM test_table1 a WHERE a.ds = '1';

drop table test_table2;

CREATE TABLE test_table2 (key INT, value STRING) PARTITIONED BY (ds STRING)
CLUSTERED BY (key) SORTED BY (value) INTO 2 BUCKETS;

-- Insert data into the bucketed table by selecting from another bucketed table
-- This should be a map-reduce operation since the sort columns do not match
EXPLAIN
INSERT OVERWRITE TABLE test_table2 PARTITION (ds = '1')
SELECT a.key, a.value FROM test_table1 a WHERE a.ds = '1';

drop table test_table2;

CREATE TABLE test_table2 (key INT, value STRING) PARTITIONED BY (ds STRING)
CLUSTERED BY (key) SORTED BY (key) INTO 4 BUCKETS;

-- Insert data into the bucketed table by selecting from another bucketed table
-- This should be a map-reduce operation since the number of buckets do not match
EXPLAIN
INSERT OVERWRITE TABLE test_table2 PARTITION (ds = '1')
SELECT a.key, a.value FROM test_table1 a WHERE a.ds = '1';

drop table test_table2;

CREATE TABLE test_table2 (key INT, value STRING) PARTITIONED BY (ds STRING)
CLUSTERED BY (key) INTO 2 BUCKETS;

-- Insert data into the bucketed table by selecting from another bucketed table
-- This should be a map-reduce operation since sort columns do not match
EXPLAIN
INSERT OVERWRITE TABLE test_table2 PARTITION (ds = '1')
SELECT a.key, a.value FROM test_table1 a WHERE a.ds = '1';
