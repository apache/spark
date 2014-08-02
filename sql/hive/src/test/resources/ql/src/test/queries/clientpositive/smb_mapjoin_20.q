set hive.optimize.bucketmapjoin = true;
set hive.optimize.bucketmapjoin.sortedmerge = true;
set hive.enforce.bucketing=true;
set hive.enforce.sorting=true;
set hive.exec.reducers.max = 1;
set hive.merge.mapfiles=false;
set hive.merge.mapredfiles=false; 

-- Create two bucketed and sorted tables
CREATE TABLE test_table1 (key int, value STRING) PARTITIONED BY (ds STRING)
CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS;
CREATE TABLE test_table2 (key STRING, value1 STRING, value2 string) PARTITIONED BY (ds STRING)
CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS;

FROM src
INSERT OVERWRITE TABLE test_table1 PARTITION (ds = '1') SELECT *;

-- Insert data into the bucketed table by selecting from another bucketed table
-- with different datatypes. This should be a map-reduce operation
EXPLAIN
INSERT OVERWRITE TABLE test_table2 PARTITION (ds = '1')
SELECT a.key, a.value, a.value FROM test_table1 a WHERE a.ds = '1';

INSERT OVERWRITE TABLE test_table2 PARTITION (ds = '1')
SELECT a.key, a.value, a.value FROM test_table1 a WHERE a.ds = '1';

select count(*) from test_table2 where ds = '1';
select count(*) from test_table2 where ds = '1' and hash(key) % 2 = 0;
select count(*) from test_table2 where ds = '1' and hash(key) % 2 = 1;

CREATE TABLE test_table3 (key STRING, value1 int, value2 string) PARTITIONED BY (ds STRING)
CLUSTERED BY (value1) SORTED BY (value1) INTO 2 BUCKETS;

-- Insert data into the bucketed table by selecting from another bucketed table
-- This should be a map-only operation, although the bucketing positions dont match
EXPLAIN
INSERT OVERWRITE TABLE test_table3 PARTITION (ds = '1')
SELECT a.value, a.key, a.value FROM test_table1 a WHERE a.ds = '1';

INSERT OVERWRITE TABLE test_table3 PARTITION (ds = '1')
SELECT a.value, a.key, a.value FROM test_table1 a WHERE a.ds = '1';

select count(*) from test_table3 where ds = '1';
select count(*) from test_table3 where ds = '1' and hash(value1) % 2 = 0;
select count(*) from test_table3 where ds = '1' and hash(value1) % 2 = 1;
select count(*) from test_table3 tablesample (bucket 1 out of 2) s where ds = '1';
select count(*) from test_table3 tablesample (bucket 2 out of 2) s where ds = '1';

-- Insert data into the bucketed table by selecting from another bucketed table
-- However, since an expression is being selected, it should involve a reducer
EXPLAIN
INSERT OVERWRITE TABLE test_table2 PARTITION (ds = '2')
SELECT a.key+a.key, a.value, a.value FROM test_table1 a WHERE a.ds = '1';
