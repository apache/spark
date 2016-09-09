set hive.auto.convert.join=true;
set hive.auto.convert.sortmerge.join=true;
set hive.optimize.bucketmapjoin = true;
set hive.optimize.bucketmapjoin.sortedmerge = true;
set hive.enforce.bucketing=true;
set hive.enforce.sorting=true;
set hive.exec.reducers.max = 1;
set hive.merge.mapfiles=false;
set hive.merge.mapredfiles=false; 
set hive.auto.convert.sortmerge.join.bigtable.selection.policy=org.apache.hadoop.hive.ql.optimizer.LeftmostBigTableSelectorForAutoSMJ;

set hive.auto.convert.sortmerge.join.to.mapjoin=true;

-- Create two bucketed and sorted tables
CREATE TABLE test_table1 (key INT, key2 INT, value STRING) PARTITIONED BY (ds STRING)
CLUSTERED BY (key, key2) SORTED BY (key ASC, key2 DESC) INTO 2 BUCKETS;
CREATE TABLE test_table2 (key INT, key2 INT, value STRING) PARTITIONED BY (ds STRING)
CLUSTERED BY (key, key2) SORTED BY (key ASC, key2 DESC) INTO 2 BUCKETS;
CREATE TABLE test_table3 (key INT, key2 INT, value STRING) PARTITIONED BY (ds STRING)
CLUSTERED BY (key, key2) SORTED BY (key ASC, key2 DESC) INTO 2 BUCKETS;

FROM src
INSERT OVERWRITE TABLE test_table1 PARTITION (ds = '1') SELECT key, key+1, value where key < 10;

FROM src
INSERT OVERWRITE TABLE test_table2 PARTITION (ds = '1') SELECT key, key+1, value where key < 100;

-- Insert data into the bucketed table by selecting from another bucketed table
-- This should be a map-only operation, since the sort-order matches
EXPLAIN
INSERT OVERWRITE TABLE test_table3 PARTITION (ds = '1')
SELECT a.key, a.key2, concat(a.value, b.value) 
FROM test_table1 a JOIN test_table2 b 
ON a.key = b.key and a.key2 = b.key2 WHERE a.ds = '1' and b.ds = '1';

INSERT OVERWRITE TABLE test_table3 PARTITION (ds = '1')
SELECT a.key, a.key2, concat(a.value, b.value) 
FROM test_table1 a JOIN test_table2 b 
ON a.key = b.key and a.key2 = b.key2 WHERE a.ds = '1' and b.ds = '1';

select * from test_table3 tablesample (bucket 1 out of 2) s where ds = '1';
select * from test_table3 tablesample (bucket 2 out of 2) s where ds = '1';

-- Insert data into the bucketed table by selecting from another bucketed table
-- This should be a map-only operation, since the sort-order matches
EXPLAIN
INSERT OVERWRITE TABLE test_table3 PARTITION (ds = '1')
SELECT subq1.key, subq1.key2, subq1.value from
(
SELECT a.key, a.key2, concat(a.value, b.value) as value
FROM test_table1 a JOIN test_table2 b 
ON a.key = b.key and a.key2 = b.key2 WHERE a.ds = '1' and b.ds = '1'
)subq1;

INSERT OVERWRITE TABLE test_table3 PARTITION (ds = '1')
SELECT subq1.key, subq1.key2, subq1.value from
(
SELECT a.key, a.key2, concat(a.value, b.value) as value
FROM test_table1 a JOIN test_table2 b 
ON a.key = b.key and a.key2 = b.key2 WHERE a.ds = '1' and b.ds = '1'
)subq1;

select * from test_table3 tablesample (bucket 1 out of 2) s where ds = '1';
select * from test_table3 tablesample (bucket 2 out of 2) s where ds = '1';

-- Insert data into the bucketed table by selecting from another bucketed table
-- This should be a map-reduce operation
EXPLAIN
INSERT OVERWRITE TABLE test_table3 PARTITION (ds = '1')
SELECT a.key2, a.key, concat(a.value, b.value) 
FROM test_table1 a JOIN test_table2 b 
ON a.key = b.key and a.key2 = b.key2 WHERE a.ds = '1' and b.ds = '1';

-- Insert data into the bucketed table by selecting from another bucketed table
-- This should be a map-reduce operation
EXPLAIN
INSERT OVERWRITE TABLE test_table3 PARTITION (ds = '1')
SELECT subq1.key2, subq1.key, subq1.value from
(
SELECT a.key, a.key2, concat(a.value, b.value) as value
FROM test_table1 a JOIN test_table2 b 
ON a.key = b.key and a.key2 = b.key2 WHERE a.ds = '1' and b.ds = '1'
)subq1;

-- Insert data into the bucketed table by selecting from another bucketed table
-- This should be a map-only operation
EXPLAIN
INSERT OVERWRITE TABLE test_table3 PARTITION (ds = '1')
SELECT subq2.key, subq2.key2, subq2.value from
(
SELECT subq1.key2, subq1.key, subq1.value from
(
SELECT a.key, a.key2, concat(a.value, b.value) as value
FROM test_table1 a JOIN test_table2 b 
ON a.key = b.key and a.key2 = b.key2 WHERE a.ds = '1' and b.ds = '1'
)subq1
)subq2;

INSERT OVERWRITE TABLE test_table3 PARTITION (ds = '1')
SELECT subq2.key, subq2.key2, subq2.value from
(
SELECT subq1.key2, subq1.key, subq1.value from
(
SELECT a.key, a.key2, concat(a.value, b.value) as value
FROM test_table1 a JOIN test_table2 b 
ON a.key = b.key and a.key2 = b.key2 WHERE a.ds = '1' and b.ds = '1'
)subq1
)subq2;

select * from test_table3 tablesample (bucket 1 out of 2) s where ds = '1';
select * from test_table3 tablesample (bucket 2 out of 2) s where ds = '1';

-- Insert data into the bucketed table by selecting from another bucketed table
-- This should be a map-only operation
EXPLAIN
INSERT OVERWRITE TABLE test_table3 PARTITION (ds = '1')
SELECT subq2.k2, subq2.k1, subq2.value from
(
SELECT subq1.key2 as k1, subq1.key as k2, subq1.value from
(
SELECT a.key, a.key2, concat(a.value, b.value) as value
FROM test_table1 a JOIN test_table2 b 
ON a.key = b.key and a.key2 = b.key2 WHERE a.ds = '1' and b.ds = '1'
)subq1
)subq2;

INSERT OVERWRITE TABLE test_table3 PARTITION (ds = '1')
SELECT subq2.k2, subq2.k1, subq2.value from
(
SELECT subq1.key2 as k1, subq1.key  as k2, subq1.value from
(
SELECT a.key, a.key2, concat(a.value, b.value) as value
FROM test_table1 a JOIN test_table2 b 
ON a.key = b.key and a.key2 = b.key2 WHERE a.ds = '1' and b.ds = '1'
)subq1
)subq2;

select * from test_table3 tablesample (bucket 1 out of 2) s where ds = '1';
select * from test_table3 tablesample (bucket 2 out of 2) s where ds = '1';

CREATE TABLE test_table4 (key INT, key2 INT, value STRING) PARTITIONED BY (ds STRING)
CLUSTERED BY (key, key2) SORTED BY (key DESC, key2 DESC) INTO 2 BUCKETS;

-- Insert data into the bucketed table by selecting from another bucketed table
-- This should be a map-reduce operation
EXPLAIN
INSERT OVERWRITE TABLE test_table4 PARTITION (ds = '1')
SELECT subq2.k2, subq2.k1, subq2.value from
(
SELECT subq1.key2 as k1, subq1.key  as k2, subq1.value from
(
SELECT a.key, a.key2, concat(a.value, b.value) as value
FROM test_table1 a JOIN test_table2 b 
ON a.key = b.key and a.key2 = b.key2 WHERE a.ds = '1' and b.ds = '1'
)subq1
)subq2;
