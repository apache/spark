set hive.exec.infer.bucket.sort=true;
set hive.exec.infer.bucket.sort.num.buckets.power.two=true;
set hive.exec.reducers.bytes.per.reducer=2500;

-- This tests inferring how data is bucketed/sorted from the operators in the reducer
-- and populating that information in partitions' metadata, it also verifies that the
-- number of reducers chosen will be a power of two

CREATE TABLE test_table (key STRING, value STRING) PARTITIONED BY (part STRING);

-- Test group by, should be bucketed and sorted by group by key
INSERT OVERWRITE TABLE test_table PARTITION (part = '1') 
SELECT key, count(*) FROM src GROUP BY key;

DESCRIBE FORMATTED test_table PARTITION (part = '1');

-- Test join, should be bucketed and sorted by join key
INSERT OVERWRITE TABLE test_table PARTITION (part = '1') 
SELECT a.key, a.value FROM src a JOIN src b ON a.key = b.key;

DESCRIBE FORMATTED test_table PARTITION (part = '1');

-- Test join with two keys, should be bucketed and sorted by join keys
INSERT OVERWRITE TABLE test_table PARTITION (part = '1') 
SELECT a.key, a.value FROM src a JOIN src b ON a.key = b.key AND a.value = b.value;

DESCRIBE FORMATTED test_table PARTITION (part = '1');

-- Test join on three tables on same key, should be bucketed and sorted by join key
INSERT OVERWRITE TABLE test_table PARTITION (part = '1') 
SELECT a.key, c.value FROM src a JOIN src b ON (a.key = b.key) JOIN src c ON (b.key = c.key);

DESCRIBE FORMATTED test_table PARTITION (part = '1');

-- Test join on three tables on different keys, should be bucketed and sorted by latter key
INSERT OVERWRITE TABLE test_table PARTITION (part = '1') 
SELECT a.key, c.value FROM src a JOIN src b ON (a.key = b.key) JOIN src c ON (b.value = c.value);

DESCRIBE FORMATTED test_table PARTITION (part = '1');

-- Test group by in subquery with another group by outside, should be bucketed and sorted by the
-- key of the outer group by
INSERT OVERWRITE TABLE test_table PARTITION (part = '1')
SELECT count(1), value FROM (SELECT key, count(1) as value FROM src group by key) a group by value;

DESCRIBE FORMATTED test_table PARTITION (part = '1');
