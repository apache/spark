set hive.exec.infer.bucket.sort=true;

-- This tests inferring how data is bucketed/sorted from the operators in the reducer
-- and populating that information in partitions' metadata

CREATE TABLE test_table (key STRING, value STRING) PARTITIONED BY (part STRING);

-- Test group by, should be bucketed and sorted by group by key
INSERT OVERWRITE TABLE test_table PARTITION (part = '1') 
SELECT key, count(*) FROM src GROUP BY key;

DESCRIBE FORMATTED test_table PARTITION (part = '1');

-- Test group by where a key isn't selected, should not be bucketed or sorted
INSERT OVERWRITE TABLE test_table PARTITION (part = '1') 
SELECT key, count(*) FROM src GROUP BY key, value;

DESCRIBE FORMATTED test_table PARTITION (part = '1');

-- Test join, should be bucketed and sorted by join key
INSERT OVERWRITE TABLE test_table PARTITION (part = '1') 
SELECT a.key, a.value FROM src a JOIN src b ON a.key = b.key;

DESCRIBE FORMATTED test_table PARTITION (part = '1');

-- Test join with two keys, should be bucketed and sorted by join keys
INSERT OVERWRITE TABLE test_table PARTITION (part = '1') 
SELECT a.key, a.value FROM src a JOIN src b ON a.key = b.key AND a.value = b.value;

DESCRIBE FORMATTED test_table PARTITION (part = '1');

-- Test join with two keys and only one selected, should not be bucketed or sorted
INSERT OVERWRITE TABLE test_table PARTITION (part = '1') 
SELECT a.key, '1' FROM src a JOIN src b ON a.key = b.key AND a.value = b.value;

DESCRIBE FORMATTED test_table PARTITION (part = '1');

-- Test join on three tables on same key, should be bucketed and sorted by join key
INSERT OVERWRITE TABLE test_table PARTITION (part = '1') 
SELECT a.key, c.value FROM src a JOIN src b ON (a.key = b.key) JOIN src c ON (b.key = c.key);

DESCRIBE FORMATTED test_table PARTITION (part = '1');

-- Test join on three tables on different keys, should be bucketed and sorted by latter key
INSERT OVERWRITE TABLE test_table PARTITION (part = '1') 
SELECT a.key, c.value FROM src a JOIN src b ON (a.key = b.key) JOIN src c ON (b.value = c.value);

DESCRIBE FORMATTED test_table PARTITION (part = '1');

-- Test distribute by, should only be bucketed by key
INSERT OVERWRITE TABLE test_table PARTITION (part = '1') 
SELECT key, value FROM src DISTRIBUTE BY key;

DESCRIBE FORMATTED test_table PARTITION (part = '1');

-- Test sort by, should be sorted by key
INSERT OVERWRITE TABLE test_table PARTITION (part = '1') 
SELECT key, value FROM src SORT BY key ASC;

DESCRIBE FORMATTED test_table PARTITION (part = '1');

-- Test sort by desc, should be sorted by key
INSERT OVERWRITE TABLE test_table PARTITION (part = '1') 
SELECT key, value FROM src SORT BY key DESC;

DESCRIBE FORMATTED test_table PARTITION (part = '1');

-- Test cluster by, should be bucketed and sorted by key
INSERT OVERWRITE TABLE test_table PARTITION (part = '1') 
SELECT key, value FROM src CLUSTER BY key;

DESCRIBE FORMATTED test_table PARTITION (part = '1');

-- Test distribute by and sort by different keys, should be bucketed by one key sorted by the other
INSERT OVERWRITE TABLE test_table PARTITION (part = '1') 
SELECT key, value FROM src DISTRIBUTE BY key SORT BY value;

DESCRIBE FORMATTED test_table PARTITION (part = '1');

-- Test join in simple subquery, should be bucketed and sorted on key
INSERT OVERWRITE TABLE test_table PARTITION (part = '1')
SELECT key, value from (SELECT a.key, b.value FROM src a JOIN src b ON (a.key = b.key)) subq;

DESCRIBE FORMATTED test_table PARTITION (part = '1');

-- Test join in simple subquery renaming key column, should be bucketed and sorted on key
INSERT OVERWRITE TABLE test_table PARTITION (part = '1')
SELECT k, value FROM (SELECT a.key as k, b.value FROM src a JOIN src b ON (a.key = b.key)) subq;

DESCRIBE FORMATTED test_table PARTITION (part = '1');

-- Test group by in simple subquery, should be bucketed and sorted on key
INSERT OVERWRITE TABLE test_table PARTITION (part = '1')
SELECT key, cnt from (SELECT key, count(*) as cnt FROM src GROUP BY key) subq;

DESCRIBE FORMATTED test_table PARTITION (part = '1');

-- Test group by in simple subquery renaming key column, should be bucketed and sorted on key
INSERT OVERWRITE TABLE test_table PARTITION (part = '1')
SELECT k, cnt FROM (SELECT key as k, count(*) as cnt FROM src GROUP BY key) subq;

DESCRIBE FORMATTED test_table PARTITION (part = '1');

-- Test group by in subquery with where outside, should still be bucketed and sorted on key
INSERT OVERWRITE TABLE test_table PARTITION (part = '1')
SELECT key, value FROM (SELECT key, count(1) AS value FROM src group by key) a where key < 10;

DESCRIBE FORMATTED test_table PARTITION (part = '1');

-- Test group by in subquery with expression on value, should still be bucketed and sorted on key
INSERT OVERWRITE TABLE test_table PARTITION (part = '1')
SELECT key, value + 1 FROM (SELECT key, count(1) AS value FROM src group by key) a where key < 10;

DESCRIBE FORMATTED test_table PARTITION (part = '1');

-- Test group by in subquery with lateral view outside, should still be bucketed and sorted
INSERT OVERWRITE TABLE test_table PARTITION (part = '1')
SELECT key, value FROM (SELECT key FROM src group by key) a lateral view explode(array(1, 2)) value as value;

DESCRIBE FORMATTED test_table PARTITION (part = '1');

-- Test group by in subquery with another group by outside, should be bucketed and sorted by the
-- key of the outer group by
INSERT OVERWRITE TABLE test_table PARTITION (part = '1')
SELECT count(1), value FROM (SELECT key, count(1) as value FROM src group by key) a group by value;

DESCRIBE FORMATTED test_table PARTITION (part = '1');

-- Test group by in subquery with select on outside reordering the columns, should be bucketed and
-- sorted by the column the group by key ends up in
INSERT OVERWRITE TABLE test_table PARTITION (part = '1')
SELECT value, key FROM (SELECT key, count(1) as value FROM src group by key) a;

DESCRIBE FORMATTED test_table PARTITION (part = '1');

-- Test group by in subquery followed by distribute by, should only be bucketed by the distribute key
INSERT OVERWRITE TABLE test_table PARTITION (part = '1')
SELECT key, value FROM (SELECT key, count(1) as value FROM src group by key) a distribute by key;

DESCRIBE FORMATTED test_table PARTITION (part = '1');

-- Test group by in subquery followed by sort by, should only be sorted by the sort key
INSERT OVERWRITE TABLE test_table PARTITION (part = '1')
SELECT key, value FROM (SELECT key, count(1) as value FROM src group by key) a sort by key;

DESCRIBE FORMATTED test_table PARTITION (part = '1');

-- Test group by in subquery followed by transform script, should not be bucketed or sorted
INSERT OVERWRITE TABLE test_table PARTITION (part = '1')
SELECT TRANSFORM (a.key, a.value) USING 'cat' AS (key, value) FROM (SELECT key, count(1) AS value FROM src GROUP BY KEY) a;

DESCRIBE FORMATTED test_table PARTITION (part = '1');

-- Test group by on function, should be bucketed and sorted by key and value because the function is applied in the mapper
INSERT OVERWRITE TABLE test_table PARTITION (part = '1')
SELECT key, value FROM (SELECT concat(key, "a") AS key, value, count(*)  FROM src GROUP BY concat(key, "a"), value) a;

DESCRIBE FORMATTED test_table PARTITION (part = '1');
