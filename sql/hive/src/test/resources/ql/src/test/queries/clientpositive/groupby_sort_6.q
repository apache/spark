set hive.enforce.bucketing = true;
set hive.enforce.sorting = true;
set hive.exec.reducers.max = 10;
set hive.map.groupby.sorted=true;

CREATE TABLE T1(key STRING, val STRING) PARTITIONED BY (ds string);

CREATE TABLE outputTbl1(key int, cnt int);

-- The plan should not be converted to a map-side group since no partition is being accessed
EXPLAIN EXTENDED
INSERT OVERWRITE TABLE outputTbl1
SELECT key, count(1) FROM T1 where ds = '1' GROUP BY key;

INSERT OVERWRITE TABLE outputTbl1
SELECT key, count(1) FROM T1 where ds = '1' GROUP BY key;

SELECT * FROM outputTbl1 ORDER BY key;

LOAD DATA LOCAL INPATH '../../data/files/T1.txt' INTO TABLE T1 PARTITION (ds='2');

-- The plan should not be converted to a map-side group since no partition is being accessed
EXPLAIN EXTENDED
INSERT OVERWRITE TABLE outputTbl1
SELECT key, count(1) FROM T1 where ds = '1' GROUP BY key;

INSERT OVERWRITE TABLE outputTbl1
SELECT key, count(1) FROM T1 where ds = '1' GROUP BY key;

SELECT * FROM outputTbl1 ORDER BY key;

-- The plan should not be converted to a map-side group since the partition being accessed
-- is neither bucketed not sorted
EXPLAIN EXTENDED
INSERT OVERWRITE TABLE outputTbl1
SELECT key, count(1) FROM T1 where ds = '2' GROUP BY key;

INSERT OVERWRITE TABLE outputTbl1
SELECT key, count(1) FROM T1 where ds = '2' GROUP BY key;

SELECT * FROM outputTbl1 ORDER BY key;
