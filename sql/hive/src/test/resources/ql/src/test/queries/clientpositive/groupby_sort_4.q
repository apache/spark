set hive.enforce.bucketing = true;
set hive.enforce.sorting = true;
set hive.exec.reducers.max = 10;
set hive.map.groupby.sorted=true;

CREATE TABLE T1(key STRING, val STRING)
CLUSTERED BY (key, val) SORTED BY (key) INTO 2 BUCKETS STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/T1.txt' INTO TABLE T1;

-- perform an insert to make sure there are 2 files
INSERT OVERWRITE TABLE T1 select key, val from T1;

CREATE TABLE outputTbl1(key STRING, cnt INT);

-- The plan should not be converted to a map-side group by.
-- However, there should no hash-based aggregation on the map-side
EXPLAIN
INSERT OVERWRITE TABLE outputTbl1
SELECT key, count(1) FROM T1 GROUP BY key;

INSERT OVERWRITE TABLE outputTbl1
SELECT key, count(1) FROM T1 GROUP BY key;

SELECT * FROM outputTbl1 ORDER BY key;

CREATE TABLE outputTbl2(key STRING, val STRING, cnt INT);

-- The plan should not be converted to a map-side group by.
-- Hash-based aggregations should be performed on the map-side
EXPLAIN
INSERT OVERWRITE TABLE outputTbl2
SELECT key, val, count(1) FROM T1 GROUP BY key, val;

INSERT OVERWRITE TABLE outputTbl2
SELECT key, val, count(1) FROM T1 GROUP BY key, val;

SELECT * FROM outputTbl2 ORDER BY key, val;
