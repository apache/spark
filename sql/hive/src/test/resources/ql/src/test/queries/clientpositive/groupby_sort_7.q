set hive.enforce.bucketing = true;
set hive.enforce.sorting = true;
set hive.exec.reducers.max = 10;
set hive.map.groupby.sorted=true;

CREATE TABLE T1(key STRING, val STRING) PARTITIONED BY (ds string)
CLUSTERED BY (val) SORTED BY (key, val) INTO 2 BUCKETS STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../data/files/T1.txt' INTO TABLE T1 PARTITION (ds='1');

-- perform an insert to make sure there are 2 files
INSERT OVERWRITE TABLE T1 PARTITION (ds='1') select key, val from T1 where ds = '1';

CREATE TABLE outputTbl1(key STRING, val STRING, cnt INT);

-- The plan should be converted to a map-side group by, since the
-- sorting columns and grouping columns match, and all the bucketing columns
-- are part of sorting columns
EXPLAIN
INSERT OVERWRITE TABLE outputTbl1
SELECT key, val, count(1) FROM T1 where ds = '1' GROUP BY key, val;

INSERT OVERWRITE TABLE outputTbl1
SELECT key, val, count(1) FROM T1 where ds = '1' GROUP BY key, val;

SELECT * FROM outputTbl1 ORDER BY key, val;

DROP TABLE T1;
