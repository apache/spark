set hive.enforce.bucketing = true;
set hive.enforce.sorting = true;
set hive.exec.reducers.max = 10;
set hive.map.groupby.sorted=true;

CREATE TABLE T1(key STRING, val STRING)
CLUSTERED BY (key) SORTED BY (val) INTO 2 BUCKETS STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../data/files/T1.txt' INTO TABLE T1;

-- perform an insert to make sure there are 2 files
INSERT OVERWRITE TABLE T1 select key, val from T1;

CREATE TABLE outputTbl1(val string, cnt int);

-- The plan should not be converted to a map-side group by even though the group by key
-- matches the sorted key. Adding a order by at the end to make the test results deterministic
EXPLAIN
INSERT OVERWRITE TABLE outputTbl1
SELECT val, count(1) FROM T1 GROUP BY val;

INSERT OVERWRITE TABLE outputTbl1
SELECT val, count(1) FROM T1 GROUP BY val;

SELECT * FROM outputTbl1 ORDER BY val;
