set hive.enforce.bucketing = true;
set hive.enforce.sorting = true;
set hive.exec.reducers.max = 10;
set hive.map.groupby.sorted=true;

CREATE TABLE T1(key STRING, val STRING)
CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/T1.txt' INTO TABLE T1;

-- perform an insert to make sure there are 2 files
INSERT OVERWRITE TABLE T1 select key, val from T1;

CREATE TABLE outputTbl1(key int, cnt int);

-- The plan should be converted to a map-side group by if the group by key
-- matches the sorted key
-- addind a order by at the end to make the test results deterministic
EXPLAIN EXTENDED
INSERT OVERWRITE TABLE outputTbl1
SELECT key, count(1) FROM T1 GROUP BY key;

INSERT OVERWRITE TABLE outputTbl1
SELECT key, count(1) FROM T1 GROUP BY key;

SELECT * FROM outputTbl1 ORDER BY key;

CREATE TABLE outputTbl2(key1 int, key2 string, cnt int);

-- no map-side group by even if the group by key is a superset of sorted key
EXPLAIN EXTENDED
INSERT OVERWRITE TABLE outputTbl2
SELECT key, val, count(1) FROM T1 GROUP BY key, val;

INSERT OVERWRITE TABLE outputTbl2
SELECT key, val, count(1) FROM T1 GROUP BY key, val;

SELECT * FROM outputTbl2 ORDER BY key1, key2;

-- It should work for sub-queries
EXPLAIN EXTENDED 
INSERT OVERWRITE TABLE outputTbl1
SELECT key, count(1) FROM (SELECT key, val FROM T1) subq1 GROUP BY key;

INSERT OVERWRITE TABLE outputTbl1
SELECT key, count(1) FROM (SELECT key, val FROM T1) subq1 GROUP BY key;

SELECT * FROM outputTbl1 ORDER BY key;

-- It should work for sub-queries with column aliases
EXPLAIN EXTENDED
INSERT OVERWRITE TABLE outputTbl1
SELECT k, count(1) FROM (SELECT key as k, val as v FROM T1) subq1 GROUP BY k;

INSERT OVERWRITE TABLE outputTbl1
SELECT k, count(1) FROM (SELECT key as k, val as v FROM T1) subq1 GROUP BY k;

SELECT * FROM outputTbl1 ORDER BY key;

CREATE TABLE outputTbl3(key1 int, key2 int, cnt int);

-- The plan should be converted to a map-side group by if the group by key contains a constant followed
-- by a match to the sorted key
EXPLAIN EXTENDED 
INSERT OVERWRITE TABLE outputTbl3
SELECT 1, key, count(1) FROM T1 GROUP BY 1, key;

INSERT OVERWRITE TABLE outputTbl3
SELECT 1, key, count(1) FROM T1 GROUP BY 1, key;

SELECT * FROM outputTbl3 ORDER BY key1, key2;

CREATE TABLE outputTbl4(key1 int, key2 int, key3 string, cnt int);

-- no map-side group by if the group by key contains a constant followed by another column
EXPLAIN EXTENDED 
INSERT OVERWRITE TABLE outputTbl4
SELECT key, 1, val, count(1) FROM T1 GROUP BY key, 1, val;

INSERT OVERWRITE TABLE outputTbl4
SELECT key, 1, val, count(1) FROM T1 GROUP BY key, 1, val;

SELECT * FROM outputTbl4 ORDER BY key1, key2, key3;

-- no map-side group by if the group by key contains a function
EXPLAIN EXTENDED 
INSERT OVERWRITE TABLE outputTbl3
SELECT key, key + 1, count(1) FROM T1 GROUP BY key, key + 1;

INSERT OVERWRITE TABLE outputTbl3
SELECT key, key + 1, count(1) FROM T1 GROUP BY key, key + 1;

SELECT * FROM outputTbl3 ORDER BY key1, key2;

-- it should not matter what follows the group by
-- test various cases

-- group by followed by another group by
EXPLAIN EXTENDED 
INSERT OVERWRITE TABLE outputTbl1
SELECT key + key, sum(cnt) from
(SELECT key, count(1) as cnt FROM T1 GROUP BY key) subq1
group by key + key;

INSERT OVERWRITE TABLE outputTbl1
SELECT key + key, sum(cnt) from
(SELECT key, count(1) as cnt FROM T1 GROUP BY key) subq1
group by key + key;

SELECT * FROM outputTbl1 ORDER BY key;

-- group by followed by a union
EXPLAIN EXTENDED 
INSERT OVERWRITE TABLE outputTbl1
SELECT * FROM (
SELECT key, count(1) FROM T1 GROUP BY key
  UNION ALL
SELECT key, count(1) FROM T1 GROUP BY key
) subq1;

INSERT OVERWRITE TABLE outputTbl1
SELECT * FROM (
SELECT key, count(1) FROM T1 GROUP BY key
  UNION ALL
SELECT key, count(1) FROM T1 GROUP BY key
) subq1;

SELECT * FROM outputTbl1 ORDER BY key;

-- group by followed by a union where one of the sub-queries is map-side group by
EXPLAIN EXTENDED
INSERT OVERWRITE TABLE outputTbl1
SELECT * FROM (
SELECT key, count(1) FROM T1 GROUP BY key
  UNION ALL
SELECT key + key as key, count(1) FROM T1 GROUP BY key + key
) subq1;

INSERT OVERWRITE TABLE outputTbl1
SELECT * FROM (
SELECT key, count(1) as cnt FROM T1 GROUP BY key
  UNION ALL
SELECT key + key as key, count(1) as cnt FROM T1 GROUP BY key + key
) subq1;

SELECT * FROM outputTbl1 ORDER BY key;

-- group by followed by a join
EXPLAIN EXTENDED 
INSERT OVERWRITE TABLE outputTbl1
SELECT subq1.key, subq1.cnt+subq2.cnt FROM 
(SELECT key, count(1) as cnt FROM T1 GROUP BY key) subq1
JOIN
(SELECT key, count(1) as cnt FROM T1 GROUP BY key) subq2
ON subq1.key = subq2.key;

INSERT OVERWRITE TABLE outputTbl1
SELECT subq1.key, subq1.cnt+subq2.cnt FROM 
(SELECT key, count(1) as cnt FROM T1 GROUP BY key) subq1
JOIN
(SELECT key, count(1) as cnt FROM T1 GROUP BY key) subq2
ON subq1.key = subq2.key;

SELECT * FROM outputTbl1 ORDER BY key;

-- group by followed by a join where one of the sub-queries can be performed in the mapper
EXPLAIN EXTENDED 
SELECT * FROM 
(SELECT key, count(1) FROM T1 GROUP BY key) subq1
JOIN
(SELECT key, val, count(1) FROM T1 GROUP BY key, val) subq2
ON subq1.key = subq2.key;

CREATE TABLE T2(key STRING, val STRING)
CLUSTERED BY (key, val) SORTED BY (key, val) INTO 2 BUCKETS STORED AS TEXTFILE;

-- perform an insert to make sure there are 2 files
INSERT OVERWRITE TABLE T2 select key, val from T1;

-- no mapside sort group by if the group by is a prefix of the sorted key
EXPLAIN EXTENDED 
INSERT OVERWRITE TABLE outputTbl1
SELECT key, count(1) FROM T2 GROUP BY key;

INSERT OVERWRITE TABLE outputTbl1
SELECT key, count(1) FROM T2 GROUP BY key;

SELECT * FROM outputTbl1 ORDER BY key;

-- The plan should be converted to a map-side group by if the group by key contains a constant in between the
-- sorted keys
EXPLAIN EXTENDED 
INSERT OVERWRITE TABLE outputTbl4
SELECT key, 1, val, count(1) FROM T2 GROUP BY key, 1, val;

INSERT OVERWRITE TABLE outputTbl4
SELECT key, 1, val, count(1) FROM T2 GROUP BY key, 1, val;

SELECT * FROM outputTbl4 ORDER BY key1, key2, key3;

CREATE TABLE outputTbl5(key1 int, key2 int, key3 string, key4 int, cnt int);

-- The plan should be converted to a map-side group by if the group by key contains a constant in between the
-- sorted keys followed by anything
EXPLAIN EXTENDED 
INSERT OVERWRITE TABLE outputTbl5
SELECT key, 1, val, 2, count(1) FROM T2 GROUP BY key, 1, val, 2;

INSERT OVERWRITE TABLE outputTbl5
SELECT key, 1, val, 2, count(1) FROM T2 GROUP BY key, 1, val, 2;

SELECT * FROM outputTbl5 
ORDER BY key1, key2, key3, key4;

-- contants from sub-queries should work fine
EXPLAIN EXTENDED
INSERT OVERWRITE TABLE outputTbl4
SELECT key, constant, val, count(1) from 
(SELECT key, 1 as constant, val from T2)subq
group by key, constant, val;

INSERT OVERWRITE TABLE outputTbl4
SELECT key, constant, val, count(1) from 
(SELECT key, 1 as constant, val from T2)subq
group by key, constant, val;

SELECT * FROM outputTbl4 ORDER BY key1, key2, key3;

-- multiple levels of contants from sub-queries should work fine
EXPLAIN EXTENDED
INSERT OVERWRITE TABLE outputTbl4
select key, constant3, val, count(1) from
(
SELECT key, constant as constant2, val, 2 as constant3 from 
(SELECT key, 1 as constant, val from T2)subq
)subq2
group by key, constant3, val;

INSERT OVERWRITE TABLE outputTbl4
select key, constant3, val, count(1) from
(
SELECT key, constant as constant2, val, 2 as constant3 from 
(SELECT key, 1 as constant, val from T2)subq
)subq2
group by key, constant3, val;

SELECT * FROM outputTbl4 ORDER BY key1, key2, key3;

set hive.map.aggr=true;
set hive.multigroupby.singlereducer=false;
set mapreduce.job.reduces=31;

CREATE TABLE DEST1(key INT, cnt INT);
CREATE TABLE DEST2(key INT, val STRING, cnt INT);

SET hive.exec.compress.intermediate=true;
SET hive.exec.compress.output=true; 

EXPLAIN
FROM T2
INSERT OVERWRITE TABLE DEST1 SELECT key, count(1) GROUP BY key
INSERT OVERWRITE TABLE DEST2 SELECT key, val, count(1) GROUP BY key, val;

FROM T2
INSERT OVERWRITE TABLE DEST1 SELECT key, count(1) GROUP BY key
INSERT OVERWRITE TABLE DEST2 SELECT key, val, count(1) GROUP BY key, val;

select * from DEST1 ORDER BY key, cnt;
select * from DEST2 ORDER BY key, val, val;

-- multi-table insert with a sub-query
EXPLAIN
FROM (select key, val from T2 where key = 8) x
INSERT OVERWRITE TABLE DEST1 SELECT key, count(1) GROUP BY key
INSERT OVERWRITE TABLE DEST2 SELECT key, val, count(1) GROUP BY key, val;

FROM (select key, val from T2 where key = 8) x
INSERT OVERWRITE TABLE DEST1 SELECT key, count(1) GROUP BY key
INSERT OVERWRITE TABLE DEST2 SELECT key, val, count(1) GROUP BY key, val;

select * from DEST1 ORDER BY key, cnt;
select * from DEST2 ORDER BY key, val, cnt;
