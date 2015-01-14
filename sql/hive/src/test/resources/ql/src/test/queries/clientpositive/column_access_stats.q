SET hive.exec.post.hooks=org.apache.hadoop.hive.ql.hooks.CheckColumnAccessHook;
SET hive.stats.collect.scancols=true;

-- This test is used for testing the ColumnAccessAnalyzer

CREATE TABLE T1(key STRING, val STRING) STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '../../data/files/T1.txt' INTO TABLE T1;

CREATE TABLE T2(key STRING, val STRING) STORED AS TEXTFILE;
CREATE TABLE T3(key STRING, val STRING) STORED AS TEXTFILE;
CREATE TABLE T4(key STRING, val STRING) PARTITIONED BY (p STRING);

-- Simple select queries
SELECT key FROM T1 ORDER BY key;
SELECT key, val FROM T1 ORDER BY key, val;
SELECT 1 FROM T1;
SELECT key, val from T4 where p=1;
SELECT val FROM T4 where p=1;
SELECT p, val FROM T4 where p=1;

-- More complicated select queries
EXPLAIN SELECT key FROM (SELECT key, val FROM T1) subq1 ORDER BY key;
SELECT key FROM (SELECT key, val FROM T1) subq1 ORDER BY key;
EXPLAIN SELECT k FROM (SELECT key as k, val as v FROM T1) subq1 ORDER BY k;
SELECT k FROM (SELECT key as k, val as v FROM T1) subq1 ORDER BY k;
SELECT key + 1 as k FROM T1 ORDER BY k;
SELECT key + val as k FROM T1 ORDER BY k;

-- Work with union
EXPLAIN
SELECT * FROM (
SELECT key as c FROM T1
 UNION ALL
SELECT val as c FROM T1
) subq1 ORDER BY c;

SELECT * FROM (
SELECT key as c FROM T1
 UNION ALL
SELECT val as c FROM T1
) subq1 ORDER BY c;

EXPLAIN
SELECT * FROM (
SELECT key as c FROM T1
 UNION ALL
SELECT key as c FROM T1
) subq1 ORDER BY c;

SELECT * FROM (
SELECT key as c FROM T1
 UNION ALL
SELECT key as c FROM T1
) subq1 ORDER BY c;

-- Work with insert overwrite
FROM T1
INSERT OVERWRITE TABLE T2 SELECT key, count(1) GROUP BY key
INSERT OVERWRITE TABLE T3 SELECT key, sum(val) GROUP BY key;

-- Simple joins
SELECT *
FROM T1 JOIN T2
ON T1.key = T2.key
ORDER BY T1.key, T1.val, T2.key, T2.val;

EXPLAIN
SELECT T1.key
FROM T1 JOIN T2
ON T1.key = T2.key
ORDER BY T1.key;

SELECT T1.key
FROM T1 JOIN T2
ON T1.key = T2.key
ORDER BY T1.key;

SELECT *
FROM T1 JOIN T2
ON T1.key = T2.key AND T1.val = T2.val
ORDER BY T1.key, T1.val;

-- Map join
SELECT /*+ MAPJOIN(a) */ * 
FROM T1 a JOIN T2 b 
ON a.key = b.key
ORDER BY a.key, a.val, b.key, b.val;

-- More joins
EXPLAIN
SELECT *
FROM T1 JOIN T2
ON T1.key = T2.key AND T1.val = 3 and T2.val = 3
ORDER BY T1.key, T1.val;

SELECT *
FROM T1 JOIN T2
ON T1.key = T2.key AND T1.val = 3 and T2.val = 3
ORDER BY T1.key, T1.val;

EXPLAIN
SELECT subq1.val
FROM 
(
  SELECT val FROM T1 WHERE key = 5  
) subq1
JOIN 
(
  SELECT val FROM T2 WHERE key = 6
) subq2 
ON subq1.val = subq2.val
ORDER BY subq1.val;

SELECT subq1.val
FROM 
(
  SELECT val FROM T1 WHERE key = 5  
) subq1
JOIN 
(
  SELECT val FROM T2 WHERE key = 6
) subq2 
ON subq1.val = subq2.val
ORDER BY subq1.val;

-- Join followed by join
EXPLAIN
SELECT *
FROM
(
  SELECT subq1.key as key
  FROM
  (
    SELECT key, val FROM T1
  ) subq1
  JOIN
  (
    SELECT key, 'teststring' as val FROM T2
  ) subq2
  ON subq1.key = subq2.key
) T4
JOIN T3
ON T3.key = T4.key
ORDER BY T3.key, T4.key;

SELECT *
FROM
(
  SELECT subq1.key as key
  FROM
  (
    SELECT key, val FROM T1
  ) subq1
  JOIN
  (
    SELECT key, 'teststring' as val FROM T2
  ) subq2
  ON subq1.key = subq2.key
) T4
JOIN T3
ON T3.key = T4.key
ORDER BY T3.key, T4.key;
