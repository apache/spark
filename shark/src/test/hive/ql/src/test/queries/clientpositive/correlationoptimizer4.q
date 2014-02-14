CREATE TABLE T1(key INT, val STRING);
LOAD DATA LOCAL INPATH '../data/files/T1.txt' INTO TABLE T1;
CREATE TABLE T2(key INT, val STRING);
LOAD DATA LOCAL INPATH '../data/files/T2.txt' INTO TABLE T2;
CREATE TABLE T3(key INT, val STRING);
LOAD DATA LOCAL INPATH '../data/files/T1.txt' INTO TABLE T3;

set hive.auto.convert.join=false;
set hive.optimize.correlation=false;
-- When Correlation Optimizer is turned off, this query will be evaluated
-- by 3 MR jobs. 
-- When Correlation Optimizer is turned on, this query will be evaluated by
-- 2 MR jobs. The subquery tmp will be evaluated in a single MR job.
EXPLAIN
SELECT SUM(HASH(tmp.key)), SUM(HASH(tmp.cnt))
FROM (SELECT y.key AS key, count(1) AS cnt
      FROM T2 x JOIN T1 y ON (x.key = y.key) JOIN T3 z ON (y.key = z.key)
      GROUP BY y.key) tmp;

SELECT SUM(HASH(tmp.key)), SUM(HASH(tmp.cnt))
FROM (SELECT y.key AS key, count(1) AS cnt
      FROM T2 x JOIN T1 y ON (x.key = y.key) JOIN T3 z ON (y.key = z.key)
      GROUP BY y.key) tmp;

set hive.optimize.correlation=true;
EXPLAIN
SELECT SUM(HASH(tmp.key)), SUM(HASH(tmp.cnt))
FROM (SELECT y.key AS key, count(1) AS cnt
      FROM T2 x JOIN T1 y ON (x.key = y.key) JOIN T3 z ON (y.key = z.key)
      GROUP BY y.key) tmp;

SELECT SUM(HASH(tmp.key)), SUM(HASH(tmp.cnt))
FROM (SELECT y.key AS key, count(1) AS cnt
      FROM T2 x JOIN T1 y ON (x.key = y.key) JOIN T3 z ON (y.key = z.key)
      GROUP BY y.key) tmp;

set hive.optimize.correlation=true;
set hive.auto.convert.join=true;
-- Enable hive.auto.convert.join.
EXPLAIN
SELECT SUM(HASH(tmp.key)), SUM(HASH(tmp.cnt))
FROM (SELECT y.key AS key, count(1) AS cnt
      FROM T2 x JOIN T1 y ON (x.key = y.key) JOIN T3 z ON (y.key = z.key)
      GROUP BY y.key) tmp;

SELECT SUM(HASH(tmp.key)), SUM(HASH(tmp.cnt))
FROM (SELECT y.key AS key, count(1) AS cnt
      FROM T2 x JOIN T1 y ON (x.key = y.key) JOIN T3 z ON (y.key = z.key)
      GROUP BY y.key) tmp;

set hive.auto.convert.join=false;
set hive.optimize.correlation=false;
-- This case should be optimized, since the key of GroupByOperator is from the leftmost table
-- of a chain of LEFT OUTER JOINs.
EXPLAIN
SELECT SUM(HASH(tmp.key)), SUM(HASH(tmp.cnt))
FROM (SELECT x.key AS key, count(1) AS cnt
      FROM T2 x LEFT OUTER JOIN T1 y ON (x.key = y.key) LEFT OUTER JOIN T3 z ON (y.key = z.key)
      GROUP BY x.key) tmp;

SELECT SUM(HASH(tmp.key)), SUM(HASH(tmp.cnt))
FROM (SELECT x.key AS key, count(1) AS cnt
      FROM T2 x LEFT OUTER JOIN T1 y ON (x.key = y.key) LEFT OUTER JOIN T3 z ON (y.key = z.key)
      GROUP BY x.key) tmp;

set hive.optimize.correlation=true;
EXPLAIN
SELECT SUM(HASH(tmp.key)), SUM(HASH(tmp.cnt))
FROM (SELECT x.key AS key, count(1) AS cnt
      FROM T2 x LEFT OUTER JOIN T1 y ON (x.key = y.key) LEFT OUTER JOIN T3 z ON (y.key = z.key)
      GROUP BY x.key) tmp;

SELECT SUM(HASH(tmp.key)), SUM(HASH(tmp.cnt))
FROM (SELECT x.key AS key, count(1) AS cnt
      FROM T2 x LEFT OUTER JOIN T1 y ON (x.key = y.key) LEFT OUTER JOIN T3 z ON (y.key = z.key)
      GROUP BY x.key) tmp;

set hive.optimize.correlation=true;
-- This query will not be optimized by correlation optimizer because
-- GroupByOperator uses y.key (a right table of a left outer join)
-- as the key.
EXPLAIN
SELECT SUM(HASH(tmp.key)), SUM(HASH(tmp.cnt))
FROM (SELECT y.key AS key, count(1) AS cnt
      FROM T2 x LEFT OUTER JOIN T1 y ON (x.key = y.key) LEFT OUTER JOIN T3 z ON (y.key = z.key)
      GROUP BY y.key) tmp;

SELECT SUM(HASH(tmp.key)), SUM(HASH(tmp.cnt))
FROM (SELECT y.key AS key, count(1) AS cnt
      FROM T2 x LEFT OUTER JOIN T1 y ON (x.key = y.key) LEFT OUTER JOIN T3 z ON (y.key = z.key)
      GROUP BY y.key) tmp;

set hive.optimize.correlation=false;
-- This case should be optimized, since the key of GroupByOperator is from the rightmost table
-- of a chain of RIGHT OUTER JOINs.
EXPLAIN
SELECT SUM(HASH(tmp.key)), SUM(HASH(tmp.cnt))
FROM (SELECT z.key AS key, count(1) AS cnt
      FROM T2 x RIGHT OUTER JOIN T1 y ON (x.key = y.key) RIGHT OUTER JOIN T3 z ON (y.key = z.key)
      GROUP BY z.key) tmp;

SELECT SUM(HASH(tmp.key)), SUM(HASH(tmp.cnt))
FROM (SELECT z.key AS key, count(1) AS cnt
      FROM T2 x RIGHT OUTER JOIN T1 y ON (x.key = y.key) RIGHT OUTER JOIN T3 z ON (y.key = z.key)
      GROUP BY z.key) tmp;

set hive.optimize.correlation=true;
EXPLAIN
SELECT SUM(HASH(tmp.key)), SUM(HASH(tmp.cnt))
FROM (SELECT z.key AS key, count(1) AS cnt
      FROM T2 x RIGHT OUTER JOIN T1 y ON (x.key = y.key) RIGHT OUTER JOIN T3 z ON (y.key = z.key)
      GROUP BY z.key) tmp;

SELECT SUM(HASH(tmp.key)), SUM(HASH(tmp.cnt))
FROM (SELECT z.key AS key, count(1) AS cnt
      FROM T2 x RIGHT OUTER JOIN T1 y ON (x.key = y.key) RIGHT OUTER JOIN T3 z ON (y.key = z.key)
      GROUP BY z.key) tmp;

set hive.optimize.correlation=true;
-- This query will not be optimized by correlation optimizer because
-- GroupByOperator uses y.key (a left table of a right outer join)
-- as the key.
EXPLAIN
SELECT SUM(HASH(tmp.key)), SUM(HASH(tmp.cnt))
FROM (SELECT y.key AS key, count(1) AS cnt
      FROM T2 x RIGHT OUTER JOIN T1 y ON (x.key = y.key) RIGHT OUTER JOIN T3 z ON (y.key = z.key)
      GROUP BY y.key) tmp;

SELECT SUM(HASH(tmp.key)), SUM(HASH(tmp.cnt))
FROM (SELECT y.key AS key, count(1) AS cnt
      FROM T2 x RIGHT OUTER JOIN T1 y ON (x.key = y.key) RIGHT OUTER JOIN T3 z ON (y.key = z.key)
      GROUP BY y.key) tmp;

set hive.optimize.correlation=false;
-- This case should not be optimized because afer the FULL OUTER JOIN, rows with null keys
-- are not grouped.
EXPLAIN
SELECT SUM(HASH(tmp.key)), SUM(HASH(tmp.cnt))
FROM (SELECT y.key AS key, count(1) AS cnt
      FROM T2 x FULL OUTER JOIN T1 y ON (x.key = y.key) FULL OUTER JOIN T3 z ON (y.key = z.key)
      GROUP BY y.key) tmp;

SELECT SUM(HASH(tmp.key)), SUM(HASH(tmp.cnt))
FROM (SELECT y.key AS key, count(1) AS cnt
      FROM T2 x FULL OUTER JOIN T1 y ON (x.key = y.key) FULL OUTER JOIN T3 z ON (y.key = z.key)
      GROUP BY y.key) tmp;

set hive.optimize.correlation=true;
EXPLAIN
SELECT SUM(HASH(tmp.key)), SUM(HASH(tmp.cnt))
FROM (SELECT y.key AS key, count(1) AS cnt
      FROM T2 x FULL OUTER JOIN T1 y ON (x.key = y.key) FULL OUTER JOIN T3 z ON (y.key = z.key)
      GROUP BY y.key) tmp;

SELECT SUM(HASH(tmp.key)), SUM(HASH(tmp.cnt))
FROM (SELECT y.key AS key, count(1) AS cnt
      FROM T2 x FULL OUTER JOIN T1 y ON (x.key = y.key) FULL OUTER JOIN T3 z ON (y.key = z.key)
      GROUP BY y.key) tmp;
