set hive.auto.convert.join=false;
set hive.optimize.correlation=false;
-- This query has a GroupByOperator folling JoinOperator and they share the same keys.
-- When Correlation Optimizer is turned off, three MR jobs will be generated.
-- When Correlation Optimizer is turned on, two MR jobs will be generated
-- and JoinOperator (on the column of key) and GroupByOperator (also on the column
-- of key) will be executed in the first MR job.
EXPLAIN
SELECT SUM(HASH(tmp.key)), SUM(HASH(tmp.cnt))
FROM (SELECT x.key AS key, count(1) AS cnt
      FROM src1 x JOIN src y ON (x.key = y.key)
      GROUP BY x.key) tmp;

SELECT SUM(HASH(tmp.key)), SUM(HASH(tmp.cnt))
FROM (SELECT x.key AS key, count(1) AS cnt
      FROM src1 x JOIN src y ON (x.key = y.key)
      GROUP BY x.key) tmp;

set hive.optimize.correlation=true;
EXPLAIN
SELECT SUM(HASH(tmp.key)), SUM(HASH(tmp.cnt))
FROM (SELECT x.key AS key, count(1) AS cnt
      FROM src1 x JOIN src y ON (x.key = y.key)
      GROUP BY x.key) tmp;

SELECT SUM(HASH(tmp.key)), SUM(HASH(tmp.cnt))
FROM (SELECT x.key AS key, count(1) AS cnt
      FROM src1 x JOIN src y ON (x.key = y.key)
      GROUP BY x.key) tmp;

set hive.auto.convert.join=true;
set hive.optimize.correlation=true;
-- Enable hive.auto.convert.join.
-- Correlation Optimizer will detect that the join will be converted to a Map-join,
-- so it will not try to optimize this query.
-- We should generate 1 MR job for subquery tmp.
EXPLAIN
SELECT SUM(HASH(tmp.key)), SUM(HASH(tmp.cnt))
FROM (SELECT x.key AS key, count(1) AS cnt
      FROM src1 x JOIN src y ON (x.key = y.key)
      GROUP BY x.key) tmp;

SELECT SUM(HASH(tmp.key)), SUM(HASH(tmp.cnt))
FROM (SELECT x.key AS key, count(1) AS cnt
      FROM src1 x JOIN src y ON (x.key = y.key)
      GROUP BY x.key) tmp;

set hive.auto.convert.join=false;
set hive.optimize.correlation=false;
-- If the key of a GroupByOperator is the left table's key in
-- a Left Semi Join, these two operators will be executed in
-- the same MR job when Correlation Optimizer is enabled.
EXPLAIN
SELECT SUM(HASH(tmp.key)), SUM(HASH(tmp.cnt))
FROM (SELECT x.key AS key, count(1) AS cnt
      FROM src1 x LEFT SEMI JOIN src y ON (x.key = y.key)
      GROUP BY x.key) tmp;

SELECT SUM(HASH(tmp.key)), SUM(HASH(tmp.cnt))
FROM (SELECT x.key AS key, count(1) AS cnt
      FROM src1 x LEFT SEMI JOIN src y ON (x.key = y.key)
      GROUP BY x.key) tmp;

set hive.optimize.correlation=true;
EXPLAIN
SELECT SUM(HASH(tmp.key)), SUM(HASH(tmp.cnt))
FROM (SELECT x.key AS key, count(1) AS cnt
      FROM src1 x LEFT SEMI JOIN src y ON (x.key = y.key)
      GROUP BY x.key) tmp;

SELECT SUM(HASH(tmp.key)), SUM(HASH(tmp.cnt))
FROM (SELECT x.key AS key, count(1) AS cnt
      FROM src1 x LEFT SEMI JOIN src y ON (x.key = y.key)
      GROUP BY x.key) tmp;
      
set hive.auto.convert.join=false;
set hive.optimize.correlation=false;
-- If the key of a GroupByOperator is the left table's key in
-- a Left Outer Join, these two operators will be executed in
-- the same MR job when Correlation Optimizer is enabled.
EXPLAIN
SELECT SUM(HASH(tmp.key)), SUM(HASH(tmp.cnt))
FROM (SELECT x.key AS key, count(1) AS cnt
      FROM src1 x LEFT OUTER JOIN src y ON (x.key = y.key)
      GROUP BY x.key) tmp;

SELECT SUM(HASH(tmp.key)), SUM(HASH(tmp.cnt))
FROM (SELECT x.key AS key, count(1) AS cnt
      FROM src1 x LEFT OUTER JOIN src y ON (x.key = y.key)
      GROUP BY x.key) tmp;

set hive.optimize.correlation=true;
EXPLAIN
SELECT SUM(HASH(tmp.key)), SUM(HASH(tmp.cnt))
FROM (SELECT x.key AS key, count(1) AS cnt
      FROM src1 x LEFT OUTER JOIN src y ON (x.key = y.key)
      GROUP BY x.key) tmp;

SELECT SUM(HASH(tmp.key)), SUM(HASH(tmp.cnt))
FROM (SELECT x.key AS key, count(1) AS cnt
      FROM src1 x LEFT OUTER JOIN src y ON (x.key = y.key)
      GROUP BY x.key) tmp;

      
set hive.optimize.correlation=false;
-- If the key of a GroupByOperator is the right table's key in
-- a Left Outer Join, we cannot use a single MR to execute these two
-- operators because those keys with a null value are not grouped.
EXPLAIN
SELECT SUM(HASH(tmp.key)), SUM(HASH(tmp.cnt))
FROM (SELECT y.key AS key, count(1) AS cnt
      FROM src1 x LEFT OUTER JOIN src y ON (x.key = y.key)
      GROUP BY y.key) tmp;

SELECT SUM(HASH(tmp.key)), SUM(HASH(tmp.cnt))
FROM (SELECT y.key AS key, count(1) AS cnt
      FROM src1 x LEFT OUTER JOIN src y ON (x.key = y.key)
      GROUP BY y.key) tmp;

set hive.optimize.correlation=true;
EXPLAIN
SELECT SUM(HASH(tmp.key)), SUM(HASH(tmp.cnt))
FROM (SELECT y.key AS key, count(1) AS cnt
      FROM src1 x LEFT OUTER JOIN src y ON (x.key = y.key)
      GROUP BY y.key) tmp;

SELECT SUM(HASH(tmp.key)), SUM(HASH(tmp.cnt))
FROM (SELECT y.key AS key, count(1) AS cnt
      FROM src1 x LEFT OUTER JOIN src y ON (x.key = y.key)
      GROUP BY y.key) tmp;

set hive.optimize.correlation=false;
-- If a column of the key of a GroupByOperator is the right table's key in
-- a Left Outer Join, we cannot use a single MR to execute these two
-- operators because those keys with a null value are not grouped.
EXPLAIN
SELECT x.key, y.value, count(1) AS cnt
FROM src1 x LEFT OUTER JOIN src y ON (x.key = y.key AND x.value = y.value)
GROUP BY x.key, y.value;

SELECT x.key, y.value, count(1) AS cnt
FROM src1 x LEFT OUTER JOIN src y ON (x.key = y.key AND x.value = y.value)
GROUP BY x.key, y.value;

set hive.optimize.correlation=true;
EXPLAIN
SELECT x.key, y.value, count(1) AS cnt
FROM src1 x LEFT OUTER JOIN src y ON (x.key = y.key AND x.value = y.value)
GROUP BY x.key, y.value;

SELECT x.key, y.value, count(1) AS cnt
FROM src1 x LEFT OUTER JOIN src y ON (x.key = y.key AND x.value = y.value)
GROUP BY x.key, y.value;

set hive.optimize.correlation=false;
-- If the key of a GroupByOperator is the right table's key in
-- a Right Outer Join, these two operators will be executed in
-- the same MR job when Correlation Optimizer is enabled.
EXPLAIN
SELECT SUM(HASH(tmp.key)), SUM(HASH(tmp.cnt))
FROM (SELECT y.key AS key, count(1) AS cnt
      FROM src1 x RIGHT OUTER JOIN src y ON (x.key = y.key)
      GROUP BY y.key) tmp;

SELECT SUM(HASH(tmp.key)), SUM(HASH(tmp.cnt))
FROM (SELECT y.key AS key, count(1) AS cnt
      FROM src1 x RIGHT OUTER JOIN src y ON (x.key = y.key)
      GROUP BY y.key) tmp;

set hive.optimize.correlation=true;
EXPLAIN
SELECT SUM(HASH(tmp.key)), SUM(HASH(tmp.cnt))
FROM (SELECT y.key AS key, count(1) AS cnt
      FROM src1 x RIGHT OUTER JOIN src y ON (x.key = y.key)
      GROUP BY y.key) tmp;

SELECT SUM(HASH(tmp.key)), SUM(HASH(tmp.cnt))
FROM (SELECT y.key AS key, count(1) AS cnt
      FROM src1 x RIGHT OUTER JOIN src y ON (x.key = y.key)
      GROUP BY y.key) tmp;


set hive.optimize.correlation=false;
-- If the key of a GroupByOperator is the left table's key in
-- a Right Outer Join, we cannot use a single MR to execute these two 
-- operators because those keys with a null value are not grouped.
EXPLAIN
SELECT SUM(HASH(tmp.key)), SUM(HASH(tmp.cnt))
FROM (SELECT x.key AS key, count(1) AS cnt
      FROM src1 x RIGHT OUTER JOIN src y ON (x.key = y.key)
      GROUP BY x.key) tmp;

SELECT SUM(HASH(tmp.key)), SUM(HASH(tmp.cnt))
FROM (SELECT x.key AS key, count(1) AS cnt
      FROM src1 x RIGHT OUTER JOIN src y ON (x.key = y.key)
      GROUP BY x.key) tmp;

set hive.optimize.correlation=true;
EXPLAIN
SELECT SUM(HASH(tmp.key)), SUM(HASH(tmp.cnt))
FROM (SELECT x.key AS key, count(1) AS cnt
      FROM src1 x RIGHT OUTER JOIN src y ON (x.key = y.key)
      GROUP BY x.key) tmp;

SELECT SUM(HASH(tmp.key)), SUM(HASH(tmp.cnt))
FROM (SELECT x.key AS key, count(1) AS cnt
      FROM src1 x RIGHT OUTER JOIN src y ON (x.key = y.key)
      GROUP BY x.key) tmp;

set hive.optimize.correlation=false;
-- This query has a Full Outer Join followed by a GroupByOperator and
-- they share the same key. Because those keys with a null value are not grouped
-- in the output of the Full Outer Join, we cannot use a single MR to execute
-- these two operators.
EXPLAIN
SELECT SUM(HASH(tmp.key)), SUM(HASH(tmp.cnt))
FROM (SELECT x.key AS key, count(1) AS cnt
      FROM src1 x FULL OUTER JOIN src y ON (x.key = y.key)
      GROUP BY x.key) tmp;

SELECT SUM(HASH(tmp.key)), SUM(HASH(tmp.cnt))
FROM (SELECT x.key AS key, count(1) AS cnt
      FROM src1 x FULL OUTER JOIN src y ON (x.key = y.key)
      GROUP BY x.key) tmp;

set hive.optimize.correlation=true;
EXPLAIN
SELECT SUM(HASH(tmp.key)), SUM(HASH(tmp.cnt))
FROM (SELECT x.key AS key, count(1) AS cnt
      FROM src1 x FULL OUTER JOIN src y ON (x.key = y.key)
      GROUP BY x.key) tmp;

SELECT SUM(HASH(tmp.key)), SUM(HASH(tmp.cnt))
FROM (SELECT x.key AS key, count(1) AS cnt
      FROM src1 x FULL OUTER JOIN src y ON (x.key = y.key)
      GROUP BY x.key) tmp;

set hive.auto.convert.join=false;
set hive.optimize.correlation=false;
-- Currently, we only handle exactly same keys, this query will not be optimized
EXPLAIN
SELECT SUM(HASH(tmp.key)), SUM(HASH(tmp.value)), SUM(HASH(tmp.cnt)) 
FROM (SELECT x.key AS key, x.value AS value, count(1) AS cnt
      FROM src1 x JOIN src y ON (x.key = y.key)
      GROUP BY x.key, x.value) tmp;
      
SELECT SUM(HASH(tmp.key)), SUM(HASH(tmp.value)), SUM(HASH(tmp.cnt)) 
FROM (SELECT x.key AS key, x.value AS value, count(1) AS cnt
      FROM src1 x JOIN src y ON (x.key = y.key)
      GROUP BY x.key, x.value) tmp;

set hive.optimize.correlation=true;
EXPLAIN
SELECT SUM(HASH(tmp.key)), SUM(HASH(tmp.value)), SUM(HASH(tmp.cnt)) 
FROM (SELECT x.key AS key, x.value AS value, count(1) AS cnt
      FROM src1 x JOIN src y ON (x.key = y.key)
      GROUP BY x.key, x.value) tmp;

SELECT SUM(HASH(tmp.key)), SUM(HASH(tmp.value)), SUM(HASH(tmp.cnt)) 
FROM (SELECT x.key AS key, x.value AS value, count(1) AS cnt
      FROM src1 x JOIN src y ON (x.key = y.key)
      GROUP BY x.key, x.value) tmp;

set hive.optimize.correlation=false;
-- Currently, we only handle exactly same keys, this query will not be optimized
EXPLAIN
SELECT SUM(HASH(tmp.key)), SUM(HASH(tmp.cnt))
FROM (SELECT x.key AS key, count(1) AS cnt
      FROM src1 x JOIN src y ON (x.key = y.key AND x.value = y.value)
      GROUP BY x.key) tmp;

SELECT SUM(HASH(tmp.key)), SUM(HASH(tmp.cnt))
FROM (SELECT x.key AS key, count(1) AS cnt
      FROM src1 x JOIN src y ON (x.key = y.key AND x.value = y.value)
      GROUP BY x.key) tmp;

set hive.optimize.correlation=true;
EXPLAIN
SELECT SUM(HASH(tmp.key)), SUM(HASH(tmp.cnt))
FROM (SELECT x.key AS key, count(1) AS cnt
      FROM src1 x JOIN src y ON (x.key = y.key AND x.value = y.value)
      GROUP BY x.key) tmp;

SELECT SUM(HASH(tmp.key)), SUM(HASH(tmp.cnt))
FROM (SELECT x.key AS key, count(1) AS cnt
      FROM src1 x JOIN src y ON (x.key = y.key AND x.value = y.value)
      GROUP BY x.key) tmp;
