set hive.auto.convert.join=false;
set hive.optimize.correlation=false;
-- In this query, subquery a and b both have a GroupByOperator and the a and b will be
-- joined. The key of JoinOperator is the same with both keys of GroupByOperators in subquery
-- a and b. When Correlation Optimizer is turned off, we have four MR jobs.
-- When Correlation Optimizer is turned on, 2 MR jobs will be generated.
-- The first job will evaluate subquery tmp (including subquery a, b, and the JoinOperator on a
-- and b).
EXPLAIN
SELECT SUM(HASH(key1)), SUM(HASH(cnt1)), SUM(HASH(key2)), SUM(HASH(cnt2))
FROM (SELECT a.key AS key1, a.cnt AS cnt1, b.key AS key2, b.cnt AS cnt2
      FROM (SELECT x.key as key, count(x.value) AS cnt FROM src x group by x.key) a
      JOIN (SELECT y.key as key, count(y.value) AS cnt FROM src1 y group by y.key) b
      ON (a.key = b.key)) tmp;

SELECT SUM(HASH(key1)), SUM(HASH(cnt1)), SUM(HASH(key2)), SUM(HASH(cnt2))
FROM (SELECT a.key AS key1, a.cnt AS cnt1, b.key AS key2, b.cnt AS cnt2
      FROM (SELECT x.key as key, count(x.value) AS cnt FROM src x group by x.key) a
      JOIN (SELECT y.key as key, count(y.value) AS cnt FROM src1 y group by y.key) b
      ON (a.key = b.key)) tmp;

set hive.optimize.correlation=true;
EXPLAIN
SELECT SUM(HASH(key1)), SUM(HASH(cnt1)), SUM(HASH(key2)), SUM(HASH(cnt2))
FROM (SELECT a.key AS key1, a.cnt AS cnt1, b.key AS key2, b.cnt AS cnt2
      FROM (SELECT x.key as key, count(x.value) AS cnt FROM src x group by x.key) a
      JOIN (SELECT y.key as key, count(y.value) AS cnt FROM src1 y group by y.key) b
      ON (a.key = b.key)) tmp;

SELECT SUM(HASH(key1)), SUM(HASH(cnt1)), SUM(HASH(key2)), SUM(HASH(cnt2))
FROM (SELECT a.key AS key1, a.cnt AS cnt1, b.key AS key2, b.cnt AS cnt2
      FROM (SELECT x.key as key, count(x.value) AS cnt FROM src x group by x.key) a
      JOIN (SELECT y.key as key, count(y.value) AS cnt FROM src1 y group by y.key) b
      ON (a.key = b.key)) tmp;

set hive.optimize.correlation=false;
-- Left Outer Join should be handled.
EXPLAIN
SELECT SUM(HASH(key1)), SUM(HASH(cnt1)), SUM(HASH(key2)), SUM(HASH(cnt2))
FROM (SELECT a.key AS key1, a.cnt AS cnt1, b.key AS key2, b.cnt AS cnt2
      FROM (SELECT x.key as key, count(x.value) AS cnt FROM src x group by x.key) a
      LEFT OUTER JOIN (SELECT y.key as key, count(y.value) AS cnt FROM src1 y group by y.key) b
      ON (a.key = b.key)) tmp;

SELECT SUM(HASH(key1)), SUM(HASH(cnt1)), SUM(HASH(key2)), SUM(HASH(cnt2))
FROM (SELECT a.key AS key1, a.cnt AS cnt1, b.key AS key2, b.cnt AS cnt2
      FROM (SELECT x.key as key, count(x.value) AS cnt FROM src x group by x.key) a
      LEFT OUTER JOIN (SELECT y.key as key, count(y.value) AS cnt FROM src1 y group by y.key) b
      ON (a.key = b.key)) tmp;

set hive.optimize.correlation=true;
EXPLAIN
SELECT SUM(HASH(key1)), SUM(HASH(cnt1)), SUM(HASH(key2)), SUM(HASH(cnt2))
FROM (SELECT a.key AS key1, a.cnt AS cnt1, b.key AS key2, b.cnt AS cnt2
      FROM (SELECT x.key as key, count(x.value) AS cnt FROM src x group by x.key) a
      LEFT OUTER JOIN (SELECT y.key as key, count(y.value) AS cnt FROM src1 y group by y.key) b
      ON (a.key = b.key)) tmp;

SELECT SUM(HASH(key1)), SUM(HASH(cnt1)), SUM(HASH(key2)), SUM(HASH(cnt2))
FROM (SELECT a.key AS key1, a.cnt AS cnt1, b.key AS key2, b.cnt AS cnt2
      FROM (SELECT x.key as key, count(x.value) AS cnt FROM src x group by x.key) a
      LEFT OUTER JOIN (SELECT y.key as key, count(y.value) AS cnt FROM src1 y group by y.key) b
      ON (a.key = b.key)) tmp;

set hive.optimize.correlation=false;
-- Right Outer Join should be handled.
EXPLAIN
SELECT SUM(HASH(key1)), SUM(HASH(cnt1)), SUM(HASH(key2)), SUM(HASH(cnt2))
FROM (SELECT a.key AS key1, a.cnt AS cnt1, b.key AS key2, b.cnt AS cnt2
      FROM (SELECT x.key as key, count(x.value) AS cnt FROM src x group by x.key) a
      RIGHT OUTER JOIN (SELECT y.key as key, count(y.value) AS cnt FROM src1 y group by y.key) b
      ON (a.key = b.key)) tmp;

SELECT SUM(HASH(key1)), SUM(HASH(cnt1)), SUM(HASH(key2)), SUM(HASH(cnt2))
FROM (SELECT a.key AS key1, a.cnt AS cnt1, b.key AS key2, b.cnt AS cnt2
      FROM (SELECT x.key as key, count(x.value) AS cnt FROM src x group by x.key) a
      RIGHT OUTER JOIN (SELECT y.key as key, count(y.value) AS cnt FROM src1 y group by y.key) b
      ON (a.key = b.key)) tmp;

set hive.optimize.correlation=true;
EXPLAIN
SELECT SUM(HASH(key1)), SUM(HASH(cnt1)), SUM(HASH(key2)), SUM(HASH(cnt2))
FROM (SELECT a.key AS key1, a.cnt AS cnt1, b.key AS key2, b.cnt AS cnt2
      FROM (SELECT x.key as key, count(x.value) AS cnt FROM src x group by x.key) a
      RIGHT OUTER JOIN (SELECT y.key as key, count(y.value) AS cnt FROM src1 y group by y.key) b
      ON (a.key = b.key)) tmp;

SELECT SUM(HASH(key1)), SUM(HASH(cnt1)), SUM(HASH(key2)), SUM(HASH(cnt2))
FROM (SELECT a.key AS key1, a.cnt AS cnt1, b.key AS key2, b.cnt AS cnt2
      FROM (SELECT x.key as key, count(x.value) AS cnt FROM src x group by x.key) a
      RIGHT OUTER JOIN (SELECT y.key as key, count(y.value) AS cnt FROM src1 y group by y.key) b
      ON (a.key = b.key)) tmp;

set hive.optimize.correlation=false;
-- Full Outer Join should be handled.
EXPLAIN
SELECT SUM(HASH(key1)), SUM(HASH(cnt1)), SUM(HASH(key2)), SUM(HASH(cnt2))
FROM (SELECT a.key AS key1, a.cnt AS cnt1, b.key AS key2, b.cnt AS cnt2
      FROM (SELECT x.key as key, count(x.value) AS cnt FROM src x group by x.key) a
      FULL OUTER JOIN (SELECT y.key as key, count(y.value) AS cnt FROM src1 y group by y.key) b
      ON (a.key = b.key)) tmp;

SELECT SUM(HASH(key1)), SUM(HASH(cnt1)), SUM(HASH(key2)), SUM(HASH(cnt2))
FROM (SELECT a.key AS key1, a.cnt AS cnt1, b.key AS key2, b.cnt AS cnt2
      FROM (SELECT x.key as key, count(x.value) AS cnt FROM src x group by x.key) a
      FULL OUTER JOIN (SELECT y.key as key, count(y.value) AS cnt FROM src1 y group by y.key) b
      ON (a.key = b.key)) tmp;

set hive.optimize.correlation=true;
EXPLAIN
SELECT SUM(HASH(key1)), SUM(HASH(cnt1)), SUM(HASH(key2)), SUM(HASH(cnt2))
FROM (SELECT a.key AS key1, a.cnt AS cnt1, b.key AS key2, b.cnt AS cnt2
      FROM (SELECT x.key as key, count(x.value) AS cnt FROM src x group by x.key) a
      FULL OUTER JOIN (SELECT y.key as key, count(y.value) AS cnt FROM src1 y group by y.key) b
      ON (a.key = b.key)) tmp;

SELECT SUM(HASH(key1)), SUM(HASH(cnt1)), SUM(HASH(key2)), SUM(HASH(cnt2))
FROM (SELECT a.key AS key1, a.cnt AS cnt1, b.key AS key2, b.cnt AS cnt2
      FROM (SELECT x.key as key, count(x.value) AS cnt FROM src x group by x.key) a
      FULL OUTER JOIN (SELECT y.key as key, count(y.value) AS cnt FROM src1 y group by y.key) b
      ON (a.key = b.key)) tmp;

set hive.optimize.correlation=false;
EXPLAIN
SELECT SUM(HASH(tmp.key)), SUM(HASH(tmp.cnt))
FROM (SELECT a.key AS key, count(1) AS cnt
      FROM (SELECT x.key as key, count(x.value) AS cnt FROM src x group by x.key) a
      FULL OUTER JOIN (SELECT y.key as key, count(y.value) AS cnt FROM src1 y group by y.key) b
      ON (a.key = b.key)
      GROUP BY a.key) tmp;

SELECT SUM(HASH(tmp.key)), SUM(HASH(tmp.cnt))
FROM (SELECT a.key AS key, count(1) AS cnt
      FROM (SELECT x.key as key, count(x.value) AS cnt FROM src x group by x.key) a
      FULL OUTER JOIN (SELECT y.key as key, count(y.value) AS cnt FROM src1 y group by y.key) b
      ON (a.key = b.key)
      GROUP BY a.key) tmp;

set hive.optimize.correlation=true;
-- After FULL OUTER JOIN, keys with null values are not grouped, right now,
-- we have to generate 2 MR jobs for tmp, 1 MR job for a join b and another for the
-- GroupByOperator on key.
EXPLAIN
SELECT SUM(HASH(tmp.key)), SUM(HASH(tmp.cnt))
FROM (SELECT a.key AS key, count(1) AS cnt
      FROM (SELECT x.key as key, count(x.value) AS cnt FROM src x group by x.key) a
      FULL OUTER JOIN (SELECT y.key as key, count(y.value) AS cnt FROM src1 y group by y.key) b
      ON (a.key = b.key)
      GROUP BY a.key) tmp;

SELECT SUM(HASH(tmp.key)), SUM(HASH(tmp.cnt))
FROM (SELECT a.key AS key, count(1) AS cnt
      FROM (SELECT x.key as key, count(x.value) AS cnt FROM src x group by x.key) a
      FULL OUTER JOIN (SELECT y.key as key, count(y.value) AS cnt FROM src1 y group by y.key) b
      ON (a.key = b.key)
      GROUP BY a.key) tmp;

set hive.optimize.correlation=false;
-- When Correlation Optimizer is turned off, we need 4 MR jobs.
-- When Correlation Optimizer is turned on, the subquery of tmp will be evaluated in
-- a single MR job (including the subquery a, the subquery b, and a join b). So, we
-- will have 2 MR jobs.
EXPLAIN
SELECT SUM(HASH(key1)), SUM(HASH(cnt1)), SUM(HASH(key2)), SUM(HASH(cnt2))
FROM (SELECT a.key AS key1, a.val AS cnt1, b.key AS key2, b.cnt AS cnt2
      FROM (SELECT x.key AS key, x.value AS val FROM src1 x JOIN src y ON (x.key = y.key)) a
      JOIN (SELECT z.key AS key, count(z.value) AS cnt FROM src1 z group by z.key) b
      ON (a.key = b.key)) tmp;

SELECT SUM(HASH(key1)), SUM(HASH(cnt1)), SUM(HASH(key2)), SUM(HASH(cnt2))
FROM (SELECT a.key AS key1, a.val AS cnt1, b.key AS key2, b.cnt AS cnt2
      FROM (SELECT x.key AS key, x.value AS val FROM src1 x JOIN src y ON (x.key = y.key)) a
      JOIN (SELECT z.key AS key, count(z.value) AS cnt FROM src1 z group by z.key) b
      ON (a.key = b.key)) tmp;

set hive.optimize.correlation=true;
EXPLAIN
SELECT SUM(HASH(key1)), SUM(HASH(cnt1)), SUM(HASH(key2)), SUM(HASH(cnt2))
FROM (SELECT a.key AS key1, a.val AS cnt1, b.key AS key2, b.cnt AS cnt2
      FROM (SELECT x.key AS key, x.value AS val FROM src1 x JOIN src y ON (x.key = y.key)) a
      JOIN (SELECT z.key AS key, count(z.value) AS cnt FROM src1 z group by z.key) b
      ON (a.key = b.key)) tmp;

SELECT SUM(HASH(key1)), SUM(HASH(cnt1)), SUM(HASH(key2)), SUM(HASH(cnt2))
FROM (SELECT a.key AS key1, a.val AS cnt1, b.key AS key2, b.cnt AS cnt2
      FROM (SELECT x.key AS key, x.value AS val FROM src1 x JOIN src y ON (x.key = y.key)) a
      JOIN (SELECT z.key AS key, count(z.value) AS cnt FROM src1 z group by z.key) b
      ON (a.key = b.key)) tmp;
