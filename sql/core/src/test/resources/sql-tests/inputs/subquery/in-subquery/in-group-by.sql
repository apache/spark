-- A test suite for GROUP BY in parent side, subquery, and both predicate subquery
-- It includes correlated cases.

-- Test aggregate operator with codegen on and off.
--CONFIG_DIM1 spark.sql.codegen.wholeStage=true
--CONFIG_DIM1 spark.sql.codegen.wholeStage=false,spark.sql.codegen.factoryMode=CODEGEN_ONLY
--CONFIG_DIM1 spark.sql.codegen.wholeStage=false,spark.sql.codegen.factoryMode=NO_CODEGEN

--ONLY_IF spark
create temporary view t1 as select * from values
  ("t1a", 6S, 8, 10L, float(15.0), 20D, 20E2BD, timestamp '2014-04-04 01:00:00.000', date '2014-04-04'),
  ("t1b", 8S, 16, 19L, float(17.0), 25D, 26E2BD, timestamp '2014-05-04 01:01:00.000', date '2014-05-04'),
  ("t1a", 16S, 12, 21L, float(15.0), 20D, 20E2BD, timestamp '2014-06-04 01:02:00.001', date '2014-06-04'),
  ("t1a", 16S, 12, 10L, float(15.0), 20D, 20E2BD, timestamp '2014-07-04 01:01:00.000', date '2014-07-04'),
  ("t1c", 8S, 16, 19L, float(17.0), 25D, 26E2BD, timestamp '2014-05-04 01:02:00.001', date '2014-05-05'),
  ("t1d", null, 16, 22L, float(17.0), 25D, 26E2BD, timestamp '2014-06-04 01:01:00.000', null),
  ("t1d", null, 16, 19L, float(17.0), 25D, 26E2BD, timestamp '2014-07-04 01:02:00.001', null),
  ("t1e", 10S, null, 25L, float(17.0), 25D, 26E2BD, timestamp '2014-08-04 01:01:00.000', date '2014-08-04'),
  ("t1e", 10S, null, 19L, float(17.0), 25D, 26E2BD, timestamp '2014-09-04 01:02:00.001', date '2014-09-04'),
  ("t1d", 10S, null, 12L, float(17.0), 25D, 26E2BD, timestamp '2015-05-04 01:01:00.000', date '2015-05-04'),
  ("t1a", 6S, 8, 10L, float(15.0), 20D, 20E2BD, timestamp '2014-04-04 01:02:00.001', date '2014-04-04'),
  ("t1e", 10S, null, 19L, float(17.0), 25D, 26E2BD, timestamp '2014-05-04 01:01:00.000', date '2014-05-04')
  as t1(t1a, t1b, t1c, t1d, t1e, t1f, t1g, t1h, t1i);

create temporary view t2 as select * from values
  ("t2a", 6S, 12, 14L, float(15), 20D, 20E2BD, timestamp '2014-04-04 01:01:00.000', date '2014-04-04'),
  ("t1b", 10S, 12, 19L, float(17), 25D, 26E2BD, timestamp '2014-05-04 01:01:00.000', date '2014-05-04'),
  ("t1b", 8S, 16, 119L, float(17), 25D, 26E2BD, timestamp '2015-05-04 01:01:00.000', date '2015-05-04'),
  ("t1c", 12S, 16, 219L, float(17), 25D, 26E2BD, timestamp '2016-05-04 01:01:00.000', date '2016-05-04'),
  ("t1b", null, 16, 319L, float(17), 25D, 26E2BD, timestamp '2017-05-04 01:01:00.000', null),
  ("t2e", 8S, null, 419L, float(17), 25D, 26E2BD, timestamp '2014-06-04 01:01:00.000', date '2014-06-04'),
  ("t1f", 19S, null, 519L, float(17), 25D, 26E2BD, timestamp '2014-05-04 01:01:00.000', date '2014-05-04'),
  ("t1b", 10S, 12, 19L, float(17), 25D, 26E2BD, timestamp '2014-06-04 01:01:00.000', date '2014-06-04'),
  ("t1b", 8S, 16, 19L, float(17), 25D, 26E2BD, timestamp '2014-07-04 01:01:00.000', date '2014-07-04'),
  ("t1c", 12S, 16, 19L, float(17), 25D, 26E2BD, timestamp '2014-08-04 01:01:00.000', date '2014-08-05'),
  ("t1e", 8S, null, 19L, float(17), 25D, 26E2BD, timestamp '2014-09-04 01:01:00.000', date '2014-09-04'),
  ("t1f", 19S, null, 19L, float(17), 25D, 26E2BD, timestamp '2014-10-04 01:01:00.000', date '2014-10-04'),
  ("t1b", null, 16, 19L, float(17), 25D, 26E2BD, timestamp '2014-05-04 01:01:00.000', null)
  as t2(t2a, t2b, t2c, t2d, t2e, t2f, t2g, t2h, t2i);

create temporary view t3 as select * from values
  ("t3a", 6S, 12, 110L, float(15), 20D, 20E2BD, timestamp '2014-04-04 01:02:00.000', date '2014-04-04'),
  ("t3a", 6S, 12, 10L, float(15), 20D, 20E2BD, timestamp '2014-05-04 01:02:00.000', date '2014-05-04'),
  ("t1b", 10S, 12, 219L, float(17), 25D, 26E2BD, timestamp '2014-05-04 01:02:00.000', date '2014-05-04'),
  ("t1b", 10S, 12, 19L, float(17), 25D, 26E2BD, timestamp '2014-05-04 01:02:00.000', date '2014-05-04'),
  ("t1b", 8S, 16, 319L, float(17), 25D, 26E2BD, timestamp '2014-06-04 01:02:00.000', date '2014-06-04'),
  ("t1b", 8S, 16, 19L, float(17), 25D, 26E2BD, timestamp '2014-07-04 01:02:00.000', date '2014-07-04'),
  ("t3c", 17S, 16, 519L, float(17), 25D, 26E2BD, timestamp '2014-08-04 01:02:00.000', date '2014-08-04'),
  ("t3c", 17S, 16, 19L, float(17), 25D, 26E2BD, timestamp '2014-09-04 01:02:00.000', date '2014-09-05'),
  ("t1b", null, 16, 419L, float(17), 25D, 26E2BD, timestamp '2014-10-04 01:02:00.000', null),
  ("t1b", null, 16, 19L, float(17), 25D, 26E2BD, timestamp '2014-11-04 01:02:00.000', null),
  ("t3b", 8S, null, 719L, float(17), 25D, 26E2BD, timestamp '2014-05-04 01:02:00.000', date '2014-05-04'),
  ("t3b", 8S, null, 19L, float(17), 25D, 26E2BD, timestamp '2015-05-04 01:02:00.000', date '2015-05-04')
  as t3(t3a, t3b, t3c, t3d, t3e, t3f, t3g, t3h, t3i);

-- correlated IN subquery
-- GROUP BY in parent side
-- TC 01.01
SELECT t1a,
       Avg(t1b)
FROM   t1
WHERE  t1a IN (SELECT t2a
               FROM   t2)
GROUP  BY t1a;

-- TC 01.02
SELECT t1a,
       Max(t1b)
FROM   t1
WHERE  t1b IN (SELECT t2b
               FROM   t2
               WHERE  t1a = t2a)
GROUP  BY t1a,
          t1d;

-- TC 01.03
SELECT t1a,
       t1b
FROM   t1
WHERE  t1c IN (SELECT t2c
               FROM   t2
               WHERE  t1a = t2a)
GROUP  BY t1a,
          t1b;

-- TC 01.04
SELECT t1a,
       Sum(DISTINCT( t1b ))
FROM   t1
WHERE  t1c IN (SELECT t2c
               FROM   t2
               WHERE  t1a = t2a)
        OR t1c IN (SELECT t3c
                   FROM   t3
                   WHERE  t1a = t3a)
GROUP  BY t1a,
          t1c;

-- TC 01.05
SELECT t1a,
       Sum(DISTINCT( t1b ))
FROM   t1
WHERE  t1c IN (SELECT t2c
               FROM   t2
               WHERE  t1a = t2a)
       AND t1c IN (SELECT t3c
                   FROM   t3
                   WHERE  t1a = t3a)
GROUP  BY t1a,
          t1c;

-- TC 01.06
SELECT t1a,
       Count(DISTINCT( t1b ))
FROM   t1
WHERE  t1c IN (SELECT t2c
               FROM   t2
               WHERE  t1a = t2a)
GROUP  BY t1a,
          t1c
HAVING t1a = "t1b";

-- GROUP BY in subquery
-- TC 01.07
SELECT *
FROM   t1
WHERE  t1b IN (SELECT Max(t2b)
               FROM   t2
               GROUP  BY t2a);

-- TC 01.08
SELECT *
FROM   (SELECT t2a,
               t2b
        FROM   t2
        WHERE  t2a IN (SELECT t1a
                       FROM   t1
                       WHERE  t1b = t2b)
        GROUP  BY t2a,
                  t2b) t2;

-- TC 01.09
SELECT Count(DISTINCT * )
FROM   t1
WHERE  t1b IN (SELECT Min(t2b)
               FROM   t2
               WHERE  t1a = t2a
                      AND t1c = t2c
               GROUP  BY t2a);

-- TC 01.10
SELECT t1a,
       t1b
FROM   t1
WHERE  t1c IN (SELECT Max(t2c)
               FROM   t2
               WHERE  t1a = t2a
               GROUP  BY t2a,
                         t2c
               HAVING t2c > 8);

-- TC 01.11
SELECT t1a,
       t1b
FROM   t1
WHERE  t1c IN (SELECT t2c
               FROM   t2
               WHERE  t2a IN (SELECT Min(t3a)
                              FROM   t3
                              WHERE  t3a = t2a
                              GROUP  BY t3b)
               GROUP  BY t2c);

-- GROUP BY in both
-- TC 01.12
SELECT t1a,
       Min(t1b)
FROM   t1
WHERE  t1c IN (SELECT Min(t2c)
               FROM   t2
               WHERE  t2b = t1b
               GROUP  BY t2a)
GROUP  BY t1a;

-- TC 01.13
SELECT t1a,
       Min(t1b)
FROM   t1
WHERE  t1c IN (SELECT Min(t2c)
               FROM   t2
               WHERE  t2b IN (SELECT Min(t3b)
                              FROM   t3
                              WHERE  t2a = t3a
                              GROUP  BY t3a)
               GROUP  BY t2c)
GROUP  BY t1a,
          t1d;

-- TC 01.14
SELECT t1a,
       Min(t1b)
FROM   t1
WHERE  t1c IN (SELECT Min(t2c)
               FROM   t2
               WHERE  t2b = t1b
               GROUP  BY t2a)
       AND t1d IN (SELECT t3d
                   FROM   t3
                   WHERE  t1c = t3c
                   GROUP  BY t3d)
GROUP  BY t1a;

-- TC 01.15
SELECT t1a,
       Min(t1b)
FROM   t1
WHERE  t1c IN (SELECT Min(t2c)
               FROM   t2
               WHERE  t2b = t1b
               GROUP  BY t2a)
        OR t1d IN (SELECT t3d
                   FROM   t3
                   WHERE  t1c = t3c
                   GROUP  BY t3d)
GROUP  BY t1a;

-- TC 01.16
SELECT t1a,
       Min(t1b)
FROM   t1
WHERE  t1c IN (SELECT Min(t2c)
               FROM   t2
               WHERE  t2b = t1b
               GROUP  BY t2a
               HAVING t2a > t1a)
        OR t1d IN (SELECT t3d
                   FROM   t3
                   WHERE  t1c = t3c
                   GROUP  BY t3d
                   HAVING t3d = t1d)
GROUP  BY t1a
HAVING Min(t1b) IS NOT NULL;

-- Window functions are not supported in IN subqueries yet
select t1a
from t1
where t1f IN (SELECT RANK() OVER (partition by t3c  order by t2b) as s
                             FROM t2, t3 where t2.t2c = t3.t3c and t2.t2a < t1.t1a);

-- Plain in-subquery with a top-level aggregation
SELECT
  t1.t1a,
  t1.t1a IN (SELECT t2a FROM t2) as v1
FROM t1
GROUP BY t1.t1a ORDER BY t1.t1a;

-- Aggregate function over expression with subquery, without explicit GROUP BY, with NOT IN
SELECT
  count(cast(t1.t1a IN (SELECT t2a FROM t2) as INT)),
  sum(cast(t1.t1b NOT IN (SELECT t2b FROM t2) as INT))
FROM t1;

-- Derived table from subquery
SELECT
    agg_results.t1a,
    COUNT(*)
    FROM (SELECT t1.t1a FROM t1 WHERE t1.t1a IN (SELECT t2a FROM t2)) AS agg_results
GROUP BY agg_results.t1a ORDER BY agg_results.t1a;

-- CASE statement with an in-subquery and aggregation
SELECT
  t1.t1a,
  CASE
    WHEN t1.t1a IN (SELECT t2a FROM t2) THEN 10
    ELSE -10
  END AS v1
FROM t1
GROUP BY t1.t1a
ORDER BY t1.t1a;

-- CASE statement with an in-subquery inside an agg function
SELECT
  t1.t1c,
  -- sums over t1.t1c
  SUM(CASE
    WHEN t1.t1c IN (SELECT t2c FROM t2) THEN 10
    ELSE -10
  END) AS v1,
  -- sums over t1.t1d
  SUM(CASE
      WHEN t1.t1d IN (SELECT t2c FROM t2) THEN 10
      ELSE -10
    END) AS v2,
  -- no agg function, uses t1.t1c
  t1.t1c + 10 IN (SELECT t2c + 2 FROM t2) AS v3,
  count(t1.t1c) as ct,
  count(t1.t1d)
FROM t1
GROUP BY t1.t1c
ORDER BY t1.t1c;

-- CASE statement with an in-subquery inside an agg function, without group-by
SELECT
  SUM(CASE
    WHEN t1.t1c IN (SELECT t2c FROM t2) THEN 10
    ELSE -10
  END) AS v1,
  count(t1.t1c) as ct
FROM t1;

-- Group-by statement contains an in-subquery, and there's an additional exists in select clause.
SELECT
    cast(t1a in (select t2a from t2) as int) + 1 as groupExpr,
    sum(cast(t1a in (select t2a from t2) as int) + 1) as aggExpr,
    cast(t1a in (select t2a from t2) as int) + 1 + cast(exists (select t2a from t2) as int)
        as complexExpr
FROM t1
GROUP BY
    cast(t1a in (select t2a from t2) as int) + 1;
