-- A test suite for ORDER BY in parent side, subquery, and both predicate subquery
-- It includes correlated cases.

-- Test sort operator with codegen on and off.
--CONFIG_DIM1 spark.sql.codegen.wholeStage=true
--CONFIG_DIM1 spark.sql.codegen.wholeStage=false,spark.sql.codegen.factoryMode=CODEGEN_ONLY
--CONFIG_DIM1 spark.sql.codegen.wholeStage=false,spark.sql.codegen.factoryMode=NO_CODEGEN

--CONFIG_DIM2 spark.sql.optimizeNullAwareAntiJoin=true
--CONFIG_DIM2 spark.sql.optimizeNullAwareAntiJoin=false
--ONLY_IF spark

create temporary view t1 as select * from values
  ("val1a", 6S, 8, 10L, float(15.0), 20D, 20E2BD, timestamp '2014-04-04 01:00:00.000', date '2014-04-04'),
  ("val1b", 8S, 16, 19L, float(17.0), 25D, 26E2BD, timestamp '2014-05-04 01:01:00.000', date '2014-05-04'),
  ("val1a", 16S, 12, 21L, float(15.0), 20D, 20E2BD, timestamp '2014-06-04 01:02:00.001', date '2014-06-04'),
  ("val1a", 16S, 12, 10L, float(15.0), 20D, 20E2BD, timestamp '2014-07-04 01:01:00.000', date '2014-07-04'),
  ("val1c", 8S, 16, 19L, float(17.0), 25D, 26E2BD, timestamp '2014-05-04 01:02:00.001', date '2014-05-05'),
  ("val1d", null, 16, 22L, float(17.0), 25D, 26E2BD, timestamp '2014-06-04 01:01:00.000', null),
  ("val1d", null, 16, 19L, float(17.0), 25D, 26E2BD, timestamp '2014-07-04 01:02:00.001', null),
  ("val1e", 10S, null, 25L, float(17.0), 25D, 26E2BD, timestamp '2014-08-04 01:01:00.000', date '2014-08-04'),
  ("val1e", 10S, null, 19L, float(17.0), 25D, 26E2BD, timestamp '2014-09-04 01:02:00.001', date '2014-09-04'),
  ("val1d", 10S, null, 12L, float(17.0), 25D, 26E2BD, timestamp '2015-05-04 01:01:00.000', date '2015-05-04'),
  ("val1a", 6S, 8, 10L, float(15.0), 20D, 20E2BD, timestamp '2014-04-04 01:02:00.001', date '2014-04-04'),
  ("val1e", 10S, null, 19L, float(17.0), 25D, 26E2BD, timestamp '2014-05-04 01:01:00.000', date '2014-05-04')
  as t1(t1a, t1b, t1c, t1d, t1e, t1f, t1g, t1h, t1i);

create temporary view t2 as select * from values
  ("val2a", 6S, 12, 14L, float(15), 20D, 20E2BD, timestamp '2014-04-04 01:01:00.000', date '2014-04-04'),
  ("val1b", 10S, 12, 19L, float(17), 25D, 26E2BD, timestamp '2014-05-04 01:01:00.000', date '2014-05-04'),
  ("val1b", 8S, 16, 119L, float(17), 25D, 26E2BD, timestamp '2015-05-04 01:01:00.000', date '2015-05-04'),
  ("val1c", 12S, 16, 219L, float(17), 25D, 26E2BD, timestamp '2016-05-04 01:01:00.000', date '2016-05-04'),
  ("val1b", null, 16, 319L, float(17), 25D, 26E2BD, timestamp '2017-05-04 01:01:00.000', null),
  ("val2e", 8S, null, 419L, float(17), 25D, 26E2BD, timestamp '2014-06-04 01:01:00.000', date '2014-06-04'),
  ("val1f", 19S, null, 519L, float(17), 25D, 26E2BD, timestamp '2014-05-04 01:01:00.000', date '2014-05-04'),
  ("val1b", 10S, 12, 19L, float(17), 25D, 26E2BD, timestamp '2014-06-04 01:01:00.000', date '2014-06-04'),
  ("val1b", 8S, 16, 19L, float(17), 25D, 26E2BD, timestamp '2014-07-04 01:01:00.000', date '2014-07-04'),
  ("val1c", 12S, 16, 19L, float(17), 25D, 26E2BD, timestamp '2014-08-04 01:01:00.000', date '2014-08-05'),
  ("val1e", 8S, null, 19L, float(17), 25D, 26E2BD, timestamp '2014-09-04 01:01:00.000', date '2014-09-04'),
  ("val1f", 19S, null, 19L, float(17), 25D, 26E2BD, timestamp '2014-10-04 01:01:00.000', date '2014-10-04'),
  ("val1b", null, 16, 19L, float(17), 25D, 26E2BD, timestamp '2014-05-04 01:01:00.000', null)
  as t2(t2a, t2b, t2c, t2d, t2e, t2f, t2g, t2h, t2i);

create temporary view t3 as select * from values
  ("val3a", 6S, 12, 110L, float(15), 20D, 20E2BD, timestamp '2014-04-04 01:02:00.000', date '2014-04-04'),
  ("val3a", 6S, 12, 10L, float(15), 20D, 20E2BD, timestamp '2014-05-04 01:02:00.000', date '2014-05-04'),
  ("val1b", 10S, 12, 219L, float(17), 25D, 26E2BD, timestamp '2014-05-04 01:02:00.000', date '2014-05-04'),
  ("val1b", 10S, 12, 19L, float(17), 25D, 26E2BD, timestamp '2014-05-04 01:02:00.000', date '2014-05-04'),
  ("val1b", 8S, 16, 319L, float(17), 25D, 26E2BD, timestamp '2014-06-04 01:02:00.000', date '2014-06-04'),
  ("val1b", 8S, 16, 19L, float(17), 25D, 26E2BD, timestamp '2014-07-04 01:02:00.000', date '2014-07-04'),
  ("val3c", 17S, 16, 519L, float(17), 25D, 26E2BD, timestamp '2014-08-04 01:02:00.000', date '2014-08-04'),
  ("val3c", 17S, 16, 19L, float(17), 25D, 26E2BD, timestamp '2014-09-04 01:02:00.000', date '2014-09-05'),
  ("val1b", null, 16, 419L, float(17), 25D, 26E2BD, timestamp '2014-10-04 01:02:00.000', null),
  ("val1b", null, 16, 19L, float(17), 25D, 26E2BD, timestamp '2014-11-04 01:02:00.000', null),
  ("val3b", 8S, null, 719L, float(17), 25D, 26E2BD, timestamp '2014-05-04 01:02:00.000', date '2014-05-04'),
  ("val3b", 8S, null, 19L, float(17), 25D, 26E2BD, timestamp '2015-05-04 01:02:00.000', date '2015-05-04')
  as t3(t3a, t3b, t3c, t3d, t3e, t3f, t3g, t3h, t3i);

-- correlated IN subquery
-- ORDER BY in parent side
-- TC 01.01
SELECT *
FROM   t1
WHERE  t1a IN (SELECT t2a
               FROM   t2)
ORDER  BY t1a;

-- TC 01.02
SELECT t1a
FROM   t1
WHERE  t1b IN (SELECT t2b
               FROM   t2
               WHERE  t1a = t2a)
ORDER  BY t1b DESC;

-- TC 01.03
SELECT t1a,
       t1b
FROM   t1
WHERE  t1c IN (SELECT t2c
               FROM   t2
               WHERE  t1a = t2a)
ORDER  BY 2 DESC nulls last;

-- TC 01.04
SELECT Count(DISTINCT( t1a ))
FROM   t1
WHERE  t1b IN (SELECT t2b
               FROM   t2
               WHERE  t1a = t2a)
ORDER  BY Count(DISTINCT( t1a ));

-- ORDER BY in subquery
-- TC 01.05
SELECT *
FROM   t1
WHERE  t1b IN (SELECT t2c
               FROM   t2
               ORDER  BY t2d);

-- ORDER BY in BOTH
-- TC 01.06
SELECT *
FROM   t1
WHERE  t1b IN (SELECT Min(t2b)
               FROM   t2
               WHERE  t1b = t2b
               ORDER  BY Min(t2b))
ORDER BY t1c DESC nulls first, t1a DESC, t1d DESC, t1h;

-- TC 01.07
SELECT t1a,
       t1b,
       t1h
FROM   t1
WHERE  t1c IN (SELECT t2c
               FROM   t2
               WHERE  t1a = t2a
               ORDER  BY t2b DESC nulls first)
        OR t1h IN (SELECT t2h
                   FROM   t2
                   WHERE  t1h > t2h)
ORDER  BY t1h DESC nulls last;

-- ORDER BY with NOT IN
-- TC 01.08
SELECT *
FROM   t1
WHERE  t1a NOT IN (SELECT t2a
                   FROM   t2)
ORDER  BY t1a;

-- TC 01.09
SELECT t1a,
       t1b
FROM   t1
WHERE  t1a NOT IN (SELECT t2a
                   FROM   t2
                   WHERE  t1a = t2a)
ORDER  BY t1b DESC nulls last;

-- TC 01.10
SELECT *
FROM   t1
WHERE  t1a NOT IN (SELECT t2a
                   FROM   t2
                   ORDER  BY t2a DESC nulls first)
       and t1c IN (SELECT t2c
                   FROM   t2
                   ORDER  BY t2b DESC nulls last)
ORDER  BY t1c DESC nulls last;

-- GROUP BY and ORDER BY
-- TC 01.11
SELECT *
FROM   t1
WHERE  t1b IN (SELECT Min(t2b)
               FROM   t2
               GROUP  BY t2a
               ORDER  BY t2a DESC);

-- TC 01.12
SELECT t1a,
       Count(DISTINCT( t1b ))
FROM   t1
WHERE  t1b IN (SELECT Min(t2b)
               FROM   t2
               WHERE  t1a = t2a
               GROUP  BY t2a
               ORDER  BY t2a)
GROUP  BY t1a,
          t1h
ORDER BY t1a;

-- GROUP BY and ORDER BY with NOT IN
-- TC 01.13
SELECT *
FROM   t1
WHERE  t1b NOT IN (SELECT Min(t2b)
                   FROM   t2
                   GROUP  BY t2a
                   ORDER  BY t2a);

-- TC 01.14
SELECT t1a,
       Sum(DISTINCT( t1b ))
FROM   t1
WHERE  t1b NOT IN (SELECT Min(t2b)
                   FROM   t2
                   WHERE  t1a = t2a
                   GROUP  BY t2c
                   ORDER  BY t2c DESC nulls last)
GROUP  BY t1a;

-- TC 01.15
SELECT Count(DISTINCT( t1a )),
       t1b
FROM   t1
WHERE  t1h NOT IN (SELECT t2h
                   FROM   t2
                   where t1a = t2a
                   order by t2d DESC nulls first
                   )
GROUP  BY t1a,
          t1b
ORDER  BY t1b DESC nulls last;
