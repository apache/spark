-- A test suite for IN HAVING in parent side, subquery, and both predicate subquery
-- It includes correlated cases.

--CONFIG_DIM1 spark.sql.optimizeNullAwareAntiJoin=true
--CONFIG_DIM1 spark.sql.optimizeNullAwareAntiJoin=false

--ONLY_IF spark
create temporary view t1(t1a, t1b, t1c, t1d, t1e, t1f, t1g, t1h, t1i) as values
  ('val1a', 6, 8, 10, 15.0, 20.0, 20E2, timestamp '2014-04-04 01:00:00.000', date '2014-04-04'),
  ('val1b', 8, 16, 19, 17.0, 25.0, 26E2, timestamp '2014-05-04 01:01:00.000', date '2014-05-04'),
  ('val1a', 16, 12, 21, 15.0, 20.0, 20E2, timestamp '2014-06-04 01:02:00.001', date '2014-06-04'),
  ('val1a', 16, 12, 10, 15.0, 20.0, 20E2, timestamp '2014-07-04 01:01:00.000', date '2014-07-04'),
  ('val1c', 8, 16, 19, 17.0, 25.0, 26E2, timestamp '2014-05-04 01:02:00.001', date '2014-05-05'),
  ('val1d', null, 16, 22, 17.0, 25.0, 26E2, timestamp '2014-06-04 01:01:00.000', null),
  ('val1d', null, 16, 19, 17.0, 25.0, 26E2, timestamp '2014-07-04 01:02:00.001', null),
  ('val1e', 10, null, 25, 17.0, 25.0, 26E2, timestamp '2014-08-04 01:01:00.000', date '2014-08-04'),
  ('val1e', 10, null, 19, 17.0, 25.0, 26E2, timestamp '2014-09-04 01:02:00.001', date '2014-09-04'),
  ('val1d', 10, null, 12, 17.0, 25.0, 26E2, timestamp '2015-05-04 01:01:00.000', date '2015-05-04'),
  ('val1a', 6, 8, 10, 15.0, 20.0, 20E2, timestamp '2014-04-04 01:02:00.001', date '2014-04-04'),
  ('val1e', 10, null, 19, 17.0, 25.0, 26E2, timestamp '2014-05-04 01:01:00.000', date '2014-05-04');

create temporary view t2(t2a, t2b, t2c, t2d, t2e, t2f, t2g, t2h, t2i) as values
  ('val2a', 6, 12, 14, 15.0, 20.0, 20E2, timestamp '2014-04-04 01:01:00.000', date '2014-04-04'),
  ('val1b', 10, 12, 19, 17.0, 25.0, 26E2, timestamp '2014-05-04 01:01:00.000', date '2014-05-04'),
  ('val1b', 8, 16, 119, 17.0, 25.0, 26E2, timestamp '2015-05-04 01:01:00.000', date '2015-05-04'),
  ('val1c', 12, 16, 219, 17.0, 25.0, 26E2, timestamp '2016-05-04 01:01:00.000', date '2016-05-04'),
  ('val1b', null, 16, 319, 17.0, 25.0, 26E2, timestamp '2017-05-04 01:01:00.000', null),
  ('val2e', 8, null, 419, 17.0, 25.0, 26E2, timestamp '2014-06-04 01:01:00.000', date '2014-06-04'),
  ('val1f', 19, null, 519, 17.0, 25.0, 26E2, timestamp '2014-05-04 01:01:00.000', date '2014-05-04'),
  ('val1b', 10, 12, 19, 17.0, 25.0, 26E2, timestamp '2014-06-04 01:01:00.000', date '2014-06-04'),
  ('val1b', 8, 16, 19, 17.0, 25.0, 26E2, timestamp '2014-07-04 01:01:00.000', date '2014-07-04'),
  ('val1c', 12, 16, 19, 17.0, 25.0, 26E2, timestamp '2014-08-04 01:01:00.000', date '2014-08-05'),
  ('val1e', 8, null, 19, 17.0, 25.0, 26E2, timestamp '2014-09-04 01:01:00.000', date '2014-09-04'),
  ('val1f', 19, null, 19, 17.0, 25.0, 26E2, timestamp '2014-10-04 01:01:00.000', date '2014-10-04'),
  ('val1b', null, 16, 19, 17.0, 25.0, 26E2, timestamp '2014-05-04 01:01:00.000', null);

create temporary view t3(t3a, t3b, t3c, t3d, t3e, t3f, t3g, t3h, t3i) as values
  ('val3a', 6, 12, 110, 15.0, 20.0, 20E2, timestamp '2014-04-04 01:02:00.000', date '2014-04-04'),
  ('val3a', 6, 12, 10, 15.0, 20.0, 20E2, timestamp '2014-05-04 01:02:00.000', date '2014-05-04'),
  ('val1b', 10, 12, 219, 17.0, 25.0, 26E2, timestamp '2014-05-04 01:02:00.000', date '2014-05-04'),
  ('val1b', 10, 12, 19, 17.0, 25.0, 26E2, timestamp '2014-05-04 01:02:00.000', date '2014-05-04'),
  ('val1b', 8, 16, 319, 17.0, 25.0, 26E2, timestamp '2014-06-04 01:02:00.000', date '2014-06-04'),
  ('val1b', 8, 16, 19, 17.0, 25.0, 26E2, timestamp '2014-07-04 01:02:00.000', date '2014-07-04'),
  ('val3c', 17, 16, 519, 17.0, 25.0, 26E2, timestamp '2014-08-04 01:02:00.000', date '2014-08-04'),
  ('val3c', 17, 16, 19, 17.0, 25.0, 26E2, timestamp '2014-09-04 01:02:00.000', date '2014-09-05'),
  ('val1b', null, 16, 419, 17.0, 25.0, 26E2, timestamp '2014-10-04 01:02:00.000', null),
  ('val1b', null, 16, 19, 17.0, 25.0, 26E2, timestamp '2014-11-04 01:02:00.000', null),
  ('val3b', 8, null, 719, 17.0, 25.0, 26E2, timestamp '2014-05-04 01:02:00.000', date '2014-05-04'),
  ('val3b', 8, null, 19, 17.0, 25.0, 26E2, timestamp '2015-05-04 01:02:00.000', date '2015-05-04');

-- correlated IN subquery
-- HAVING in the subquery
-- TC 01.01
SELECT t1a,
       t1b,
       t1h
FROM   t1
WHERE  t1b IN (SELECT t2b
               FROM   t2
               GROUP BY t2b
               HAVING t2b < 10);

-- TC 01.02
SELECT t1a,
       t1b,
       t1c
FROM   t1
WHERE  t1b IN (SELECT Min(t2b)
               FROM   t2
               WHERE  t1a = t2a
               GROUP  BY t2b
               HAVING t2b > 1);

-- HAVING in the parent
-- TC 01.03
SELECT t1a, t1b, t1c
FROM   t1
WHERE  t1b IN (SELECT t2b
               FROM   t2
               WHERE t1c < t2c)
GROUP BY t1a, t1b, t1c
HAVING t1b < 10;

-- TC 01.04
SELECT t1a, t1b, t1c
FROM   t1
WHERE  t1b IN (SELECT t2b
               FROM   t2
               WHERE t1c = t2c)
GROUP BY t1a, t1b, t1c
HAVING COUNT (DISTINCT t1b) < 10;

-- BOTH
-- TC 01.05
SELECT Count(DISTINCT( t1a )),
       t1b
FROM   t1
WHERE  t1c IN (SELECT t2c
               FROM   t2
               WHERE  t1a = t2a
               GROUP BY t2c
               HAVING t2c > 10)
GROUP  BY t1b
HAVING t1b >= 8;

-- TC 01.06
SELECT t1a,
       Max(t1b)
FROM   t1
WHERE  t1b > 0
GROUP  BY t1a
HAVING t1a IN (SELECT t2a
               FROM   t2
               WHERE  t2b IN (SELECT t3b
                              FROM   t3
                              WHERE  t2c = t3c)
               );

-- HAVING clause with NOT IN
-- TC 01.07
SELECT t1a,
       t1c,
       Min(t1d)
FROM   t1
WHERE  t1a NOT IN (SELECT t2a
                   FROM   t2
                   GROUP BY t2a
                   HAVING t2a > 'val2a')
GROUP BY t1a, t1c
HAVING Min(t1d) > t1c;

-- TC 01.08
SELECT t1a,
       t1b
FROM   t1
WHERE  t1d NOT IN (SELECT t2d
                   FROM   t2
                   WHERE  t1a = t2a
                   GROUP BY t2c, t2d
                   HAVING t2c > 8)
GROUP  BY t1a, t1b
HAVING t1b < 10;

-- TC 01.09
SELECT t1a,
       Max(t1b)
FROM   t1
WHERE  t1b > 0
GROUP  BY t1a
HAVING t1a NOT IN (SELECT t2a
                   FROM   t2
                   WHERE  t2b > 3);

