-- A test suite for simple IN predicate subquery
-- It includes correlated cases.

--CONFIG_DIM1 spark.sql.optimizeNullAwareAntiJoin=true
--CONFIG_DIM1 spark.sql.optimizeNullAwareAntiJoin=false

create temporary view t1(t1a, t1b, t1c, t1d, t1e, t1f, t1g, t1h, t1i) as values
  ('t1a', 6, 8, 10, 15.0, 20.0, 2000.0, timestamp '2014-04-04 01:00:00.000', date '2014-04-04'),
  ('t1b', 8, 16, 19, 17.0, 25.0, 2600.0, timestamp '2014-05-04 01:01:00.000', date '2014-05-04'),
  ('t1a', 16, 12, 21, 15.0, 20.0, 2000.0, timestamp '2014-06-04 01:02:00.001', date '2014-06-04'),
  ('t1a', 16, 12, 10, 15.0, 20.0, 2000.0, timestamp '2014-07-04 01:01:00.000', date '2014-07-04'),
  ('t1c', 8, 16, 19, 17.0, 25.0, 2600.0, timestamp '2014-05-04 01:02:00.001', date '2014-05-05'),
  ('t1d', null, 16, 22, 17.0, 25.0, 2600.0, timestamp '2014-06-04 01:01:00.000', null),
  ('t1d', null, 16, 19, 17.0, 25.0, 2600.0, timestamp '2014-07-04 01:02:00.001', null),
  ('t1e', 10, null, 25, 17.0, 25.0, 2600.0, timestamp '2014-08-04 01:01:00.000', date '2014-08-04'),
  ('t1e', 10, null, 19, 17.0, 25.0, 2600.0, timestamp '2014-09-04 01:02:00.001', date '2014-09-04'),
  ('t1d', 10, null, 12, 17.0, 25.0, 2600.0, timestamp '2015-05-04 01:01:00.000', date '2015-05-04'),
  ('t1a', 6, 8, 10, 15.0, 20.0, 2000.0, timestamp '2014-04-04 01:02:00.001', date '2014-04-04'),
  ('t1e', 10, null, 19, 17.0, 25.0, 2600.0, timestamp '2014-05-04 01:01:00.000', date '2014-05-04');

create temporary view t2(t2a, t2b, t2c, t2d, t2e, t2f, t2g, t2h, t2i) as values
  ('t2a', 6, 12, 14, 15.0, 20.0, 2000.0, timestamp '2014-04-04 01:01:00.000', date '2014-04-04'),
  ('t1b', 10, 12, 19, 17.0, 25.0, 2600.0, timestamp '2014-05-04 01:01:00.000', date '2014-05-04'),
  ('t1b', 8, 16, 119, 17.0, 25.0, 2600.0, timestamp '2015-05-04 01:01:00.000', date '2015-05-04'),
  ('t1c', 12, 16, 219, 17.0, 25.0, 2600.0, timestamp '2016-05-04 01:01:00.000', date '2016-05-04'),
  ('t1b', null, 16, 319, 17.0, 25.0, 2600.0, timestamp '2017-05-04 01:01:00.000', null),
  ('t2e', 8, null, 419, 17.0, 25.0, 2600.0, timestamp '2014-06-04 01:01:00.000', date '2014-06-04'),
  ('t1f', 19, null, 519, 17.0, 25.0, 2600.0, timestamp '2014-05-04 01:01:00.000', date '2014-05-04'),
  ('t1b', 10, 12, 19, 17.0, 25.0, 2600.0, timestamp '2014-06-04 01:01:00.000', date '2014-06-04'),
  ('t1b', 8, 16, 19, 17.0, 25.0, 2600.0, timestamp '2014-07-04 01:01:00.000', date '2014-07-04'),
  ('t1c', 12, 16, 19, 17.0, 25.0, 2600.0, timestamp '2014-08-04 01:01:00.000', date '2014-08-05'),
  ('t1e', 8, null, 19, 17.0, 25.0, 2600.0, timestamp '2014-09-04 01:01:00.000', date '2014-09-04'),
  ('t1f', 19, null, 19, 17.0, 25.0, 2600.0, timestamp '2014-10-04 01:01:00.000', date '2014-10-04'),
  ('t1b', null, 16, 19, 17.0, 25.0, 2600.0, timestamp '2014-05-04 01:01:00.000', null);

create temporary view t3(t3a, t3b, t3c, t3d, t3e, t3f, t3g, t3h, t3i) as values
  ('t3a', 6, 12, 110, 15.0, 20.0, 2000.0, timestamp '2014-04-04 01:02:00.000', date '2014-04-04'),
  ('t3a', 6, 12, 10, 15.0, 20.0, 2000.0, timestamp '2014-05-04 01:02:00.000', date '2014-05-04'),
  ('t1b', 10, 12, 219, 17.0, 25.0, 2600.0, timestamp '2014-05-04 01:02:00.000', date '2014-05-04'),
  ('t1b', 10, 12, 19, 17.0, 25.0, 2600.0, timestamp '2014-05-04 01:02:00.000', date '2014-05-04'),
  ('t1b', 8, 16, 319, 17.0, 25.0, 2600.0, timestamp '2014-06-04 01:02:00.000', date '2014-06-04'),
  ('t1b', 8, 16, 19, 17.0, 25.0, 2600.0, timestamp '2014-07-04 01:02:00.000', date '2014-07-04'),
  ('t3c', 17, 16, 519, 17.0, 25.0, 2600.0, timestamp '2014-08-04 01:02:00.000', date '2014-08-04'),
  ('t3c', 17, 16, 19, 17.0, 25.0, 2600.0, timestamp '2014-09-04 01:02:00.000', date '2014-09-05'),
  ('t1b', null, 16, 419, 17.0, 25.0, 2600.0, timestamp '2014-10-04 01:02:00.000', null),
  ('t1b', null, 16, 19, 17.0, 25.0, 2600.0, timestamp '2014-11-04 01:02:00.000', null),
  ('t3b', 8, null, 719, 17.0, 25.0, 2600.0, timestamp '2014-05-04 01:02:00.000', date '2014-05-04'),
  ('t3b', 8, null, 19, 17.0, 25.0, 2600.0, timestamp '2015-05-04 01:02:00.000', date '2015-05-04');

-- correlated IN subquery
-- simple select
-- TC 01.01
SELECT *
FROM   t1
WHERE  t1a IN (SELECT t2a
               FROM   t2);

-- TC 01.02
SELECT *
FROM   t1
WHERE  t1b IN (SELECT t2b
               FROM   t2
               WHERE  t1a = t2a);

-- TC 01.03
SELECT t1a,
       t1b
FROM   t1
WHERE  t1c IN (SELECT t2b
               FROM   t2
               WHERE  t1a != t2a);

-- TC 01.04
SELECT t1a,
       t1b
FROM   t1
WHERE  t1c IN (SELECT t2b
               FROM   t2
               WHERE  t1a = t2a
                       OR t1b > t2b);

-- TC 01.05
SELECT t1a,
       t1b
FROM   t1
WHERE  t1c IN (SELECT t2b
               FROM   t2
               WHERE  t2i IN (SELECT t3i
                              FROM   t3
                              WHERE  t2c = t3c));

-- TC 01.06
SELECT t1a,
       t1b
FROM   t1
WHERE  t1c IN (SELECT t2b
               FROM   t2
               WHERE  t2a IN (SELECT t3a
                              FROM   t3
                              WHERE  t2c = t3c
                                     AND t2b IS NOT NULL));

-- simple select for NOT IN
-- TC 01.07
SELECT DISTINCT( t1a ),
               t1b,
               t1h
FROM   t1
WHERE  t1a NOT IN (SELECT t2a
                   FROM   t2);

-- DDLs
create temporary view a(a1, a2) as values
  (1, 1), (2, 1), (null, 1), (1, 3), (null, 3), (1, null), (null, 2);

create temporary view b(b1, b2, b3) as values
  (1, 1, 2), (null, 3, 2), (1, null, 2), (1, 2, null);

-- TC 02.01
SELECT a1, a2
FROM   a
WHERE  a1 NOT IN (SELECT b.b1
                  FROM   b
                  WHERE  a.a2 = b.b2)
;

-- TC 02.02
SELECT a1, a2
FROM   a
WHERE  a1 NOT IN (SELECT b.b1
                  FROM   b
                  WHERE  a.a2 = b.b2
                  AND    b.b3 > 1)
;
