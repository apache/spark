-- A test suite for scalar subquery in predicate context

CREATE OR REPLACE TEMPORARY VIEW p AS VALUES (1, 1) AS T(pk, pv);
CREATE OR REPLACE TEMPORARY VIEW c AS VALUES (1, 1) AS T(ck, cv);

-- SPARK-18814.1: Simplified version of TPCDS-Q32
SELECT pk, cv
FROM   p, c
WHERE  p.pk = c.ck
AND    c.cv = (SELECT avg(c1.cv)
               FROM   c c1
               WHERE  c1.ck = p.pk);

-- SPARK-18814.2: Adding stack of aggregates
SELECT pk, cv
FROM   p, c
WHERE  p.pk = c.ck
AND    c.cv = (SELECT max(avg)
               FROM   (SELECT   c1.cv, avg(c1.cv) avg
                       FROM     c c1
                       WHERE    c1.ck = p.pk
                       GROUP BY c1.cv));

create temporary view t1 as select * from values
  ('val1a', 6S, 8, 10L, float(15.0), 20D, 20E2, timestamp '2014-04-04 00:00:00.000', date '2014-04-04'),
  ('val1b', 8S, 16, 19L, float(17.0), 25D, 26E2, timestamp '2014-05-04 01:01:00.000', date '2014-05-04'),
  ('val1a', 16S, 12, 21L, float(15.0), 20D, 20E2, timestamp '2014-06-04 01:02:00.001', date '2014-06-04'),
  ('val1a', 16S, 12, 10L, float(15.0), 20D, 20E2, timestamp '2014-07-04 01:01:00.000', date '2014-07-04'),
  ('val1c', 8S, 16, 19L, float(17.0), 25D, 26E2, timestamp '2014-05-04 01:02:00.001', date '2014-05-05'),
  ('val1d', null, 16, 22L, float(17.0), 25D, 26E2, timestamp '2014-06-04 01:01:00.000', null),
  ('val1d', null, 16, 19L, float(17.0), 25D, 26E2, timestamp '2014-07-04 01:02:00.001', null),
  ('val1e', 10S, null, 25L, float(17.0), 25D, 26E2, timestamp '2014-08-04 01:01:00.000', date '2014-08-04'),
  ('val1e', 10S, null, 19L, float(17.0), 25D, 26E2, timestamp '2014-09-04 01:02:00.001', date '2014-09-04'),
  ('val1d', 10S, null, 12L, float(17.0), 25D, 26E2, timestamp '2015-05-04 01:01:00.000', date '2015-05-04'),
  ('val1a', 6S, 8, 10L, float(15.0), 20D, 20E2, timestamp '2014-04-04 01:02:00.001', date '2014-04-04'),
  ('val1e', 10S, null, 19L, float(17.0), 25D, 26E2, timestamp '2014-05-04 01:01:00.000', date '2014-05-04')
  as t1(t1a, t1b, t1c, t1d, t1e, t1f, t1g, t1h, t1i);

create temporary view t2 as select * from values
  ('val2a', 6S, 12, 14L, float(15), 20D, 20E2, timestamp '2014-04-04 01:01:00.000', date '2014-04-04'),
  ('val1b', 10S, 12, 19L, float(17), 25D, 26E2, timestamp '2014-05-04 01:01:00.000', date '2014-05-04'),
  ('val1b', 8S, 16, 119L, float(17), 25D, 26E2, timestamp '2015-05-04 01:01:00.000', date '2015-05-04'),
  ('val1c', 12S, 16, 219L, float(17), 25D, 26E2, timestamp '2016-05-04 01:01:00.000', date '2016-05-04'),
  ('val1b', null, 16, 319L, float(17), 25D, 26E2, timestamp '2017-05-04 01:01:00.000', null),
  ('val2e', 8S, null, 419L, float(17), 25D, 26E2, timestamp '2014-06-04 01:01:00.000', date '2014-06-04'),
  ('val1f', 19S, null, 519L, float(17), 25D, 26E2, timestamp '2014-05-04 01:01:00.000', date '2014-05-04'),
  ('val1b', 10S, 12, 19L, float(17), 25D, 26E2, timestamp '2014-06-04 01:01:00.000', date '2014-06-04'),
  ('val1b', 8S, 16, 19L, float(17), 25D, 26E2, timestamp '2014-07-04 01:01:00.000', date '2014-07-04'),
  ('val1c', 12S, 16, 19L, float(17), 25D, 26E2, timestamp '2014-08-04 01:01:00.000', date '2014-08-05'),
  ('val1e', 8S, null, 19L, float(17), 25D, 26E2, timestamp '2014-09-04 01:01:00.000', date '2014-09-04'),
  ('val1f', 19S, null, 19L, float(17), 25D, 26E2, timestamp '2014-10-04 01:01:00.000', date '2014-10-04'),
  ('val1b', null, 16, 19L, float(17), 25D, 26E2, timestamp '2014-05-04 01:01:00.000', null)
  as t2(t2a, t2b, t2c, t2d, t2e, t2f, t2g, t2h, t2i);

create temporary view t3 as select * from values
  ('val3a', 6S, 12, 110L, float(15), 20D, 20E2, timestamp '2014-04-04 01:02:00.000', date '2014-04-04'),
  ('val3a', 6S, 12, 10L, float(15), 20D, 20E2, timestamp '2014-05-04 01:02:00.000', date '2014-05-04'),
  ('val1b', 10S, 12, 219L, float(17), 25D, 26E2, timestamp '2014-05-04 01:02:00.000', date '2014-05-04'),
  ('val1b', 10S, 12, 19L, float(17), 25D, 26E2, timestamp '2014-05-04 01:02:00.000', date '2014-05-04'),
  ('val1b', 8S, 16, 319L, float(17), 25D, 26E2, timestamp '2014-06-04 01:02:00.000', date '2014-06-04'),
  ('val1b', 8S, 16, 19L, float(17), 25D, 26E2, timestamp '2014-07-04 01:02:00.000', date '2014-07-04'),
  ('val3c', 17S, 16, 519L, float(17), 25D, 26E2, timestamp '2014-08-04 01:02:00.000', date '2014-08-04'),
  ('val3c', 17S, 16, 19L, float(17), 25D, 26E2, timestamp '2014-09-04 01:02:00.000', date '2014-09-05'),
  ('val1b', null, 16, 419L, float(17), 25D, 26E2, timestamp '2014-10-04 01:02:00.000', null),
  ('val1b', null, 16, 19L, float(17), 25D, 26E2, timestamp '2014-11-04 01:02:00.000', null),
  ('val3b', 8S, null, 719L, float(17), 25D, 26E2, timestamp '2014-05-04 01:02:00.000', date '2014-05-04'),
  ('val3b', 8S, null, 19L, float(17), 25D, 26E2, timestamp '2015-05-04 01:02:00.000', date '2015-05-04')
  as t3(t3a, t3b, t3c, t3d, t3e, t3f, t3g, t3h, t3i);

-- Group 1: scalar subquery in predicate context
--          no correlation
-- TC 01.01
SELECT t1a, t1b
FROM   t1
WHERE  t1c = (SELECT max(t2c)
              FROM   t2);

-- TC 01.02
SELECT t1a, t1d, t1f
FROM   t1
WHERE  t1c = (SELECT max(t2c)
              FROM   t2)
AND    t1b > (SELECT min(t3b)
              FROM   t3);

-- TC 01.03
SELECT t1a, t1h
FROM   t1
WHERE  t1c = (SELECT max(t2c)
              FROM   t2)
OR     t1b = (SELECT min(t3b)
              FROM   t3
              WHERE  t3b > 10);

-- TC 01.04
-- scalar subquery over outer join
SELECT t1a, t1b, t2d
FROM   t1 LEFT JOIN t2
       ON t1a = t2a
WHERE  t1b = (SELECT min(t3b)
              FROM   t3);

-- TC 01.05
-- test casting
SELECT t1a, t1b, t1g
FROM   t1
WHERE  t1c + 5 = (SELECT max(t2e)
                  FROM   t2);

-- TC 01.06
-- test casting
SELECT t1a, t1h
FROM   t1
WHERE  date(t1h) = (SELECT min(t2i)
                    FROM   t2);

-- TC 01.07
-- same table, expressions in scalar subquery
SELECT t2d, t1a
FROM   t1, t2
WHERE  t1b = t2b
AND    t2c + 1 = (SELECT max(t2c) + 1
                  FROM   t2, t1
                  WHERE  t2b = t1b);

-- TC 01.08
-- same table
SELECT DISTINCT t2a, max_t1g
FROM   t2, (SELECT   max(t1g) max_t1g, t1a
            FROM     t1
            GROUP BY t1a) t1
WHERE  t2a = t1a
AND    max_t1g = (SELECT max(t1g)
                  FROM   t1);

-- TC 01.09
-- more than one scalar subquery
SELECT t3b, t3c
FROM   t3
WHERE  (SELECT max(t3c)
        FROM   t3
        WHERE  t3b > 10) >=
       (SELECT min(t3b)
        FROM   t3
        WHERE  t3c > 0)
AND    (t3b is null or t3c is null);

-- Group 2: scalar subquery in predicate context
--          with correlation
-- TC 02.01
SELECT t1a
FROM   t1
WHERE  t1a < (SELECT   max(t2a)
              FROM     t2
              WHERE    t2c = t1c
              GROUP BY t2c);

-- TC 02.02
SELECT t1a, t1c
FROM   t1
WHERE  (SELECT   max(t2a)
        FROM     t2
        WHERE    t2c = t1c
        GROUP BY t2c) IS NULL;

-- TC 02.03
SELECT t1a
FROM   t1
WHERE  t1a = (SELECT   max(t2a)
              FROM     t2
              WHERE    t2c = t1c
              GROUP BY t2c
              HAVING   count(*) >= 0)
OR     t1i > '2014-12-31';

-- TC 02.03.01
SELECT t1a
FROM   t1
WHERE  t1a = (SELECT   max(t2a)
              FROM     t2
              WHERE    t2c = t1c
              GROUP BY t2c
              HAVING   count(*) >= 1)
OR     t1i > '2014-12-31';

-- TC 02.04
-- t1 on the right of an outer join
-- can be reduced to inner join
SELECT count(t1a)
FROM   t1 RIGHT JOIN t2
ON     t1d = t2d
WHERE  t1a < (SELECT   max(t2a)
              FROM     t2
              WHERE    t2c = t1c
              GROUP BY t2c);

-- TC 02.05
SELECT t1a
FROM   t1
WHERE  t1b <= (SELECT   max(t2b)
               FROM     t2
               WHERE    t2c = t1c
               GROUP BY t2c)
AND    t1b >= (SELECT   min(t2b)
               FROM     t2
               WHERE    t2c = t1c
               GROUP BY t2c);

-- TC 02.06
-- set op
SELECT t1a
FROM   t1
WHERE  t1a <= (SELECT   max(t2a)
               FROM     t2
               WHERE    t2c = t1c
               GROUP BY t2c)
INTERSECT
SELECT t1a
FROM   t1
WHERE  t1a >= (SELECT   min(t2a)
               FROM     t2
               WHERE    t2c = t1c
               GROUP BY t2c);

-- TC 02.07.01
-- set op
SELECT t1a
FROM   t1
WHERE  t1a <= (SELECT   max(t2a)
               FROM     t2
               WHERE    t2c = t1c
               GROUP BY t2c)
UNION ALL
SELECT t1a
FROM   t1
WHERE  t1a >= (SELECT   min(t2a)
               FROM     t2
               WHERE    t2c = t1c
               GROUP BY t2c);

-- TC 02.07.02
-- set op
SELECT t1a
FROM   t1
WHERE  t1a <= (SELECT   max(t2a)
               FROM     t2
               WHERE    t2c = t1c
               GROUP BY t2c)
UNION DISTINCT
SELECT t1a
FROM   t1
WHERE  t1a >= (SELECT   min(t2a)
               FROM     t2
               WHERE    t2c = t1c
               GROUP BY t2c);

-- TC 02.08
-- set op
SELECT t1a
FROM   t1
WHERE  t1a <= (SELECT   max(t2a)
               FROM     t2
               WHERE    t2c = t1c
               GROUP BY t2c)
MINUS
SELECT t1a
FROM   t1
WHERE  t1a >= (SELECT   min(t2a)
               FROM     t2
               WHERE    t2c = t1c
               GROUP BY t2c);

-- TC 02.09
-- in HAVING clause
SELECT   t1a
FROM     t1
GROUP BY t1a, t1c
HAVING   max(t1b) <= (SELECT   max(t2b)
                      FROM     t2
                      WHERE    t2c = t1c
                      GROUP BY t2c);
