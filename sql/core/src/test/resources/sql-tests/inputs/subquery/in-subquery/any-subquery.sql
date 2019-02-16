-- A basic test suite for ANY/SOME predicate
create temporary view t1 as select * from values
  ("val1a", 6S, 8, 10L, float(15.0), 20D, 20E2, timestamp '2014-04-04 01:00:00.000', date '2014-04-04'),
  ("val1b", 8S, 16, 19L, float(17.0), 25D, 26E2, timestamp '2014-05-04 01:01:00.000', date '2014-05-04'),
  ("val1a", 16S, 12, 21L, float(15.0), 20D, 20E2, timestamp '2014-06-04 01:02:00.001', date '2014-06-04'),
  ("val1a", 16S, 12, 10L, float(15.0), 20D, 20E2, timestamp '2014-07-04 01:01:00.000', date '2014-07-04'),
  ("val1c", 8S, 16, 19L, float(17.0), 25D, 26E2, timestamp '2014-05-04 01:02:00.001', date '2014-05-05'),
  ("val1d", null, 16, 22L, float(17.0), 25D, 26E2, timestamp '2014-06-04 01:01:00.000', null),
  ("val1d", null, 16, 19L, float(17.0), 25D, 26E2, timestamp '2014-07-04 01:02:00.001', null),
  ("val1e", 10S, null, 25L, float(17.0), 25D, 26E2, timestamp '2014-08-04 01:01:00.000', date '2014-08-04'),
  ("val1e", 10S, null, 19L, float(17.0), 25D, 26E2, timestamp '2014-09-04 01:02:00.001', date '2014-09-04'),
  ("val1d", 10S, null, 12L, float(17.0), 25D, 26E2, timestamp '2015-05-04 01:01:00.000', date '2015-05-04'),
  ("val1a", 6S, 8, 10L, float(15.0), 20D, 20E2, timestamp '2014-04-04 01:02:00.001', date '2014-04-04'),
  ("val1e", 10S, null, 19L, float(17.0), 25D, 26E2, timestamp '2014-05-04 01:01:00.000', date '2014-05-04')
  as t1(t1a, t1b, t1c, t1d, t1e, t1f, t1g, t1h, t1i);

create temporary view t2 as select * from values
  ("val2a", 6S, 12, 14L, float(15), 20D, 20E2, timestamp '2014-04-04 01:01:00.000', date '2014-04-04'),
  ("val1b", 10S, 12, 19L, float(17), 25D, 26E2, timestamp '2014-05-04 01:01:00.000', date '2014-05-04'),
  ("val1b", 8S, 16, 119L, float(17), 25D, 26E2, timestamp '2015-05-04 01:01:00.000', date '2015-05-04'),
  ("val1c", 12S, 16, 219L, float(17), 25D, 26E2, timestamp '2016-05-04 01:01:00.000', date '2016-05-04'),
  ("val1b", null, 16, 319L, float(17), 25D, 26E2, timestamp '2017-05-04 01:01:00.000', null),
  ("val2e", 8S, null, 419L, float(17), 25D, 26E2, timestamp '2014-06-04 01:01:00.000', date '2014-06-04'),
  ("val1f", 19S, null, 519L, float(17), 25D, 26E2, timestamp '2014-05-04 01:01:00.000', date '2014-05-04'),
  ("val1b", 10S, 12, 19L, float(17), 25D, 26E2, timestamp '2014-06-04 01:01:00.000', date '2014-06-04'),
  ("val1b", 8S, 16, 19L, float(17), 25D, 26E2, timestamp '2014-07-04 01:01:00.000', date '2014-07-04'),
  ("val1c", 12S, 16, 19L, float(17), 25D, 26E2, timestamp '2014-08-04 01:01:00.000', date '2014-08-05'),
  ("val1e", 8S, null, 19L, float(17), 25D, 26E2, timestamp '2014-09-04 01:01:00.000', date '2014-09-04'),
  ("val1f", 19S, null, 19L, float(17), 25D, 26E2, timestamp '2014-10-04 01:01:00.000', date '2014-10-04'),
  ("val1b", null, 16, 19L, float(17), 25D, 26E2, timestamp '2014-05-04 01:01:00.000', null)
  as t2(t2a, t2b, t2c, t2d, t2e, t2f, t2g, t2h, t2i);

create temporary view t3 as select * from values
  ("val3a", 6S, 12, 110L, float(15), 20D, 20E2, timestamp '2014-04-04 01:02:00.000', date '2014-04-04'),
  ("val3a", 6S, 12, 10L, float(15), 20D, 20E2, timestamp '2014-05-04 01:02:00.000', date '2014-05-04'),
  ("val1b", 10S, 12, 219L, float(17), 25D, 26E2, timestamp '2014-05-04 01:02:00.000', date '2014-05-04'),
  ("val1b", 10S, 12, 19L, float(17), 25D, 26E2, timestamp '2014-05-04 01:02:00.000', date '2014-05-04'),
  ("val1b", 8S, 16, 319L, float(17), 25D, 26E2, timestamp '2014-06-04 01:02:00.000', date '2014-06-04'),
  ("val1b", 8S, 16, 19L, float(17), 25D, 26E2, timestamp '2014-07-04 01:02:00.000', date '2014-07-04'),
  ("val3c", 17S, 16, 519L, float(17), 25D, 26E2, timestamp '2014-08-04 01:02:00.000', date '2014-08-04'),
  ("val3c", 17S, 16, 19L, float(17), 25D, 26E2, timestamp '2014-09-04 01:02:00.000', date '2014-09-05'),
  ("val1b", null, 16, 419L, float(17), 25D, 26E2, timestamp '2014-10-04 01:02:00.000', null),
  ("val1b", null, 16, 19L, float(17), 25D, 26E2, timestamp '2014-11-04 01:02:00.000', null),
  ("val3b", 8S, null, 719L, float(17), 25D, 26E2, timestamp '2014-05-04 01:02:00.000', date '2014-05-04'),
  ("val3b", 8S, null, 19L, float(17), 25D, 26E2, timestamp '2015-05-04 01:02:00.000', date '2015-05-04')
  as t3(t3a, t3b, t3c, t3d, t3e, t3f, t3g, t3h, t3i);

-- Simple ANY subquery
SELECT *
FROM   t1
WHERE  t1a = ANY (SELECT t2a
                  FROM   t2);

-- Alia
SELECT *
FROM   t1
WHERE  t1a = SOME (SELECT t2a
                   FROM   t2);

-- Type Coercion
SELECT *
FROM   t1
WHERE  t1c >= ANY (SELECT t2b
                   FROM   t2);

-- Correlated subquery
SELECT *
FROM   t1
WHERE  t1c <= ANY (SELECT t2b
                   FROM   t2
                   WHERE  t1a = t2a
                   OR     t1b > t2b);

-- Multi-column ANY subquery
SELECT t1a, t1b
FROM   t1
WHERE  (t1b, t1c) <= ANY (SELECT t2b, t2c
                          FROM   t2
                          WHERE  t2c > 12);

-- Invalid Query(columns number)
SELECT *
FROM   t1
WHERE  (t1b, t1c) < ANY (SELECT t2b, t2c, t2d
                         FROM   t2
                         WHERE  t2c IS NOT NULL);

-- Invalid Query(columns type)
SELECT *
FROM   t1
WHERE  (t1b, t1c) > ANY (SELECT t2b, t2h
                         FROM   t2
                         WHERE  t2b > 2);

-- Not ANY subquery
SELECT t1a, t1b
FROM   t1
WHERE  t1c NOT > ANY (SELECT t2c
                      FROM   t2 );

-- Self join
SELECT t1a, t1b
FROM   t1
WHERE  t1c > ANY (SELECT t1c
                  FROM   t1 );

-- ANY subquery with join 1
SELECT t1a, t2a
FROM   t1
JOIN   t2
ON     t1c = t2c
WHERE   t1b > ANY (SELECT t3b
                  FROM   t3);

-- ANY subquery with join 2
SELECT t1a, t2a
FROM   t1, t2
WHERE  t1c = t2c
AND    t1b > ANY (SELECT t3b
                  FROM   t3);

-- ANY subquery with join 3
SELECT t1a, t2a
FROM   t1
JOIN   t2
ON     t1c = t2c
WHERE  t1b > ANY (SELECT t2b
                  FROM   t2);

-- ANY subquery with group 1
SELECT t1a, Avg(t1c)
FROM   t1
WHERE  t1a = ANY (SELECT t2a
                  FROM   t2)
GROUP  BY t1a;

-- ANY subquery with group 2
SELECT t1a,
       Max(t1b)
FROM   t1
WHERE  t1b > ANY (SELECT t2b
                  FROM   t2
                  WHERE  t1a = t2a)
GROUP  BY t1a,
          t1d;

-- ANY subquery with group in subquery
SELECT t1a, t1b
FROM   t1
WHERE  t1b < ANY (SELECT Max(t2b)
                  FROM   t2
                  GROUP  BY t2a);

-- ANY subquery with having in subquery
SELECT t1a, t1b, t1h
FROM   t1
WHERE  t1b = ANY (SELECT t2b
                  FROM   t2
                  GROUP BY t2b
                  HAVING t2b < 10);

-- ANY subquery with having in parent
SELECT t1a, t1b, t1c
FROM   t1
WHERE  t1b < ANY (SELECT t2b
                  FROM   t2
                  WHERE t1c < t2c)
GROUP BY t1a, t1b, t1c
HAVING t1b <= 8;

-- ANY subquery with order by ins subquery
SELECT t1a, t1b
FROM   t1
WHERE  t1b < ANY(SELECT Min(t2b)
                 FROM   t2
                 GROUP  BY t2a
                 ORDER  BY t2a DESC);

-- ANY subquery with order by in parent
SELECT t1a, t1b
FROM   t1
WHERE  t1a != ANY (SELECT t2a
                   FROM   t2)
ORDER  BY t1a;