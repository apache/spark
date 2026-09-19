-- A test suite for not-in-joins in parent side, subquery, and both predicate subquery
-- It includes correlated cases.
--ONLY_IF spark

--CONFIG_DIM1 spark.sql.optimizeNullAwareAntiJoin=true
--CONFIG_DIM1 spark.sql.optimizeNullAwareAntiJoin=false

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
-- different not JOIN in parent side
-- TC 01.01
SELECT t1a,
       t1b,
       t1c,
       t3a,
       t3b,
       t3c
FROM   t1
       JOIN t3
WHERE  t1a NOT IN (SELECT t2a
                   FROM   t2)
       AND t1b = t3b;

-- TC 01.02
SELECT          t1a,
                t1b,
                t1c,
                count(distinct(t3a)),
                t3b,
                t3c
FROM            t1
FULL OUTER JOIN t3 on t1b != t3b
RIGHT JOIN      t2 on t1c = t2c
where           t1a NOT IN
                (
                       SELECT t2a
                       FROM   t2
                       WHERE  t2c NOT IN
                              (
                                     SELECT t1c
                                     FROM   t1
                                     WHERE  t1a = t2a))
AND             t1b != t3b
AND             t1d = t2d
GROUP BY        t1a, t1b, t1c, t3a, t3b, t3c
HAVING          count(distinct(t3a)) >= 1
ORDER BY        t1a, t3b;

-- TC 01.03
SELECT t1a,
       t1b,
       t1c,
       t1d,
       t1h
FROM   t1
WHERE  t1a NOT IN
       (
                 SELECT    t2a
                 FROM      t2
                 LEFT JOIN t3 on t2b = t3b
                 WHERE t1d = t2d
                  )
AND    t1d NOT IN
       (
              SELECT t2d
              FROM   t2
              RIGHT JOIN t1 on t2e = t1e
              WHERE t1a = t2a);

-- TC 01.04
SELECT Count(DISTINCT( t1a )),
       t1b,
       t1c,
       t1d
FROM   t1
WHERE  t1a NOT IN (SELECT t2a
                   FROM   t2
                   JOIN t1
                   WHERE  t2b <> t1b)
GROUP  BY t1b,
          t1c,
          t1d
HAVING t1d NOT IN (SELECT t2d
                   FROM   t2
                   WHERE  t1d = t2d)
ORDER BY t1b DESC, t1d ASC;

-- TC 01.05
SELECT   COUNT(DISTINCT(t1a)),
         t1b,
         t1c,
         t1d
FROM     t1
WHERE    t1a NOT IN
         (
                SELECT t2a
                FROM   t2 INNER
                JOIN   t1 ON t1a = t2a)
GROUP BY t1b,
         t1c,
         t1d
HAVING   t1b < sum(t1c);

-- TC 01.06
SELECT   COUNT(DISTINCT(t1a)),
         t1b,
         t1c,
         t1d
FROM     t1
WHERE    t1a NOT IN
         (
                SELECT t2a
                FROM   t2 INNER
                JOIN   t1
                ON     t1a = t2a)
AND      t1d NOT IN
         (
                    SELECT     t2d
                    FROM       t2
                    INNER JOIN t3
                    ON         t2b = t3b )
GROUP BY t1b,
         t1c,
         t1d
HAVING   t1b < sum(t1c);

