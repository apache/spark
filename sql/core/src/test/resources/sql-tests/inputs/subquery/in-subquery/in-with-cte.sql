-- A test suite for in with cte in parent side, subquery, and both predicate subquery
-- It includes correlated cases.

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
-- outside CTE
-- TC 01.01
WITH cte1
     AS (SELECT t1a,
                t1b
         FROM   t1
         WHERE  t1a = 'val1a')
SELECT t1a,
       t1b,
       t1c,
       t1d,
       t1h
FROM   t1
WHERE  t1b IN (SELECT cte1.t1b
               FROM   cte1
               WHERE  cte1.t1b > 0);

-- TC 01.02
WITH cte1 AS
(
       SELECT t1a,
              t1b
       FROM   t1)
SELECT count(distinct(t1a)), t1b, t1c
FROM   t1
WHERE  t1b IN
       (
              SELECT cte1.t1b
              FROM   cte1
              WHERE  cte1.t1b > 0
              UNION
              SELECT cte1.t1b
              FROM   cte1
              WHERE  cte1.t1b > 5
              UNION ALL
              SELECT cte1.t1b
              FROM   cte1
              INTERSECT
              SELECT cte1.t1b
              FROM   cte1
              UNION
              SELECT cte1.t1b
              FROM   cte1 )
GROUP BY t1a, t1b, t1c
HAVING t1c IS NOT NULL;

-- TC 01.03
WITH cte1 AS
(
       SELECT t1a,
              t1b,
              t1c,
              t1d,
              t1e
       FROM   t1)
SELECT t1a,
       t1b,
       t1c,
       t1h
FROM   t1
WHERE  t1c IN
       (
              SELECT          cte1.t1c
              FROM            cte1
              JOIN            cte1 cte2
              on              cte1.t1b > cte2.t1b
              FULL OUTER JOIN cte1 cte3
              ON              cte1.t1c = cte3.t1c
              LEFT JOIN       cte1 cte4
              ON              cte1.t1d = cte4.t1d
              INNER JOIN  cte1 cte5
              ON              cte1.t1b < cte5.t1b
              LEFT OUTER JOIN  cte1 cte6
              ON              cte1.t1d > cte6.t1d);

-- CTE inside and outside
-- TC 01.04
WITH cte1
     AS (SELECT t1a,
                t1b
         FROM   t1
         WHERE  t1b IN (SELECT t2b
                        FROM   t2
                               RIGHT JOIN t1
                                       ON t1c = t2c
                               LEFT JOIN t3
                                      ON t2d = t3d)
                AND t1a = 'val1b')
SELECT *
FROM   (SELECT *
        FROM   cte1
               JOIN cte1 cte2
                 ON cte1.t1b > 5
                    AND cte1.t1a = cte2.t1a
               FULL OUTER JOIN cte1 cte3
                            ON cte1.t1a = cte3.t1a
               INNER JOIN cte1 cte4
                       ON cte1.t1b = cte4.t1b) s;

-- TC 01.05
WITH cte1 AS
(
       SELECT t1a,
              t1b,
              t1h
       FROM   t1
       WHERE  t1a IN
              (
                     SELECT t2a
                     FROM   t2
                     WHERE  t1b < t2b))
SELECT   Count(DISTINCT t1a),
         t1b
FROM     (
                    SELECT     cte1.t1a,
                               cte1.t1b
                    FROM       cte1
                    JOIN       cte1 cte2
                    on         cte1.t1h >= cte2.t1h) s
WHERE    t1b IN
         (
                SELECT t1b
                FROM   t1)
GROUP BY t1b;

-- TC 01.06
WITH cte1 AS
(
       SELECT t1a,
              t1b,
              t1c
       FROM   t1
       WHERE  t1b IN
              (
                     SELECT t2b
                     FROM   t2 FULL OUTER JOIN T3 on t2a = t3a
                     WHERE  t1c = t2c) AND
              t1a = 'val1b')
SELECT *
FROM            (
                       SELECT *
                       FROM   cte1
                       INNER JOIN   cte1 cte2 ON cte1.t1a = cte2.t1a
                       RIGHT OUTER JOIN cte1 cte3  ON cte1.t1b = cte3.t1b
                       LEFT OUTER JOIN cte1 cte4 ON cte1.t1c = cte4.t1c
                       ) s
;

-- TC 01.07
WITH cte1
     AS (SELECT t1a,
                t1b
         FROM   t1
         WHERE  t1b IN (SELECT t2b
                        FROM   t2
                        WHERE  t1c = t2c))
SELECT Count(DISTINCT( s.t1a )),
       s.t1b
FROM   (SELECT cte1.t1a,
               cte1.t1b
        FROM   cte1
               RIGHT OUTER JOIN cte1 cte2
                             ON cte1.t1a = cte2.t1a) s
GROUP  BY s.t1b;

-- TC 01.08
WITH cte1 AS
(
       SELECT t1a,
              t1b
       FROM   t1
       WHERE  t1b IN
              (
                     SELECT t2b
                     FROM   t2
                     WHERE  t1c = t2c))
SELECT DISTINCT(s.t1b)
FROM            (
                                SELECT          cte1.t1b
                                FROM            cte1
                                LEFT OUTER JOIN cte1 cte2
                                ON              cte1.t1b = cte2.t1b) s
WHERE           s.t1b IN
                (
                       SELECT t1.t1b
                       FROM   t1 INNER
                       JOIN   cte1
                       ON     t1.t1a = cte1.t1a);

-- CTE with NOT IN
-- TC 01.09
WITH cte1
     AS (SELECT t1a,
                t1b
         FROM   t1
         WHERE  t1a = 'val1d')
SELECT t1a,
       t1b,
       t1c,
       t1h
FROM   t1
WHERE  t1b NOT IN (SELECT cte1.t1b
                   FROM   cte1
                   WHERE  cte1.t1b < 0) AND
       t1c > 10;

-- TC 01.10
WITH cte1 AS
(
       SELECT t1a,
              t1b,
              t1c,
              t1d,
              t1h
       FROM   t1
       WHERE  t1d NOT IN
              (
                              SELECT          t2d
                              FROM            t2
                              FULL OUTER JOIN t3 ON t2a = t3a
                              JOIN t1 on t1b = t2b))
SELECT   t1a,
         t1b,
         t1c,
         t1d,
         t1h
FROM     t1
WHERE    t1b NOT IN
         (
                    SELECT     cte1.t1b
                    FROM       cte1 INNER
                    JOIN       cte1 cte2 ON cte1.t1a = cte2.t1a
                    RIGHT JOIN cte1 cte3 ON cte1.t1b = cte3.t1b
                    JOIN cte1 cte4 ON cte1.t1c = cte4.t1c) AND
         t1c IS NOT NULL
ORDER BY t1c DESC;

