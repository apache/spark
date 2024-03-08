-- A test suite for set-operations in parent side, subquery, and both predicate subquery
-- It includes correlated cases.
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
-- UNION, UNION ALL, UNION DISTINCT, INTERSECT and EXCEPT in the parent
-- TC 01.01
SELECT t2a,
       t2b,
       t2c,
       t2h,
       t2i
FROM   (SELECT *
        FROM   t2
        WHERE  t2a IN (SELECT t1a
                       FROM   t1)
        UNION ALL
        SELECT *
        FROM   t3
        WHERE  t3a IN (SELECT t1a
                       FROM   t1)) AS t3
WHERE  t2i IS NOT NULL AND
       2 * t2b = t2c
ORDER  BY t2c DESC nulls first;

-- TC 01.02
SELECT t2a,
       t2b,
       t2d,
       Count(DISTINCT( t2h )),
       t2i
FROM   (SELECT *
        FROM   t2
        WHERE  t2a IN (SELECT t1a
                       FROM   t1
                       WHERE  t2b = t1b)
        UNION
        SELECT *
        FROM   t1
        WHERE  t1a IN (SELECT t3a
                       FROM   t3
                       WHERE  t1c = t3c)) AS t3
GROUP  BY t2a,
          t2b,
          t2d,
          t2i
ORDER  BY t2d DESC;

-- TC 01.03
SELECT t2a,
       t2b,
       t2c,
       Min(t2d)
FROM   t2
WHERE  t2a IN (SELECT t1a
               FROM   t1
               WHERE  t1b = t2b)
GROUP BY t2a, t2b, t2c
UNION ALL
SELECT t2a,
       t2b,
       t2c,
       Max(t2d)
FROM   t2
WHERE  t2a IN (SELECT t1a
               FROM   t1
               WHERE  t2c = t1c)
GROUP BY t2a, t2b, t2c
UNION
SELECT t3a,
       t3b,
       t3c,
       Min(t3d)
FROM   t3
WHERE  t3a IN (SELECT t2a
               FROM   t2
               WHERE  t3c = t2c)
GROUP BY t3a, t3b, t3c
UNION DISTINCT
SELECT t1a,
       t1b,
       t1c,
       Max(t1d)
FROM   t1
WHERE  t1a IN (SELECT t3a
               FROM   t3
               WHERE  t3d = t1d)
GROUP BY t1a, t1b, t1c;

-- TC 01.04
SELECT DISTINCT( t2a ),
               t2b,
               Count(t2c),
               t2d,
               t2h,
               t2i
FROM   t2
WHERE  t2a IN (SELECT t1a
               FROM   t1
               WHERE  t1b = t2b)
GROUP  BY t2a,
          t2b,
          t2c,
          t2d,
          t2h,
          t2i
UNION
SELECT DISTINCT( t2a ),
               t2b,
               Count(t2c),
               t2d,
               t2h,
               t2i
FROM   t2
WHERE  t2a IN (SELECT t1a
               FROM   t1
               WHERE  t2c = t1c)
GROUP  BY t2a,
          t2b,
          t2c,
          t2d,
          t2h,
          t2i
HAVING t2b IS NOT NULL;

-- TC 01.05
SELECT t2a,
               t2b,
               Count(t2c),
               t2d,
               t2h,
               t2i
FROM   t2
WHERE  t2a IN (SELECT DISTINCT(t1a)
               FROM   t1
               WHERE  t1b = t2b)
GROUP  BY t2a,
          t2b,
          t2c,
          t2d,
          t2h,
          t2i

UNION
SELECT DISTINCT( t2a ),
               t2b,
               Count(t2c),
               t2d,
               t2h,
               t2i
FROM   t2
WHERE  t2b IN (SELECT Max(t1b)
               FROM   t1
               WHERE  t2c = t1c)
GROUP  BY t2a,
          t2b,
          t2c,
          t2d,
          t2h,
          t2i
HAVING t2b IS NOT NULL
UNION DISTINCT
SELECT t2a,
       t2b,
       t2c,
       t2d,
       t2h,
       t2i
FROM   t2
WHERE  t2d IN (SELECT min(t1d)
               FROM   t1
               WHERE  t2c = t1c);

-- TC 01.06
SELECT t2a,
       t2b,
       t2c,
       t2d
FROM   t2
WHERE  t2a IN (SELECT t1a
               FROM   t1
               WHERE  t1b = t2b AND
                      t1d < t2d)
INTERSECT
SELECT t2a,
       t2b,
       t2c,
       t2d
FROM   t2
WHERE  t2b IN (SELECT Max(t1b)
               FROM   t1
               WHERE  t2c = t1c)
EXCEPT
SELECT t2a,
       t2b,
       t2c,
       t2d
FROM   t2
WHERE  t2d IN (SELECT Min(t3d)
               FROM   t3
               WHERE  t2c = t3c)
UNION ALL
SELECT t2a,
       t2b,
       t2c,
       t2d
FROM   t2
WHERE  t2c IN (SELECT Max(t1c)
               FROM   t1
               WHERE t1d = t2d);

-- UNION, UNION ALL, UNION DISTINCT, INTERSECT and EXCEPT in the subquery
-- TC 01.07
SELECT DISTINCT(t1a),
       t1b,
       t1c,
       t1d
FROM   t1
WHERE  t1a IN (SELECT t3a
               FROM   (SELECT t2a t3a
                       FROM   t2
                       UNION ALL
                       SELECT t2a t3a
                       FROM   t2) AS t3
               UNION
               SELECT t2a
               FROM   (SELECT t2a
                       FROM   t2
                       WHERE  t2b > 6
                       UNION
                       SELECT t2a
                       FROM   t2
                       WHERE  t2b > 6) AS t4
               UNION DISTINCT
               SELECT t2a
               FROM   (SELECT t2a
                       FROM   t2
                       WHERE  t2b > 6
                       UNION DISTINCT
                       SELECT t1a
                       FROM   t1
                       WHERE  t1b > 6) AS t5)
GROUP BY t1a, t1b, t1c, t1d
HAVING t1c IS NOT NULL AND t1b IS NOT NULL
ORDER BY t1c DESC, t1a DESC;

-- TC 01.08
SELECT t1a,
       t1b,
       t1c
FROM   t1
WHERE  t1b IN (SELECT t2b
               FROM   (SELECT t2b
                       FROM   t2
                       WHERE  t2b > 6
                       INTERSECT
                       SELECT t1b
                       FROM   t1
                       WHERE  t1b > 6) AS t3
               WHERE  t2b = t1b);

-- TC 01.09
SELECT t1a,
       t1b,
       t1c
FROM   t1
WHERE  t1h IN (SELECT t2h
               FROM   (SELECT t2h
                       FROM   t2
                       EXCEPT
                       SELECT t3h
                       FROM   t3) AS t3)
ORDER BY t1b DESC NULLs first, t1c  DESC NULLs last;

-- UNION, UNION ALL, UNION DISTINCT, INTERSECT and EXCEPT in the parent and subquery
-- TC 01.10
SELECT t1a,
       t1b,
       t1c
FROM   t1
WHERE  t1b IN
       (
              SELECT t2b
              FROM   (
                            SELECT t2b
                            FROM   t2
                            WHERE  t2b > 6
                            INTERSECT
                            SELECT t1b
                            FROM   t1
                            WHERE  t1b > 6) AS t3)
UNION DISTINCT
SELECT t1a,
       t1b,
       t1c
FROM   t1
WHERE  t1b IN
       (
              SELECT t2b
              FROM   (
                            SELECT t2b
                            FROM   t2
                            WHERE  t2b > 6
                            EXCEPT
                            SELECT t1b
                            FROM   t1
                            WHERE  t1b > 6) AS t4
              WHERE  t2b = t1b)
ORDER BY t1c DESC NULLS last, t1a DESC;

-- TC 01.11
SELECT *
FROM   (SELECT *
        FROM   (SELECT *
                FROM   t2
                WHERE  t2h IN (SELECT t1h
                               FROM   t1
                               WHERE  t1a = t2a)
                UNION DISTINCT
                SELECT *
                FROM   t1
                WHERE  t1h IN (SELECT t3h
                               FROM   t3
                               UNION
                               SELECT t1h
                               FROM   t1)
                UNION
                SELECT *
                FROM   t3
                WHERE  t3a IN (SELECT t2a
                               FROM   t2
                               UNION ALL
                               SELECT t1a
                               FROM   t1
                               WHERE  t1b > 0)
               INTERSECT
               SELECT *
               FROM   T1
               WHERE  t1b IN (SELECT t3b
                              FROM   t3
                              UNION DISTINCT
                              SELECT t2b
                              FROM   t2
                               )
              EXCEPT
              SELECT *
              FROM   t2
              WHERE  t2h IN (SELECT t1i
                             FROM   t1)) t4
        WHERE  t4.t2b IN (SELECT Min(t3b)
                          FROM   t3
                          WHERE  t4.t2a = t3a));

-- UNION, UNION ALL, UNION DISTINCT, INTERSECT and EXCEPT for NOT IN
-- TC 01.12
SELECT t2a,
       t2b,
       t2c,
       t2i
FROM   (SELECT *
        FROM   t2
        WHERE  t2a NOT IN (SELECT t1a
                           FROM   t1
                           UNION
                           SELECT t3a
                           FROM   t3)
        UNION ALL
        SELECT *
        FROM   t2
        WHERE  t2a NOT IN (SELECT t1a
                           FROM   t1
                           INTERSECT
                           SELECT t2a
                           FROM   t2)) AS t3
WHERE  t3.t2a NOT IN (SELECT t1a
                      FROM   t1
                      INTERSECT
                      SELECT t2a
                      FROM   t2)
       AND t2c IS NOT NULL
ORDER  BY t2a;

-- TC 01.13
SELECT   Count(DISTINCT(t1a)),
         t1b,
         t1c,
         t1i
FROM     t1
WHERE    t1b NOT IN
         (
                SELECT t2b
                FROM   (
                              SELECT t2b
                              FROM   t2
                              WHERE  t2b NOT IN
                                     (
                                            SELECT t1b
                                            FROM   t1)
                              UNION
                              SELECT t1b
                              FROM   t1
                              WHERE  t1b NOT IN
                                     (
                                            SELECT t3b
                                            FROM   t3)
                              UNION
                                    distinct SELECT t3b
                              FROM   t3
                              WHERE  t3b NOT IN
                                     (
                                            SELECT t2b
                                            FROM   t2)) AS t3
                WHERE  t2b = t1b)
GROUP BY t1a,
         t1b,
         t1c,
         t1i
HAVING   t1b NOT IN
         (
                SELECT t2b
                FROM   t2
                WHERE  t2c IS NULL
                EXCEPT
                SELECT t3b
                FROM   t3)
ORDER BY t1c DESC NULLS LAST, t1i;

-- Correlated set ops inside IN - unsupported

SELECT *
FROM   t1
WHERE  t1a IN (SELECT t2a
               FROM   t2
               WHERE t2b = t1b
               UNION ALL
               SELECT t3a
               FROM   t3);

SELECT *
FROM   t1
WHERE  t1a IN (SELECT t2a
               FROM   t2
               WHERE t2b = t1b
               UNION DISTINCT
               SELECT t3a
               FROM   t3);

SELECT *
FROM   t1
WHERE  t1a IN (SELECT t2a
               FROM   t2
               WHERE t2b = t1b
               INTERSECT ALL
               SELECT t3a
               FROM   t3);

SELECT *
FROM   t1
WHERE  t1a IN (SELECT t2a
               FROM   t2
               WHERE t2b = t1b
               INTERSECT DISTINCT
               SELECT t3a
               FROM   t3);

SELECT *
FROM   t1
WHERE  t1a IN (SELECT t2a
               FROM   t2
               WHERE t2b = t1b
               EXCEPT ALL
               SELECT t3a
               FROM   t3);

SELECT *
FROM   t1
WHERE  t1a IN (SELECT t2a
               FROM   t2
               WHERE t2b = t1b
               EXCEPT DISTINCT
               SELECT t3a
               FROM   t3);

SELECT *
FROM   t1
WHERE  t1a NOT IN (SELECT t2a
               FROM   t2
               WHERE t2b = t1b
               UNION ALL
               SELECT t3a
               FROM   t3);

SELECT *
FROM   t1
WHERE  t1a NOT IN (SELECT t2a
               FROM   t2
               WHERE t2b = t1b
               UNION DISTINCT
               SELECT t3a
               FROM   t3);

SELECT *
FROM   t1
WHERE  t1a NOT IN (SELECT t2a
               FROM   t2
               WHERE t2b = t1b
               INTERSECT ALL
               SELECT t3a
               FROM   t3);

SELECT *
FROM   t1
WHERE  t1a NOT IN (SELECT t2a
               FROM   t2
               WHERE t2b = t1b
               INTERSECT DISTINCT
               SELECT t3a
               FROM   t3);

SELECT *
FROM   t1
WHERE  t1a NOT IN (SELECT t2a
               FROM   t2
               WHERE t2b = t1b
               EXCEPT ALL
               SELECT t3a
               FROM   t3);

SELECT *
FROM   t1
WHERE  t1a NOT IN (SELECT t2a
               FROM   t2
               WHERE t2b = t1b
               EXCEPT DISTINCT
               SELECT t3a
               FROM   t3);
