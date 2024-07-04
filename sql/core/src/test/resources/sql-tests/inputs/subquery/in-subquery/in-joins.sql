-- A test suite for IN JOINS in parent side, subquery, and both predicate subquery
-- It includes correlated cases.

-- There are 2 dimensions we want to test
--  1. run with broadcast hash join, sort merge join or shuffle hash join.
--  2. run with whole-stage-codegen, operator codegen or no codegen.

--CONFIG_DIM1 spark.sql.autoBroadcastJoinThreshold=10485760
--CONFIG_DIM1 spark.sql.autoBroadcastJoinThreshold=-1,spark.sql.join.preferSortMergeJoin=true
--CONFIG_DIM1 spark.sql.autoBroadcastJoinThreshold=-1,spark.sql.join.forceApplyShuffledHashJoin=true

--CONFIG_DIM2 spark.sql.codegen.wholeStage=true
--CONFIG_DIM2 spark.sql.codegen.wholeStage=false,spark.sql.codegen.factoryMode=CODEGEN_ONLY
--CONFIG_DIM2 spark.sql.codegen.wholeStage=false,spark.sql.codegen.factoryMode=NO_CODEGEN

--CONFIG_DIM3 spark.sql.optimizeNullAwareAntiJoin=true
--CONFIG_DIM3 spark.sql.optimizeNullAwareAntiJoin=false

--ONLY_IF spark
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

create temporary view s1 as select * from values
    (1), (3), (5), (7), (9)
  as s1(id);

create temporary view s2 as select * from values
    (1), (3), (4), (6), (9)
  as s2(id);

create temporary view s3 as select * from values
    (3), (4), (6), (9)
  as s3(id);

-- correlated IN subquery
-- different JOIN in parent side
-- TC 01.01
SELECT t1a, t1b, t1c, t3a, t3b, t3c
FROM   t1 natural JOIN t3
WHERE  t1a IN (SELECT t2a
               FROM   t2
               WHERE t1a = t2a)
       AND t1b = t3b
       AND t1a = t3a
ORDER  BY t1a,
          t1b,
          t1c DESC nulls first;

-- TC 01.02
SELECT    Count(DISTINCT(t1a)),
          t1b,
          t3a,
          t3b,
          t3c
FROM      t1 natural left JOIN t3
WHERE     t1a IN
          (
                 SELECT t2a
                 FROM   t2
                 WHERE t1d = t2d)
AND       t1b > t3b
GROUP BY  t1a,
          t1b,
          t3a,
          t3b,
          t3c
ORDER BY  t1a DESC, t3b DESC, t3c ASC;

-- TC 01.03
SELECT     Count(DISTINCT(t1a))
FROM       t1 natural right JOIN t3
WHERE      t1a IN
           (
                  SELECT t2a
                  FROM   t2
                  WHERE  t1b = t2b)
AND        t1d IN
           (
                  SELECT t2d
                  FROM   t2
                  WHERE  t1c > t2c)
AND        t1a = t3a
GROUP BY   t1a
ORDER BY   t1a;

-- TC 01.04
SELECT          t1a,
                t1b,
                t1c,
                t3a,
                t3b,
                t3c
FROM            t1 FULL OUTER JOIN t3
where           t1a IN
                (
                       SELECT t2a
                       FROM   t2
                       WHERE t2c IS NOT NULL)
AND             t1b != t3b
AND             t1a = 'val1b'
ORDER BY        t1a;

-- TC 01.05
SELECT     Count(DISTINCT(t1a)),
           t1b
FROM       t1 RIGHT JOIN t3
where      t1a IN
           (
                  SELECT t2a
                  FROM   t2
                  WHERE  t2h > t3h)
AND        t3a IN
           (
                  SELECT t2a
                  FROM   t2
                  WHERE  t2c > t3c)
AND        t1h >= t3h
GROUP BY   t1a,
           t1b
HAVING     t1b > 8
ORDER BY   t1a;

-- TC 01.06
SELECT   Count(DISTINCT(t1a))
FROM     t1 LEFT OUTER
JOIN     t3
ON t1a = t3a
WHERE    t1a IN
         (
                SELECT t2a
                FROM   t2
                WHERE  t1h < t2h )
GROUP BY t1a
ORDER BY t1a;

-- TC 01.07
SELECT   Count(DISTINCT(t1a)),
         t1b
FROM     t1 INNER JOIN     t2
ON       t1a > t2a
WHERE    t1b IN
         (
                SELECT t2b
                FROM   t2
                WHERE  t2h > t1h)
OR       t1a IN
         (
                SELECT t2a
                FROM   t2
                WHERE  t2h < t1h)
GROUP BY t1b
HAVING   t1b > 6;

-- different JOIN in the subquery
-- TC 01.08
SELECT   Count(DISTINCT(t1a)),
         t1b
FROM     t1
WHERE    t1a IN
         (
                    SELECT     t2a
                    FROM       t2
                    JOIN t1
                    WHERE      t2b <> t1b)
AND      t1h IN
         (
                    SELECT     t2h
                    FROM       t2
                    RIGHT JOIN t3
                    where      t2b = t3b)
GROUP BY t1b
HAVING t1b > 8;

-- TC 01.09
SELECT   Count(DISTINCT(t1a)),
         t1b
FROM     t1
WHERE    t1a IN
         (
                    SELECT     t2a
                    FROM       t2
                    JOIN t1
                    WHERE      t2b <> t1b)
AND      t1h IN
         (
                    SELECT     t2h
                    FROM       t2
                    RIGHT JOIN t3
                    where      t2b = t3b)
AND       t1b IN
         (
                    SELECT     t2b
                    FROM       t2
                    FULL OUTER JOIN t3
                    where      t2b = t3b)

GROUP BY t1b
HAVING   t1b > 8;

-- JOIN in the parent and subquery
-- TC 01.10
SELECT     Count(DISTINCT(t1a)),
           t1b
FROM       t1
INNER JOIN t2 on t1b = t2b
RIGHT JOIN t3 ON t1a = t3a
where      t1a IN
           (
                           SELECT          t2a
                           FROM            t2
                           FULL OUTER JOIN t3
                           WHERE           t2b > t3b)
AND        t1c IN
           (
                           SELECT          t3c
                           FROM            t3
                           LEFT OUTER JOIN t2
                           ON              t3a = t2a )
AND        t1b IN
           (
                  SELECT t3b
                  FROM   t3 LEFT OUTER
                  JOIN   t1
                  WHERE  t3c = t1c)

AND        t1a = t2a
GROUP BY   t1b
ORDER BY   t1b DESC;

-- TC 01.11
SELECT    t1a,
          t1b,
          t1c,
          count(distinct(t2a)),
          t2b,
          t2c
FROM      t1
FULL JOIN t2  on t1a = t2a
RIGHT JOIN t3 on t1a = t3a
where     t1a IN
          (
                 SELECT t2a
                 FROM   t2 INNER
                 JOIN   t3
                 ON     t2b < t3b
                 WHERE  t2c IN
                        (
                               SELECT t1c
                               FROM   t1
                               WHERE  t1a = t2a))
and t1a = t2a
Group By t1a, t1b, t1c, t2a, t2b, t2c
HAVING t2c IS NOT NULL
ORDER By t2b DESC nulls last;


SELECT s1.id FROM s1
JOIN s2 ON s1.id = s2.id
AND s1.id IN (SELECT 9);


SELECT s1.id FROM s1
JOIN s2 ON s1.id = s2.id
AND s1.id NOT IN (SELECT 9);


-- IN with Subquery ON INNER JOIN
SELECT s1.id FROM s1
JOIN s2 ON s1.id = s2.id
AND s1.id IN (SELECT id FROM s3);


-- IN with Subquery ON LEFT SEMI JOIN
SELECT s1.id AS id2 FROM s1
LEFT SEMI JOIN s2
ON s1.id = s2.id
AND s1.id IN (SELECT id FROM s3);


-- IN with Subquery ON LEFT ANTI JOIN
SELECT s1.id as id2 FROM s1
LEFT ANTI JOIN s2
ON s1.id = s2.id
AND s1.id IN (SELECT id FROM s3);


-- IN with Subquery ON LEFT OUTER JOIN
SELECT s1.id, s2.id as id2 FROM s1
LEFT OUTER JOIN s2
ON s1.id = s2.id
AND s1.id IN (SELECT id FROM s3);


-- IN with Subquery ON RIGHT OUTER JOIN
SELECT s1.id, s2.id as id2 FROM s1
RIGHT OUTER JOIN s2
ON s1.id = s2.id
AND s1.id IN (SELECT id FROM s3);


-- IN with Subquery ON FULL OUTER JOIN
SELECT s1.id, s2.id AS id2 FROM s1
FULL OUTER JOIN s2
ON s1.id = s2.id
AND s1.id IN (SELECT id FROM s3);


-- NOT IN with Subquery ON INNER JOIN
SELECT s1.id FROM s1
JOIN s2 ON s1.id = s2.id
AND s1.id NOT IN (SELECT id FROM s3);


-- NOT IN with Subquery ON LEFT SEMI JOIN
SELECT s1.id AS id2 FROM s1
LEFT SEMI JOIN s2
ON s1.id = s2.id
AND s1.id NOT IN (SELECT id FROM s3);


-- NOT IN with Subquery ON LEFT ANTI JOIN
SELECT s1.id AS id2 FROM s1
LEFT ANTI JOIN s2
ON s1.id = s2.id
AND s1.id NOT IN (SELECT id FROM s3);


-- NOT IN with Subquery ON LEFT OUTER JOIN
SELECT s1.id, s2.id AS id2 FROM s1
LEFT OUTER JOIN s2
ON s1.id = s2.id
AND s1.id NOT IN (SELECT id FROM s3);


-- NOT IN with Subquery ON RIGHT OUTER JOIN
SELECT s1.id, s2.id AS id2 FROM s1
RIGHT OUTER JOIN s2
ON s1.id = s2.id
AND s1.id NOT IN (SELECT id FROM s3);


-- NOT IN with Subquery ON FULL OUTER JOIN
SELECT s1.id, s2.id AS id2 FROM s1
FULL OUTER JOIN s2
ON s1.id = s2.id
AND s1.id NOT IN (SELECT id FROM s3);


DROP VIEW s1;

DROP VIEW s2;

DROP VIEW s3;
