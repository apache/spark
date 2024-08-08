-- A test suite for scalar subquery in predicate context

--ONLY_IF spark
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
  ('val1a', 6S, 8, 10L, float(15.0), 20D, 20E2BD, timestamp '2014-04-04 00:00:00.000', date '2014-04-04'),
  ('val1b', 8S, 16, 19L, float(17.0), 25D, 26E2BD, timestamp '2014-05-04 01:01:00.000', date '2014-05-04'),
  ('val1a', 16S, 12, 21L, float(15.0), 20D, 20E2BD, timestamp '2014-06-04 01:02:00.001', date '2014-06-04'),
  ('val1a', 16S, 12, 10L, float(15.0), 20D, 20E2BD, timestamp '2014-07-04 01:01:00.000', date '2014-07-04'),
  ('val1c', 8S, 16, 19L, float(17.0), 25D, 26E2BD, timestamp '2014-05-04 01:02:00.001', date '2014-05-05'),
  ('val1d', null, 16, 22L, float(17.0), 25D, 26E2BD, timestamp '2014-06-04 01:01:00.000', null),
  ('val1d', null, 16, 19L, float(17.0), 25D, 26E2BD, timestamp '2014-07-04 01:02:00.001', null),
  ('val1e', 10S, null, 25L, float(17.0), 25D, 26E2BD, timestamp '2014-08-04 01:01:00.000', date '2014-08-04'),
  ('val1e', 10S, null, 19L, float(17.0), 25D, 26E2BD, timestamp '2014-09-04 01:02:00.001', date '2014-09-04'),
  ('val1d', 10S, null, 12L, float(17.0), 25D, 26E2BD, timestamp '2015-05-04 01:01:00.000', date '2015-05-04'),
  ('val1a', 6S, 8, 10L, float(15.0), 20D, 20E2BD, timestamp '2014-04-04 01:02:00.001', date '2014-04-04'),
  ('val1e', 10S, null, 19L, float(17.0), 25D, 26E2BD, timestamp '2014-05-04 01:01:00.000', date '2014-05-04')
  as t1(t1a, t1b, t1c, t1d, t1e, t1f, t1g, t1h, t1i);

create temporary view t2 as select * from values
  ('val2a', 6S, 12, 14L, float(15), 20D, 20E2BD, timestamp '2014-04-04 01:01:00.000', date '2014-04-04'),
  ('val1b', 10S, 12, 19L, float(17), 25D, 26E2BD, timestamp '2014-05-04 01:01:00.000', date '2014-05-04'),
  ('val1b', 8S, 16, 119L, float(17), 25D, 26E2BD, timestamp '2015-05-04 01:01:00.000', date '2015-05-04'),
  ('val1c', 12S, 16, 219L, float(17), 25D, 26E2BD, timestamp '2016-05-04 01:01:00.000', date '2016-05-04'),
  ('val1b', null, 16, 319L, float(17), 25D, 26E2BD, timestamp '2017-05-04 01:01:00.000', null),
  ('val2e', 8S, null, 419L, float(17), 25D, 26E2BD, timestamp '2014-06-04 01:01:00.000', date '2014-06-04'),
  ('val1f', 19S, null, 519L, float(17), 25D, 26E2BD, timestamp '2014-05-04 01:01:00.000', date '2014-05-04'),
  ('val1b', 10S, 12, 19L, float(17), 25D, 26E2BD, timestamp '2014-06-04 01:01:00.000', date '2014-06-04'),
  ('val1b', 8S, 16, 19L, float(17), 25D, 26E2BD, timestamp '2014-07-04 01:01:00.000', date '2014-07-04'),
  ('val1c', 12S, 16, 19L, float(17), 25D, 26E2BD, timestamp '2014-08-04 01:01:00.000', date '2014-08-05'),
  ('val1e', 8S, null, 19L, float(17), 25D, 26E2BD, timestamp '2014-09-04 01:01:00.000', date '2014-09-04'),
  ('val1f', 19S, null, 19L, float(17), 25D, 26E2BD, timestamp '2014-10-04 01:01:00.000', date '2014-10-04'),
  ('val1b', null, 16, 19L, float(17), 25D, 26E2BD, timestamp '2014-05-04 01:01:00.000', null)
  as t2(t2a, t2b, t2c, t2d, t2e, t2f, t2g, t2h, t2i);

create temporary view t3 as select * from values
  ('val3a', 6S, 12, 110L, float(15), 20D, 20E2BD, timestamp '2014-04-04 01:02:00.000', date '2014-04-04'),
  ('val3a', 6S, 12, 10L, float(15), 20D, 20E2BD, timestamp '2014-05-04 01:02:00.000', date '2014-05-04'),
  ('val1b', 10S, 12, 219L, float(17), 25D, 26E2BD, timestamp '2014-05-04 01:02:00.000', date '2014-05-04'),
  ('val1b', 10S, 12, 19L, float(17), 25D, 26E2BD, timestamp '2014-05-04 01:02:00.000', date '2014-05-04'),
  ('val1b', 8S, 16, 319L, float(17), 25D, 26E2BD, timestamp '2014-06-04 01:02:00.000', date '2014-06-04'),
  ('val1b', 8S, 16, 19L, float(17), 25D, 26E2BD, timestamp '2014-07-04 01:02:00.000', date '2014-07-04'),
  ('val3c', 17S, 16, 519L, float(17), 25D, 26E2BD, timestamp '2014-08-04 01:02:00.000', date '2014-08-04'),
  ('val3c', 17S, 16, 19L, float(17), 25D, 26E2BD, timestamp '2014-09-04 01:02:00.000', date '2014-09-05'),
  ('val1b', null, 16, 419L, float(17), 25D, 26E2BD, timestamp '2014-10-04 01:02:00.000', null),
  ('val1b', null, 16, 19L, float(17), 25D, 26E2BD, timestamp '2014-11-04 01:02:00.000', null),
  ('val3b', 8S, null, 719L, float(17), 25D, 26E2BD, timestamp '2014-05-04 01:02:00.000', date '2014-05-04'),
  ('val3b', 8S, null, 19L, float(17), 25D, 26E2BD, timestamp '2015-05-04 01:02:00.000', date '2015-05-04')
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

-- SPARK-44549: window function in the correlated subquery.
SELECT 1
FROM t1
WHERE t1b < (SELECT MAX(tmp.s) FROM (
             SELECT SUM(t2b) OVER (partition by t2c order by t2d) as s
               FROM t2 WHERE t2.t2d = t1.t1d) as tmp);

-- Same as above but with LIMIT/ORDER BY instead of MAX
SELECT 1
FROM t1
WHERE t1b < (SELECT SUM(t2b) OVER (partition by t2c order by t2d) as s
               FROM t2 WHERE t2.t2d = t1.t1d
             ORDER BY s DESC
             LIMIT 1);

-- SPARK-44549: window function in the correlated subquery with non-equi predicate.
SELECT 1
FROM t1
WHERE t1b < (SELECT MAX(tmp.s) FROM (
             SELECT SUM(t2b) OVER (partition by t2c order by t2d) as s
               FROM t2 WHERE t2.t2d <= t1.t1d) as tmp);

-- Same as above but with LIMIT/ORDER BY
SELECT 1
FROM t1
WHERE t1b < (SELECT SUM(t2b) OVER (partition by t2c order by t2d) as s
               FROM t2 WHERE t2.t2d <= t1.t1d
             ORDER BY s DESC
             LIMIT 1);

-- SPARK-44549: window function in the correlated subquery over joins.
SELECT t1b
FROM t1
WHERE t1b > (SELECT MAX(tmp.s) FROM (
             SELECT RANK() OVER (partition by t3c, t2b order by t3c) as s
               FROM t2, t3 where t2.t2c = t3.t3c AND t2.t2a = t1.t1a) as tmp);

-- SPARK-44549: window function in the correlated subquery over aggregation.
SELECT t1b
FROM t1
WHERE t1b > (SELECT MAX(tmp.s) FROM (
             SELECT RANK() OVER (partition by t3c, t3d order by t3c) as s
               FROM (SELECT t3b, t3c, max(t3d) as t3d FROM t3 GROUP BY t3b, t3c) as g) as tmp)
ORDER BY t1b;


-- SPARK-44549: correlation in window function itself is not supported yet.
SELECT 1
FROM t1
WHERE t1b = (SELECT MAX(tmp.s) FROM (
             SELECT SUM(t2c) OVER (partition by t2c order by t1.t1d + t2d) as s
               FROM t2) as tmp);

-- SPARK-36191: ORDER BY/LIMIT in the correlated subquery, equi-predicate
SELECT t1a, t1b
FROM   t1
WHERE  t1c = (SELECT t2c
              FROM   t2
              WHERE  t2b < t1b
              ORDER BY t2d LIMIT 1);


-- SPARK-36191: ORDER BY/LIMIT in the correlated subquery, non-equi-predicate
SELECT t1a, t1b
FROM   t1
WHERE  t1c = (SELECT t2c
              FROM   t2
              WHERE  t2c = t1c
              ORDER BY t2c LIMIT 1);

-- SPARK-46526: LIMIT over correlated predicate that references only the outer table.
SELECT t1a, t1b
FROM   t1
WHERE  t1c = (SELECT t2c
              FROM   t2
              WHERE  t1b < t1d
              ORDER BY t2c LIMIT 1);

SELECT t1a, t1b
FROM   t1
WHERE  t1c = (SELECT MAX(t2c)
              FROM   t2
              WHERE  t1b < t1d
              ORDER BY min(t2c) LIMIT 1);

SELECT t1a, t1b
FROM   t1
WHERE  t1c = (SELECT DISTINCT t2c
              FROM   t2
              WHERE  t1b < t1d
              ORDER BY t2c LIMIT 1);

-- Set operations in correlation path

CREATE OR REPLACE TEMP VIEW t0(t0a, t0b) AS VALUES (1, 1), (2, 0);
CREATE OR REPLACE TEMP VIEW t1(t1a, t1b, t1c) AS VALUES (1, 1, 3);
CREATE OR REPLACE TEMP VIEW t2(t2a, t2b, t2c) AS VALUES (1, 1, 5), (2, 2, 7);

SELECT * FROM t0 WHERE t0a <
(SELECT sum(c) FROM
  (SELECT t1c as c
  FROM   t1
  WHERE  t1a = t0a
  UNION ALL
  SELECT t2c as c
  FROM   t2
  WHERE  t2b = t0b)
);

SELECT * FROM t0 WHERE t0a <
(SELECT sum(c) FROM
  (SELECT t1c as c
  FROM   t1
  WHERE  t1a = t0a
  UNION ALL
  SELECT t2c as c
  FROM   t2
  WHERE  t2a = t0a)
);

SELECT * FROM t0 WHERE t0a <
(SELECT sum(c) FROM
  (SELECT t1c as c
  FROM   t1
  WHERE  t1a > t0a
  UNION ALL
  SELECT t2c as c
  FROM   t2
  WHERE  t2b <= t0b)
);

SELECT * FROM t0 WHERE t0a <
(SELECT sum(t1c) FROM
  (SELECT t1c
  FROM   t1
  WHERE  t1a = t0a
  UNION ALL
  SELECT t2c
  FROM   t2
  WHERE  t2b = t0b)
);

SELECT * FROM t0 WHERE t0a <
(SELECT sum(t1c) FROM
  (SELECT t1c
  FROM   t1
  WHERE  t1a = t0a
  UNION DISTINCT
  SELECT t2c
  FROM   t2
  WHERE  t2b = t0b)
);

-- Tests for column aliasing
SELECT * FROM t0 WHERE t0a <
(SELECT sum(t1a + 3 * t1b + 5 * t1c) FROM
  (SELECT t1c as t1a, t1a as t1b, t0a as t1c
  FROM   t1
  WHERE  t1a = t0a
  UNION ALL
  SELECT t0a as t2b, t2c as t1a, t0b as t2c
  FROM   t2
  WHERE  t2b = t0b)
);

-- Test handling of COUNT bug
SELECT * FROM t0 WHERE t0a <
(SELECT count(t1c) FROM
  (SELECT t1c
  FROM   t1
  WHERE  t1a = t0a
  UNION DISTINCT
  SELECT t2c
  FROM   t2
  WHERE  t2b = t0b)
);

-- Correlated references in project
SELECT * FROM t0 WHERE t0a <
(SELECT sum(d) FROM
  (SELECT t1a - t0a as d
  FROM   t1
  UNION ALL
  SELECT t2a - t0a as d
  FROM   t2)
);

-- Correlated references in aggregate - unsupported
SELECT * FROM t0 WHERE t0a <
(SELECT sum(d) FROM
  (SELECT sum(t0a) as d
  FROM   t1
  UNION ALL
  SELECT sum(t2a) + t0a as d
  FROM   t2)
);

-- In HAVING clause
SELECT t0a, t0b FROM t0
GROUP BY t0a, t0b
HAVING t0a <
(SELECT sum(c) FROM
  (SELECT t1c as c
  FROM   t1
  WHERE  t1a > t0a
  UNION ALL
  SELECT t2c as c
  FROM   t2
  WHERE  t2b <= t0b)
);

-- SPARK-43760: the result of the subquery can be NULL.
select *
from range(1, 3) t1
where (select t2.id c
       from range (1, 2) t2 where t1.id = t2.id
      ) is not null;

-- Correlated references in join predicates
SELECT * FROM t0 WHERE t0a <
(SELECT sum(t1c) FROM
  (SELECT t1c
   FROM   t1 JOIN t2 ON (t1a = t0a AND t2b = t1b))
);

SELECT * FROM t0 WHERE t0a <
(SELECT sum(t1c) FROM
  (SELECT t1c
   FROM   t1 JOIN t2 ON (t1a < t0a AND t2b >= t1b))
);

SELECT * FROM t0 WHERE t0a <
(SELECT sum(t1c) FROM
  (SELECT t1c
  FROM  t1 LEFT JOIN t2 ON (t1a = t0a AND t2b = t0b))
);

select *
from range(1, 3) t1
where (select t2.id c
       from range (1, 2) t2 where t1.id = t2.id
      ) between 1 and 2;

SELECT *
FROM t1
WHERE (SELECT max(t2c)
       FROM t2 WHERE t1b = t2b
      ) between 1 and 2;
