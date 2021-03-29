-- A test suite for scalar subquery in SELECT clause

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

-- Group 1: scalar subquery in SELECT clause
--          no correlation
-- TC 01.01
-- more than one scalar subquery
SELECT (SELECT min(t3d) FROM t3) min_t3d,
       (SELECT max(t2h) FROM t2) max_t2h
FROM   t1
WHERE  t1a = 'val1c';

-- TC 01.02
-- scalar subquery in an IN subquery
SELECT   t1a, count(*)
FROM     t1
WHERE    t1c IN (SELECT   (SELECT min(t3c) FROM t3)
                 FROM     t2
                 GROUP BY t2g
                 HAVING   count(*) > 1)
GROUP BY t1a;

-- TC 01.03
-- under a set op
SELECT (SELECT min(t3d) FROM t3) min_t3d,
       null
FROM   t1
WHERE  t1a = 'val1c'
UNION
SELECT null,
       (SELECT max(t2h) FROM t2) max_t2h
FROM   t1
WHERE  t1a = 'val1c';

-- TC 01.04
SELECT (SELECT min(t3c) FROM t3) min_t3d
FROM   t1
WHERE  t1a = 'val1a'
INTERSECT
SELECT (SELECT min(t2c) FROM t2) min_t2d
FROM   t1
WHERE  t1a = 'val1d';

-- TC 01.05
SELECT q1.t1a, q2.t2a, q1.min_t3d, q2.avg_t3d
FROM   (SELECT t1a, (SELECT min(t3d) FROM t3) min_t3d
        FROM   t1
        WHERE  t1a IN ('val1e', 'val1c')) q1
       FULL OUTER JOIN
       (SELECT t2a, (SELECT avg(t3d) FROM t3) avg_t3d
        FROM   t2
        WHERE  t2a IN ('val1c', 'val2a')) q2
ON     q1.t1a = q2.t2a
AND    q1.min_t3d < q2.avg_t3d;

-- Group 2: scalar subquery in SELECT clause
--          with correlation
-- TC 02.01
SELECT (SELECT min(t3d) FROM t3 WHERE t3.t3a = t1.t1a) min_t3d,
       (SELECT max(t2h) FROM t2 WHERE t2.t2a = t1.t1a) max_t2h
FROM   t1
WHERE  t1a = 'val1b';

-- TC 02.02
SELECT (SELECT min(t3d) FROM t3 WHERE t3a = t1a) min_t3d
FROM   t1
WHERE  t1a = 'val1b'
MINUS
SELECT (SELECT min(t3d) FROM t3) abs_min_t3d
FROM   t1
WHERE  t1a = 'val1b';

-- TC 02.03
SELECT t1a, t1b
FROM   t1
WHERE  NOT EXISTS (SELECT (SELECT max(t2b)
                           FROM   t2 LEFT JOIN t1
                           ON     t2a = t1a
                           WHERE  t2c = t3c) dummy
                   FROM   t3
                   WHERE  t3b < (SELECT max(t2b)
                                 FROM   t2 LEFT JOIN t1
                                 ON     t2a = t1a
                                 WHERE  t2c = t3c)
                   AND    t3a = t1a);

-- SPARK-34876: Non-nullable aggregates should not return NULL in a correlated subquery
SELECT t1a,
    (SELECT count(t2d) FROM t2 WHERE t2a = t1a) count_t2,
    (SELECT approx_count_distinct(t2d) FROM t2 WHERE t2a = t1a) approx_count_distinct_t2,
    (SELECT collect_list(t2d) FROM t2 WHERE t2a = t1a) collect_list_t2,
    (SELECT collect_set(t2d) FROM t2 WHERE t2a = t1a) collect_set_t2,
    (SELECT hex(count_min_sketch(t2d, 0.5d, 0.5d, 1)) FROM t2 WHERE t2a = t1a) collect_set_t2
FROM t1;