-- A test suite for scalar subquery in SELECT clause

--ONLY_IF spark
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
    (SELECT count_if(t2d > 0) FROM t2 WHERE t2a = t1a) count_if_t2,
    (SELECT approx_count_distinct(t2d) FROM t2 WHERE t2a = t1a) approx_count_distinct_t2,
    (SELECT collect_list(t2d) FROM t2 WHERE t2a = t1a) collect_list_t2,
    (SELECT sort_array(collect_set(t2d)) FROM t2 WHERE t2a = t1a) collect_set_t2,
    (SELECT hex(count_min_sketch(t2d, 0.5d, 0.5d, 1)) FROM t2 WHERE t2a = t1a) collect_set_t2
FROM t1;

-- SPARK-36028: Allow Project to host outer references in scalar subqueries
SELECT t1c, (SELECT t1c) FROM t1;
SELECT t1c, (SELECT t1c WHERE t1c = 8) FROM t1;
SELECT t1c, t1d, (SELECT c + d FROM (SELECT t1c AS c, t1d AS d)) FROM t1;
SELECT t1c, (SELECT SUM(c) FROM (SELECT t1c AS c)) FROM t1;
SELECT t1a, (SELECT SUM(t2b) FROM t2 JOIN (SELECT t1a AS a) ON t2a = a) FROM t1;

-- CTE in correlated scalar subqueries
CREATE OR REPLACE TEMPORARY VIEW t1 AS VALUES (0, 1), (1, 2) t1(c1, c2);
CREATE OR REPLACE TEMPORARY VIEW t2 AS VALUES (0, 2), (0, 3) t2(c1, c2);

-- Single row subquery
SELECT c1, (WITH t AS (SELECT 1 AS a) SELECT a + c1 FROM t) FROM t1;
-- Correlation in CTE.
SELECT c1, (WITH t AS (SELECT * FROM t2 WHERE c1 = t1.c1) SELECT SUM(c2) FROM t) FROM t1;
-- Multiple CTE definitions.
SELECT c1, (
    WITH t3 AS (SELECT c1 + 1 AS c1, c2 + 1 AS c2 FROM t2),
    t4 AS (SELECT * FROM t3 WHERE t1.c1 = c1)
    SELECT SUM(c2) FROM t4
) FROM t1;
-- Multiple CTE references.
SELECT c1, (
    WITH t AS (SELECT * FROM t2)
    SELECT SUM(c2) FROM (SELECT c1, c2 FROM t UNION SELECT c2, c1 FROM t) r(c1, c2)
    WHERE c1 = t1.c1
) FROM t1;
-- Reference CTE in both the main query and the subquery.
WITH v AS (SELECT * FROM t2)
SELECT * FROM t1 WHERE c1 > (
    WITH t AS (SELECT * FROM t2)
    SELECT COUNT(*) FROM v WHERE c1 = t1.c1 AND c1 > (SELECT SUM(c2) FROM t WHERE c1 = v.c1)
);
-- Single row subquery that references CTE in the main query.
WITH t AS (SELECT 1 AS a)
SELECT c1, (SELECT a FROM t WHERE a = c1) FROM t1;
-- Multiple CTE references with non-deterministic CTEs.
WITH
v1 AS (SELECT c1, c2, rand(0) c3 FROM t1),
v2 AS (SELECT c1, c2, rand(0) c4 FROM v1 WHERE c3 IN (SELECT c3 FROM v1))
SELECT c1, (
    WITH v3 AS (SELECT c1, c2, rand(0) c5 FROM t2)
    SELECT COUNT(*) FROM (
        SELECT * FROM v2 WHERE c1 > 0
        UNION SELECT * FROM v2 WHERE c2 > 0
        UNION SELECT * FROM v3 WHERE c2 > 0
    ) WHERE c1 = v1.c1
) FROM v1;

-- Multi-value subquery error
SELECT (SELECT a FROM (SELECT 1 AS a UNION ALL SELECT 2 AS a) t) AS b;

-- SPARK-36114: Support correlated non-equality predicates
CREATE OR REPLACE TEMP VIEW t1(c1, c2) AS (VALUES (0, 1), (1, 2));
CREATE OR REPLACE TEMP VIEW t2(c1, c2) AS (VALUES (0, 2), (0, 3));

-- Neumann example Q2
CREATE OR REPLACE TEMP VIEW students(id, name, major, year) AS (VALUES
    (0, 'A', 'CS', 2022),
    (1, 'B', 'CS', 2022),
    (2, 'C', 'Math', 2022));
CREATE OR REPLACE TEMP VIEW exams(sid, course, curriculum, grade, date) AS (VALUES
    (0, 'C1', 'CS', 4, 2020),
    (0, 'C2', 'CS', 3, 2021),
    (1, 'C1', 'CS', 2, 2020),
    (1, 'C2', 'CS', 1, 2021));

SELECT students.name, exams.course
FROM students, exams
WHERE students.id = exams.sid
  AND (students.major = 'CS' OR students.major = 'Games Eng')
  AND exams.grade >= (
        SELECT avg(exams.grade) + 1
        FROM exams
        WHERE students.id = exams.sid
           OR (exams.curriculum = students.major AND students.year > exams.date));

-- Correlated non-equality predicates
SELECT (SELECT min(c2) FROM t2 WHERE t1.c1 > t2.c1) FROM t1;
SELECT (SELECT min(c2) FROM t2 WHERE t1.c1 >= t2.c1 AND t1.c2 < t2.c2) FROM t1;

-- Correlated non-equality predicates with the COUNT bug.
SELECT (SELECT count(*) FROM t2 WHERE t1.c1 > t2.c1) FROM t1;

-- Correlated equality predicates that are not supported after SPARK-35080
SELECT c, (
    SELECT count(*)
    FROM (VALUES ('ab'), ('abc'), ('bc')) t2(c)
    WHERE t1.c = substring(t2.c, 1, 1)
) FROM (VALUES ('a'), ('b')) t1(c);

SELECT c, (
    SELECT count(*)
    FROM (VALUES (0, 6), (1, 5), (2, 4), (3, 3)) t1(a, b)
    WHERE a + b = c
) FROM (VALUES (6)) t2(c);

-- SPARK-43156: scalar subquery with Literal result like `COUNT(1) is null`
SELECT *, (SELECT count(1) is null FROM t2 WHERE t1.c1 = t2.c1) FROM t1;

select (select f from (select false as f, max(c2) from t1 where t1.c1 = t1.c1)) from t2;

-- SPARK-43596: handle IsNull when rewriting the domain join
set spark.sql.optimizer.optimizeOneRowRelationSubquery.alwaysInline=false;
WITH T AS (SELECT 1 AS a)
SELECT (SELECT sum(1) FROM T WHERE a = col OR upper(col)= 'Y')
FROM (SELECT null as col) as foo;
set spark.sql.optimizer.optimizeOneRowRelationSubquery.alwaysInline=true;

-- SPARK-43760: the result of the subquery can be NULL.
select * from (
 select t1.id c1, (
  select t2.id c from range (1, 2) t2
  where t1.id = t2.id  ) c2
 from range (1, 3) t1 ) t
where t.c2 is not null;

-- SPARK-43838: Subquery on single table with having clause
SELECT c1, c2, (SELECT count(*) cnt FROM t1 t2 WHERE t1.c1 = t2.c1 HAVING cnt = 0) FROM t1
