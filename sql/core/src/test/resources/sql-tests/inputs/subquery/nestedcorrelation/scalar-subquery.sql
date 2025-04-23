set spark.sql.optimizer.supportNestedCorrelatedSubqueries.enabled=true;
set spark.sql.optimizer.supportNestedCorrelatedSubqueriesForScalarSubqueries.enabled=true;

-- One test case for each following type:
-- ---------------------------------------------------------------
--   | SubqueryPosition | VulnerableToCountBug | TriggerDomainJoin|
-- ---------------------------------------------------------------
-- 1 | Filter           | False                | True             |
-- ---------------------------------------------------------------
-- 2 | Filter           | False                | False            |
-- ---------------------------------------------------------------
-- 3 | Filter           | True                 | True             |
-- ---------------------------------------------------------------
-- 4 | Filter           | True                 | False            |
-- ---------------------------------------------------------------
-- 5 | Project          | False                | True             |
-- ---------------------------------------------------------------
-- 6 | Project          | False                | False            |
-- ---------------------------------------------------------------
-- 7 | Project          | True                 | True             |
-- ---------------------------------------------------------------
-- 8 | Project          | True                 | False            |
-- ---------------------------------------------------------------
-- 9 | Aggregate        | False                | True             |
-- ---------------------------------------------------------------
-- 10| Aggregate        | False                | False            |
-- ---------------------------------------------------------------
-- 11| Aggregate        | True                 | True             |
-- ---------------------------------------------------------------
-- 12| Aggregate        | True                 | False            |
-- ---------------------------------------------------------------
-- 13| Filter(havingClause) | False            | True             |
-- ---------------------------------------------------------------
-- 14| Filter(havingClause) | False            | False            |
-- ---------------------------------------------------------------
-- 15| Filter(havingClause) | True             | True             |
-- ---------------------------------------------------------------
-- 16| Filter(havingClause) | True             | False            |
-- ---------------------------------------------------------------

DROP TABLE IF EXISTS myt1;
DROP TABLE IF EXISTS myt2;
DROP TABLE IF EXISTS myt3;
CREATE TABLE myt1(a INT, b INT, c INT);
CREATE TABLE myt2(a INT, b INT, c INT);
CREATE TABLE myt3(a INT, b INT, c INT);
INSERT INTO myt1 VALUES (0, 0, 0), (1, 1, 1), (2, 2, 2), (3, 3, 3), (NULL, NULL, NULL);
INSERT INTO myt2 VALUES (0, 0, 0), (1, 1, 1), (2, 2, 2), (3, 3, 3), (NULL, NULL, NULL);
INSERT INTO myt3 VALUES (0, 0, 0), (1, 1, 1), (2, 2, 2), (3, 3, 3), (NULL, NULL, NULL);

-- query 1
SELECT *
FROM myt1
WHERE myt1.a = (
  SELECT MAX(myt2.a)
  FROM myt2
  WHERE myt2.a = (
   SELECT MAX(myt3.a)
   FROM myt3
   WHERE myt3.b > myt2.b AND myt3.c > myt1.c
  ) AND myt2.b > myt1.b
);
-- query 2
SELECT *
FROM myt1
WHERE myt1.a = (
  SELECT MAX(myt2.a)
  FROM myt2
  WHERE myt2.a = (
   SELECT MAX(myt3.a)
   FROM myt3
   WHERE myt3.b = myt2.b AND myt3.c = myt1.c
  ) AND myt2.b = myt1.b
);
-- query 3
SELECT *
FROM myt1
WHERE myt1.a = (
  SELECT COUNT(myt2.a)
  FROM myt2
  WHERE myt2.a = (
   SELECT COUNT(myt3.a)
   FROM myt3
   WHERE myt3.b > myt2.b AND myt3.c > myt1.c
  ) AND myt2.b > myt1.b
);
-- query 4
SELECT *
FROM myt1
WHERE myt1.a = (
  SELECT COUNT(myt2.a)
  FROM myt2
  WHERE myt2.a = (
   SELECT COUNT(myt3.a)
   FROM myt3
   WHERE myt3.b = myt2.b AND myt3.c = myt1.c
  ) AND myt2.b = myt1.b
);
-- query 5
SELECT myt1.a, (
    SELECT MAX(myt2.a), (
            SELECT MAX(myt3.a)
            FROM myt3
            WHERE myt3.b > myt2.b AND myt3.c > myt1.c
        )
    FROM myt2
)
FROM myt1;
-- query 6
SELECT myt1.a, (
    SELECT MAX(myt2.a), (
            SELECT MAX(myt3.a)
            FROM myt3
            WHERE myt3.b = myt2.b AND myt3.c = myt1.c
        )
    FROM myt2
)
FROM myt1;
-- query 7
SELECT myt1.a, (
    SELECT COUNT(myt2.a), (
            SELECT COUNT(myt3.a)
            FROM myt3
            WHERE myt3.b > myt2.b AND myt3.c > myt1.c
        )
    FROM myt2
)
FROM myt1;
-- query 8
SELECT myt1.a, (
    SELECT COUNT(myt2.a), (
            SELECT COUNT(myt3.a)
            FROM myt3
            WHERE myt3.b = myt2.b AND myt3.c = myt1.c
        )
    FROM myt2
)
FROM myt1;
-- query 9
SELECT MIN(
        SELECT MAX(
            SELECT MAX(myt3.a)
            FROM myt3
            WHERE myt3.b > myt2.b AND myt3.c > myt1.c
        )
        FROM myt2
    )
)
FROM myt1;
-- query 10
SELECT MIN(
        SELECT MAX(
            SELECT MAX(myt3.a)
            FROM myt3
            WHERE myt3.b = myt2.b AND myt3.c = myt1.c
        )
        FROM myt2
    )
)
FROM myt1;
-- query 11
SELECT COUNT(
        SELECT COUNT(
            SELECT COUNT(myt3.a)
            FROM myt3
            WHERE myt3.b > myt2.b AND myt3.c > myt1.c
        )
        FROM myt2
    )
)
FROM myt1;
-- query 12
SELECT COUNT(
        SELECT COUNT(
            SELECT COUNT(myt3.a)
            FROM myt3
            WHERE myt3.b = myt2.b AND myt3.c = myt1.c
        )
        FROM myt2
    )
)
FROM myt1;
-- query 13
SELECT MAX(myt1.a)
FROM myt1
HAVING (
    SELECT MAX(myt2.a)
    FROM myt2
    WHERE myt2.a = (
        SELECT MAX(myt3.a)
        FROM myt3
        WHERE myt3.a > MAX(myt1.a)
    ) AND myt2.b > myt1.b
);
-- query 14
SELECT MAX(myt1.a)
FROM myt1
HAVING (
    SELECT MAX(myt2.a)
    FROM myt2
    WHERE myt2.a = (
        SELECT MAX(myt3.a)
        FROM myt3
        WHERE myt3.a = MAX(myt1.a)
    ) AND myt2.b = myt1.b
);
-- query 15
SELECT MAX(myt1.a)
FROM myt1
HAVING (
    SELECT COUNT(myt2.a)
    FROM myt2
    WHERE myt2.a = (
        SELECT COUNT(myt3.a)
        FROM myt3
        WHERE myt3.a > MAX(myt1.a)
    ) AND myt2.b > myt1.b
);
-- query 16
SELECT MAX(myt1.a)
FROM myt1
HAVING (
    SELECT COUNT(myt2.a)
    FROM myt2
    WHERE myt2.a = (
        SELECT COUNT(myt3.a)
        FROM myt3
        WHERE myt3.a = MAX(myt1.a)
    ) AND myt2.b = myt1.b
);

-- test that queries containing both nested correlated scalar subqueries
-- and other types of subqueries will be blocked by the analyzer when
-- we only support nested correlated scalar subqueries.
SELECT myt1.a
FROM myt1
WHERE EXISTS (
  SELECT 1
  FROM myt2
  WHERE myt2.a = (
    SELECT MAX(myt3.a)
    FROM myt3
    WHERE myt3.b > myt2.b AND myt3.c > myt1.c
  ) AND myt2.b > myt1.b
);

SELECT myt1.a
FROM myt1
WHERE myt1.b = (
  SELECT myt2.b
  FROM myt2
  WHERE EXISTS (
    SELECT 1
    FROM myt3
    WHERE myt3.b > myt2.b AND myt3.c > myt1.c
  ) AND myt2.b > myt1.b
);

-- testcases extracted from DUCKDB
SELECT 1 FROM (SELECT 1) t0(c0) WHERE (SELECT (SELECT c0)) = 1;

DROP TABLE IF EXISTS table_integers;
CREATE TABLE table_integers(i INTEGER);
INSERT INTO table_integers VALUES (1), (2), (3), (NULL);

SELECT i, (SELECT (SELECT 42+i1.i)+42+i1.i) AS j FROM table_integers i1 ORDER BY i;

SELECT i, (SELECT (SELECT (SELECT (SELECT 42+i1.i)++i1.i)+42+i1.i)+42+i1.i) AS j FROM table_integers i1 ORDER BY i;

SELECT i, (SELECT (SELECT (SELECT (SELECT i1.i+i1.i+i1.i+i1.i+i1.i)))) AS j FROM table_integers i1 ORDER BY i;

SELECT i, (SELECT (SELECT (SELECT (SELECT i1.i+i1.i+i1.i+i1.i+i1.i+i2.i) FROM table_integers i2 WHERE i2.i=i1.i))) AS j FROM table_integers i1 ORDER BY i;

SELECT i, (SELECT SUM(s1.i) FROM (SELECT i FROM table_integers WHERE i=i1.i) s1 LEFT OUTER JOIN table_integers s2 ON s1.i=s2.i) AS j FROM table_integers i1 ORDER BY i;

SELECT i, (SELECT SUM(s1.i) FROM (SELECT i FROM table_integers WHERE i<>i1.i) s1 LEFT OUTER JOIN table_integers s2 ON s1.i=s2.i) AS j FROM table_integers i1 ORDER BY i;

SELECT i, (SELECT SUM(ss2.i) FROM (SELECT i FROM table_integers s1 WHERE i=i1.i) ss2) AS j FROM table_integers i1 ORDER BY i;

SELECT i, (SELECT * FROM (SELECT (SELECT 42+i1.i)) s1) AS j FROM table_integers i1 ORDER BY i;

SELECT i, (SELECT s1.k+s2.k FROM (SELECT (SELECT 42+i1.i) AS k) s1, (SELECT (SELECT 42+i1.i) AS k) s2) AS j FROM table_integers i1 ORDER BY i;

SELECT i, (SELECT s1.k+s2.k FROM (SELECT (SELECT 42+i1.i) AS k) s1 LEFT OUTER JOIN (SELECT (SELECT 42+i1.i) AS k) s2 ON s1.k=s2.k) AS j FROM table_integers i1 ORDER BY i;

SELECT i, (SELECT i1.i IN (1, 2, 3, 4, 5, 6, 7, 8)) AS j FROM table_integers i1 ORDER BY i;