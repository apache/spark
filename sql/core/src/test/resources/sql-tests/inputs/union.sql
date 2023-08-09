CREATE OR REPLACE TEMPORARY VIEW t1 AS VALUES (1, 'a'), (2, 'b') tbl(c1, c2);
CREATE OR REPLACE TEMPORARY VIEW t2 AS VALUES (1.0, 1), (2.0, 4) tbl(c1, c2);

-- Simple Union
SELECT *
FROM   (SELECT * FROM t1
        UNION ALL
        SELECT * FROM t1);

-- Type Coerced Union
SELECT *
FROM   (SELECT * FROM t1
        UNION ALL
        SELECT * FROM t2
        UNION ALL
        SELECT * FROM t2);

-- Regression test for SPARK-18622
SELECT a
FROM (SELECT 0 a, 0 b
      UNION ALL
      SELECT SUM(1) a, CAST(0 AS BIGINT) b
      UNION ALL SELECT 0 a, 0 b) T;

-- Regression test for SPARK-18841 Push project through union should not be broken by redundant alias removal.
CREATE OR REPLACE TEMPORARY VIEW p1 AS VALUES 1 T(col);
CREATE OR REPLACE TEMPORARY VIEW p2 AS VALUES 1 T(col);
CREATE OR REPLACE TEMPORARY VIEW p3 AS VALUES 1 T(col);
SELECT 1 AS x,
       col
FROM   (SELECT col AS col
        FROM (SELECT p1.col AS col
              FROM   p1 CROSS JOIN p2
              UNION ALL
              SELECT col
              FROM p3) T1) T2;

-- SPARK-24012 Union of map and other compatible columns.
SELECT map(1, 2), 'str'
UNION ALL
SELECT map(1, 2, 3, NULL), 1;

-- SPARK-24012 Union of array and other compatible columns.
SELECT array(1, 2), 'str'
UNION ALL
SELECT array(1, 2, 3, NULL), 1;

-- SPARK-32638: corrects references when adding aliases in WidenSetOperationTypes
CREATE OR REPLACE TEMPORARY VIEW t3 AS VALUES (decimal(1)) tbl(v);
SELECT t.v FROM (
  SELECT v FROM t3
  UNION ALL
  SELECT v + v AS v FROM t3
) t;

SELECT SUM(t.v) FROM (
  SELECT v FROM t3
  UNION
  SELECT v + v AS v FROM t3
) t;

-- Clean-up
DROP VIEW IF EXISTS t1;
DROP VIEW IF EXISTS t2;
DROP VIEW IF EXISTS t3;
DROP VIEW IF EXISTS p1;
DROP VIEW IF EXISTS p2;
DROP VIEW IF EXISTS p3;
