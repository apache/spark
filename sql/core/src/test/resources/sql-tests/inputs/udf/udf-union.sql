-- This test file was converted from union.sql.

CREATE OR REPLACE TEMPORARY VIEW t1 AS VALUES (1, 'a'), (2, 'b') tbl(c1, c2);
CREATE OR REPLACE TEMPORARY VIEW t2 AS VALUES (1.0, 1), (2.0, 4) tbl(c1, c2);

-- Simple Union
SELECT udf(c1) as c1, udf(c2) as c2
FROM   (SELECT udf(c1) as c1, udf(c2) as c2 FROM t1
        UNION ALL
        SELECT udf(c1) as c1, udf(c2) as c2 FROM t1);

-- Type Coerced Union
SELECT udf(c1) as c1, udf(c2) as c2
FROM   (SELECT udf(c1) as c1, udf(c2) as c2 FROM t1 WHERE c2 = 'a'
        UNION ALL
        SELECT udf(c1) as c1, udf(c2) as c2 FROM t2
        UNION ALL
        SELECT udf(c1) as c1, udf(c2) as c2 FROM t2);

-- Regression test for SPARK-18622
SELECT udf(udf(a)) as a
FROM (SELECT udf(0) a, udf(0) b
      UNION ALL
      SELECT udf(SUM(1)) a, udf(CAST(0 AS BIGINT)) b
      UNION ALL SELECT udf(0) a, udf(0) b) T;

-- Regression test for SPARK-18841 Push project through union should not be broken by redundant alias removal.
CREATE OR REPLACE TEMPORARY VIEW p1 AS VALUES 1 T(col);
CREATE OR REPLACE TEMPORARY VIEW p2 AS VALUES 1 T(col);
CREATE OR REPLACE TEMPORARY VIEW p3 AS VALUES 1 T(col);
SELECT udf(1) AS x,
       udf(col) as col
FROM   (SELECT udf(col) AS col
        FROM (SELECT udf(p1.col) AS col
              FROM   p1 CROSS JOIN p2
              UNION ALL
              SELECT udf(col)
              FROM p3) T1) T2;

-- SPARK-24012 Union of map and other compatible columns.
SELECT map(1, 2), udf('str') as str
UNION ALL
SELECT map(1, 2, 3, NULL), udf(1);

-- SPARK-24012 Union of array and other compatible columns.
SELECT array(1, 2), udf('str') as str
UNION ALL
SELECT array(1, 2, 3, NULL), udf(1);


-- Clean-up
DROP VIEW IF EXISTS t1;
DROP VIEW IF EXISTS t2;
DROP VIEW IF EXISTS p1;
DROP VIEW IF EXISTS p2;
DROP VIEW IF EXISTS p3;
