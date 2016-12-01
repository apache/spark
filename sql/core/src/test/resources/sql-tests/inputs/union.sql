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

-- Clean-up
DROP VIEW IF EXISTS t1;
DROP VIEW IF EXISTS t2;
