create temporary view t as select * from values 0, 1, 2 as t(id);
create temporary view t2 as select * from values 0, 1 as t(id);

-- WITH clause should not fall into infinite loop by referencing self
WITH s AS (SELECT 1 FROM s) SELECT * FROM s;

WITH r AS (SELECT (SELECT * FROM r))
SELECT * FROM r;

-- WITH clause should reference the base table
WITH t AS (SELECT 1 FROM t) SELECT * FROM t;

-- WITH clause should not allow cross reference
WITH s1 AS (SELECT 1 FROM s2), s2 AS (SELECT 1 FROM s1) SELECT * FROM s1, s2;

-- WITH clause should reference the previous CTE
WITH t1 AS (SELECT * FROM t2), t2 AS (SELECT 2 FROM t1) SELECT * FROM t1 cross join t2;

-- SPARK-18609 CTE with self-join
WITH CTE1 AS (
  SELECT b.id AS id
  FROM   T2 a
         CROSS JOIN (SELECT id AS id FROM T2) b
)
SELECT t1.id AS c1,
       t2.id AS c2
FROM   CTE1 t1
       CROSS JOIN CTE1 t2;

-- CTE with column alias
WITH t(x) AS (SELECT 1)
SELECT * FROM t WHERE x = 1;

-- CTE with multiple column aliases
WITH t(x, y) AS (SELECT 1, 2)
SELECT * FROM t WHERE x = 1 AND y = 2;

-- CTE with duplicate column aliases
WITH t(x, x) AS (SELECT 1, 2)
SELECT * FROM t;

-- CTE with empty column alias list is not allowed
WITH t() AS (SELECT 1)
SELECT * FROM t;

-- CTEs with duplicate names are not allowed
WITH
  t(x) AS (SELECT 1),
  t(x) AS (SELECT 2)
SELECT * FROM t;

-- Clean up
DROP VIEW IF EXISTS t;
DROP VIEW IF EXISTS t2;
