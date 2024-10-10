CREATE TEMPORARY VIEW t1 AS SELECT * FROM VALUES (0, 0), (1, 1), (2, 2) AS t(c1, c2);
CREATE TEMPORARY VIEW t2 AS
WITH v as (
  SELECT c1 + c1 c3 FROM t1
)
SELECT SUM(c3) s FROM v;

CACHE TABLE cache_table
WITH
t2 AS (SELECT 1)
SELECT * FROM t2;

SELECT * FROM cache_table;

EXPLAIN EXTENDED SELECT * FROM cache_table;

-- Nested WithCTE
CACHE TABLE cache_nested_cte_table
WITH
v AS (
  SELECT c1 * c2 c3 from t1
)
SELECT SUM(c3) FROM v
EXCEPT
SELECT s FROM t2;

SELECT * FROM cache_nested_cte_table;

EXPLAIN EXTENDED SELECT * FROM cache_nested_cte_table;

-- Clean up
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
