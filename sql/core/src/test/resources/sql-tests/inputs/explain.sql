-- Test tables
CREATE table  explain_temp1 (key int, val int) USING PARQUET;
CREATE table  explain_temp2 (key int, val int) USING PARQUET;
CREATE table  explain_temp3 (key int, val int) USING PARQUET;

SET spark.sql.codegen.wholeStage = true;

-- single table
EXPLAIN FORMATTED
  SELECT key, max(val) 
  FROM   explain_temp1 
  WHERE  key > 0 
  GROUP  BY key 
  ORDER  BY key; 

EXPLAIN FORMATTED
  SELECT key, max(val)
  FROM explain_temp1
  WHERE key > 0
  GROUP BY key
  HAVING max(val) > 0;

-- simple union
EXPLAIN FORMATTED
  SELECT key, val FROM explain_temp1 WHERE key > 0
  UNION 
  SELECT key, val FROM explain_temp1 WHERE key > 0;

-- Join
EXPLAIN FORMATTED
  SELECT * 
  FROM   explain_temp1 a, 
         explain_temp2 b 
  WHERE  a.key = b.key; 

EXPLAIN FORMATTED
  SELECT * 
  FROM   explain_temp1 a 
         LEFT OUTER JOIN explain_temp2 b 
                      ON a.key = b.key;

-- Subqueries nested.
EXPLAIN FORMATTED
  SELECT * 
  FROM   explain_temp1 
  WHERE  key = (SELECT max(key) 
                FROM   explain_temp2 
                WHERE  key = (SELECT max(key) 
                              FROM   explain_temp3 
                              WHERE  val > 0) 
                       AND val = 2) 
         AND val > 3;

EXPLAIN FORMATTED
  SELECT * 
  FROM   explain_temp1 
  WHERE  key = (SELECT max(key) 
                FROM   explain_temp2 
                WHERE  val > 0) 
         OR
         key = (SELECT max(key) 
                FROM   explain_temp3
                WHERE  val > 0);

-- Reuse subquery
EXPLAIN FORMATTED
  SELECT (SELECT Avg(key) FROM explain_temp1) + (SELECT Avg(key) FROM explain_temp1)
  FROM explain_temp1;

-- CTE + ReuseExchange
EXPLAIN FORMATTED
  WITH cte1 AS (
    SELECT *
    FROM explain_temp1 
    WHERE key > 10
  )
  SELECT * FROM cte1 a, cte1 b WHERE a.key = b.key;

EXPLAIN FORMATTED
  WITH cte1 AS (
    SELECT key, max(val)
    FROM explain_temp1 
    WHERE key > 10
    GROUP BY key
  )
  SELECT * FROM cte1 a, cte1 b WHERE a.key = b.key;

-- A spark plan which has innerChildren other than subquery
EXPLAIN FORMATTED
  CREATE VIEW explain_view AS
    SELECT key, val FROM explain_temp1;

-- cleanup
DROP TABLE explain_temp1;
DROP TABLE explain_temp2;
DROP TABLE explain_temp3;
