-- Test tables
CREATE table  explain_temp1 (key int, val int) USING PARQUET;
CREATE table  explain_temp2 (key int, val int) USING PARQUET;
CREATE table  explain_temp3 (key int, val int) USING PARQUET;

-- Simple explain
SET spark.sql.explain.legacy.format = false;
SET spark.sql.codegen.wholeStage = true;

-- single table
EXPLAIN
  SELECT KEY, Max(val) 
  FROM   explain_temp1 
  WHERE  KEY > 0 
  GROUP  BY KEY 
  ORDER  BY KEY; 

EXPLAIN
  SELECT key, Max(val)
  FROM explain_temp1
  WHERE key > 0
  GROUP BY key
  HAVING max(val) > 0;

-- simple union
EXPLAIN
  SELECT key, val FROM explain_temp1 WHERE key > 0
  UNION 
  SELECT key, val FROM explain_temp1 WHERE key > 0;

-- Join
EXPLAIN
  SELECT * 
  FROM   explain_temp1 a, 
         explain_temp2 b 
  WHERE  a.KEY = b.KEY; 

EXPLAIN
  SELECT * 
  FROM   explain_temp1 a 
         INNER JOIN explain_temp2 b 
                 ON a.KEY = b.KEY; 
EXPLAIN
  SELECT * 
  FROM   explain_temp1 a 
         LEFT OUTER JOIN explain_temp2 b 
                      ON a.KEY = b.KEY;

-- Subqueries
EXPLAIN
  SELECT * 
  FROM   explain_temp1 
  WHERE  KEY = (SELECT Max(KEY) 
                FROM   explain_temp2 
                WHERE  KEY = (SELECT Max(KEY) 
                              FROM   explain_temp3 
                              WHERE  val > 0) 
                       AND val = 2) 
         AND val > 3;

EXPLAIN
  SELECT * 
  FROM   explain_temp1 
  WHERE  KEY = (SELECT Max(KEY) 
                FROM   explain_temp2 
                WHERE  val > 0) 
         OR
         KEY = (SELECT Max(KEY) 
                FROM   explain_temp3
                WHERE  val > 0);

-- Reuse subquery
EXPLAIN
  SELECT (SELECT Avg(key) FROM explain_temp1) + (SELECT Avg(key) FROM explain_temp1)
  FROM explain_temp1;

-- CTE + ReuseExchange
EXPLAIN
  WITH cte1 AS (
    SELECT *
    FROM explain_temp1 
    WHERE key > 10
  )
  SELECT * FROM cte1 a, cte1 b WHERE a.key = b.key;

EXPLAIN
  WITH cte1 AS (
    SELECT key, max(val)
    FROM explain_temp1 
    WHERE key > 10
    GROUP BY key
  )
  SELECT * FROM cte1 a, cte1 b WHERE a.key = b.key;

   
-- cleanup
DROP TABLE explain_temp1;
DROP TABLE explain_temp2;
DROP TABLE explain_temp3;
