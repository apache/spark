--IMPORT explain.sql

--SET spark.sql.adaptive.enabled=true
--SET spark.sql.maxMetadataStringLength = 500

-- Test tables
CREATE table  explain_temp1 USING PARQUET AS SELECT 1 as key, 2 as val;
CREATE table  explain_temp2 USING PARQUET AS SELECT 1 as key, 3 as val;

-- EXPLAIN FINAL
EXPLAIN FINAL
  SELECT *
  FROM   explain_temp1 a,
         explain_temp2 b
  WHERE  a.key = b.key;

EXPLAIN FINAL EXTENDED
  SELECT *
  FROM   explain_temp1 a,
         explain_temp2 b
  WHERE  a.key = b.key;

EXPLAIN FINAL FORMATTED
  SELECT *
  FROM   explain_temp1 a,
         explain_temp2 b
  WHERE  a.key = b.key;

EXPLAIN FINAL CODEGEN
  SELECT *
  FROM   explain_temp1 a,
         explain_temp2 b
  WHERE  a.key = b.key;

EXPLAIN FINAL COST
  SELECT *
  FROM   explain_temp1 a,
         explain_temp2 b
  WHERE  a.key = b.key;

-- negative test
EXPLAIN FINAL FORMATTED;

-- cleanup
DROP TABLE explain_temp1;
DROP TABLE explain_temp2;
