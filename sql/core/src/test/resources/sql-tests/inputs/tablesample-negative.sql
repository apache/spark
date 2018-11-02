-- Negative testcases for tablesample
CREATE DATABASE mydb1;
USE mydb1;
CREATE TABLE t1 USING parquet AS SELECT 1 AS i1;

-- Negative tests: negative percentage
SELECT mydb1.t1 FROM t1 TABLESAMPLE (-1 PERCENT);

-- Negative tests:  percentage over 100
-- The TABLESAMPLE clause samples without replacement, so the value of PERCENT must not exceed 100
SELECT mydb1.t1 FROM t1 TABLESAMPLE (101 PERCENT);

-- reset
DROP DATABASE mydb1 CASCADE;
