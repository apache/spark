-- Negative testcases for column resolution
CREATE DATABASE mydb1;
USE mydb1;
CREATE TABLE t1 USING parquet AS SELECT 1 AS i1;

CREATE DATABASE mydb2;
USE mydb2;
CREATE TABLE t1 USING parquet AS SELECT 20 AS i1;

-- Negative tests: column resolution scenarios with ambiguous cases in join queries
SET spark.sql.crossJoin.enabled = true;
USE mydb1;
SELECT i1 FROM t1, mydb1.t1;
SELECT t1.i1 FROM t1, mydb1.t1;
SELECT mydb1.t1.i1 FROM t1, mydb1.t1;
SELECT i1 FROM t1, mydb2.t1;
SELECT t1.i1 FROM t1, mydb2.t1;
USE mydb2;
SELECT i1 FROM t1, mydb1.t1;
SELECT t1.i1 FROM t1, mydb1.t1;
SELECT i1 FROM t1, mydb2.t1;
SELECT t1.i1 FROM t1, mydb2.t1;
SELECT db1.t1.i1 FROM t1, mydb2.t1;
SET spark.sql.crossJoin.enabled = false;

-- Negative tests
USE mydb1;
SELECT mydb1.t1 FROM t1;
SELECT t1.x.y.* FROM t1;
SELECT t1 FROM mydb1.t1;
USE mydb2;
SELECT mydb1.t1.i1 FROM t1;

-- reset
DROP DATABASE mydb1 CASCADE;
DROP DATABASE mydb2 CASCADE;
