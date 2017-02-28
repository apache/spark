-- Scenario: column resolution scenarios with datasource table
CREATE DATABASE mydb1;
use mydb1;
CREATE TABLE t1 USING parquet as SELECT 1 as i1;

CREATE DATABASE mydb2;
use mydb2;
CREATE TABLE t1 USING parquet as SELECT 20 as i1;

USE mydb1;
SELECT i1 FROM t1;
SELECT i1 FROM mydb1.t1;
SELECT t1.i1 FROM t1;
SELECT t1.i1 FROM mydb1.t1;

-- TODO: Support this scenario
SELECT mydb1.t1.i1 FROM t1;
-- TODO: Support this scenario
SELECT mydb1.t1.i1 FROM mydb1.t1;

USE mydb2;
SELECT i1 FROM t1;
SELECT i1 FROM mydb1.t1;
SELECT t1.i1 FROM t1;
SELECT t1.i1 FROM mydb1.t1;
-- TODO: Support this scenario
SELECT mydb1.t1.i1 FROM mydb1.t1;

-- Scenario: resolve fully qualified table name in star expansion
use mydb1;
SELECT t1.* FROM t1;
SELECT mydb1.t1.* FROM mydb1.t1;
SELECT t1.* FROM mydb1.t1;
use mydb2;
SELECT t1.* FROM t1;
-- TODO: Support this scenario
SELECT mydb1.t1.* FROM mydb1.t1;
SELECT t1.* FROM mydb1.t1;
SELECT a.* FROM mydb1.t1 AS a;

-- Scenario: resolve in case of subquery

USE mydb1;
CREATE TABLE t3 USING parquet AS SELECT * FROM VALUES (4,1), (3,1) AS t3(c1, c2);
CREATE TABLE t4 USING parquet AS SELECT * FROM VALUES (4,1), (2,1) AS t4(c2, c3);

SELECT * FROM t3 WHERE c1 IN (SELECT c2 FROM t4 WHERE t4.c3 = t3.c2);

-- TODO: Support this scenario
SELECT * FROM mydb1.t3 WHERE c1 IN
  (SELECT mydb1.t4.c2 FROM mydb1.t4 WHERE mydb1.t4.c3 = mydb1.t3.c2);

-- Scenario: column resolution scenarios in join queries
set spark.sql.crossJoin.enabled = true;

-- TODO: Support this scenario
SELECT mydb1.t1.i1 FROM t1, mydb2.t1;

-- TODO: Support this scenario
SELECT mydb1.t1.i1 FROM mydb1.t1, mydb2.t1;

USE mydb2;
-- TODO: Support this scenario
SELECT mydb1.t1.i1 FROM t1, mydb1.t1;

-- Scenario: Table with struct column
USE mydb1;
CREATE TABLE t5(i1 INT, t5 STRUCT<i1:INT, i2:INT>) USING PARQUET;
INSERT INTO t5 VALUES(1, (2, 3));
SELECT t5.i1 FROM t5;
SELECT t5.t5.i1 FROM t5;
SELECT t5.t5.i1 FROM mydb1.t5;
SELECT t5.i1 FROM mydb1.t5;
-- TODO: Support this scenario
SELECT mydb1.t5.t5.i1 FROM mydb1.t5;
-- TODO: Support this scenario
SELECT mydb1.t5.t5.i2 from mydb1.t5;

set spark.sql.crossJoin.enabled = true;
USE default;
DROP DATABASE mydb1 CASCADE;
DROP DATABASE mydb2 CASCADE;
