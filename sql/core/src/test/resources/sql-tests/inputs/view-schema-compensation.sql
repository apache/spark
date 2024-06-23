-- This test suite checks the WITH SCHEMA COMPENSATION clause
-- Disable ANSI mode to ensure we are forcing it explicitly in the CASTS
SET spark.sql.ansi.enabled = false;

-- In COMPENSATION views get invalidated if the type can't cast
DROP TABLE IF EXISTS t;
CREATE TABLE t(c1 INT NOT NULL) USING PARQUET;
CREATE OR REPLACE VIEW v WITH SCHEMA COMPENSATION AS SELECT * FROM t;
SELECT * FROM v;
-- Baseline: v(c1 INT);
DESCRIBE EXTENDED v;

-- Widen the column c1 in t
DROP TABLE t;
CREATE TABLE t(c1 BIGINT NOT NULL) USING PARQUET;
INSERT INTO t VALUES (1);
SELECT * FROM v;
-- The view still describes as v(c1 BIGINT)
DESCRIBE EXTENDED v;

-- In COMPENSATION views ignore added a column and change the type
-- Expect the added column to be ignore, but the type will be tolerated, as long as it can cast
DROP TABLE IF EXISTS t;
CREATE TABLE t(c1 STRING NOT NULL, c2 INT) USING PARQUET;
INSERT INTO t VALUES ('1', 2);
SELECT * FROM v;
-- The view still describes as v(c1 INT);
DESCRIBE EXTENDED v;

-- Runtime error if the cast fails
INSERT INTO t VALUES ('a', 2);
SELECT * FROM v;

-- Compile time error if the cast can't be done
DROP TABLE IF EXISTS t;
CREATE TABLE t(c1 MAP<STRING, STRING>, c2 INT) USING PARQUET;

-- The view should be invalidated, we can't cast a MAP to INT
SELECT * FROM v;

-- The view still describes as v(c1 INT);
DESCRIBE EXTENDED v;

-- Still can't drop a column, though
DROP TABLE IF EXISTS t;
CREATE TABLE t(c1 INT, c2 INT) USING PARQUET;
INSERT INTO t VALUES (1, 2);
CREATE OR REPLACE VIEW v AS SELECT * FROM t;
SELECT * FROM v;

-- Describes as v(c1 INT, c2 INT)
DESCRIBE EXTENDED v;

DROP TABLE IF EXISTS t;
CREATE TABLE t(c1 INT NOT NULL) USING PARQUET;

-- The view should be invalidated, it lost a column
SELECT * FROM v;
DESCRIBE EXTENDED v;

-- Attempt to rename a column, this fails
DROP TABLE IF EXISTS t;
CREATE TABLE t(c3 INT NOT NULL, c2 INT) USING PARQUET;
SELECT * FROM v;
DESCRIBE EXTENDED v;

-- Test ALTER VIEW ... WITH SCHEMA ...
DROP TABLE IF EXISTS t;
CREATE TABLE t(c1 INT) USING PARQUET;
INSERT INTO t VALUES(1);
CREATE OR REPLACE VIEW v WITH SCHEMA BINDING AS SELECT * FROM t;
DESCRIBE EXTENDED v;

DROP TABLE IF EXISTS t;
CREATE TABLE t(c1 STRING) USING PARQUET;
INSERT INTO t VALUES('1');

-- This fails, because teh view uses SCHEMA BINDING
SELECT * FROM v;

-- Now upgrade the view to schema compensation
ALTER VIEW v WITH SCHEMA COMPENSATION;
DESCRIBE EXTENDED v;

-- Success
SELECT * FROM v;

-- Cleanup
DROP VIEW IF EXISTS v;
DROP TABLE IF EXISTS t;
