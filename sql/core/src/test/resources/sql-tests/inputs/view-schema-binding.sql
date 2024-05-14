-- This test suite checks that the WITH SCHEMA BINDING clause is correctly implemented

-- New view with schema binding
-- 1.a BINDING is persisted
DROP TABLE IF EXISTS t;
CREATE TABLE t(c1 INT NOT NULL) USING PARQUET;
CREATE OR REPLACE VIEW v WITH SCHEMA BINDING AS SELECT * FROM t;
SELECT * FROM v;
-- Baseline: v(c1 INT);
DESCRIBE EXTENDED v;

-- Widen the column c1 in t
DROP TABLE t;
CREATE TABLE t(c1 BIGINT NOT NULL) USING PARQUET;
-- The view should be invalidated, cannot upcast from BIGINT to INT
SELECT * FROM v;

-- The view still describes as v(c1 INT);
DESCRIBE EXTENDED v;

-- 1.b In BINDING views get invalidated if a column is lost
DROP TABLE IF EXISTS t;
CREATE TABLE t(c1 INT, c2 INT) USING PARQUET;
CREATE OR REPLACE VIEW v WITH SCHEMA BINDING AS SELECT * FROM t;
SELECT * FROM v;
-- Baseline: v(c1 INT, c2 INT);
DESCRIBE EXTENDED v;

-- Drop the column c2 from t
DROP TABLE t;
CREATE TABLE t(c1 INT) USING PARQUET;
-- The view should be invalidated, it lost a column
SELECT * FROM v;

-- The view still describes as v(c1 INT, c2 INT);
DESCRIBE EXTENDED v;

-- Test ALTER VIEW ... WITH SCHEMA BINDING
SET spark.sql.viewSchemaBindingMode=DISABLED;

DROP TABLE IF EXISTS t;
CREATE TABLE t(c1 INT NOT NULL) USING PARQUET;
CREATE OR REPLACE VIEW v AS SELECT * FROM t;
SELECT * FROM v;
-- Baseline: v(c1 INT);
-- There is no binding recorded
DESCRIBE EXTENDED v;

ALTER VIEW v WITH SCHEMA BINDING;
-- Baseline: v(c1 INT);
-- There is SCHEMA BINDING recorded
DESCRIBE EXTENDED v;

DROP TABLE t;
CREATE TABLE t(c1 BIGINT NOT NULL) USING PARQUET;
-- The view should be invalidated, cannot upcast from BIGINT to INT
SELECT * FROM v;

-- The view still describes as v(c1 INT);
DESCRIBE EXTENDED v;

-- 99 Cleanup
DROP VIEW IF EXISTS v;
DROP TABLE IF EXISTS t;
