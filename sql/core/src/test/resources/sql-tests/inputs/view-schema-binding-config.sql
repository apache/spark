-- This test suits check the spark.sql.viewSchemaBindingMode configuration.
-- It can be DISABLED and COMPENSATION

-- Verify the default binding is true
SET spark.sql.legacy.viewSchemaBindingMode;

-- 1. Test DISABLED mode.
SET spark.sql.legacy.viewSchemaBindingMode = false;

-- 1.a Attempts to use the SCHEMA BINDING clause fail with FEATURE_NOT_ENABLED
CREATE OR REPLACE VIEW v WITH SCHEMA BINDING AS SELECT 1;
CREATE OR REPLACE VIEW v WITH SCHEMA COMPENSATION AS SELECT 1;
CREATE OR REPLACE VIEW v WITH SCHEMA TYPE EVOLUTION AS SELECT 1;
CREATE OR REPLACE VIEW v WITH SCHEMA EVOLUTION AS SELECT 1;

-- 1.b Existing SHOW and DESCRIBE should behave as before Spark 4.0.0
CREATE OR REPLACE VIEW v AS SELECT 1;
DESCRIBE EXTENDED v;
SHOW TABLE EXTENDED LIKE 'v';
SHOW CREATE TABLE v;
DROP VIEW IF EXISTS v;

CREATE OR REPLACE TEMPORARY VIEW v AS SELECT 1;
DESCRIBE EXTENDED v;
SHOW TABLE EXTENDED LIKE 'v';
DROP VIEW IF EXISTS v;

-- 1.c Views get invalidated if the types change in an unsafe matter
DROP TABLE IF EXISTS t;
CREATE TABLE t(c1 INT NOT NULL) USING PARQUET;
CREATE OR REPLACE VIEW v AS SELECT * FROM t;
SELECT * FROM v;
-- Baseline: v(c1 INT);
DESCRIBE EXTENDED v;
SHOW CREATE TABLE v;

-- Widen the column c1 in t
DROP TABLE IF EXISTS t;
CREATE TABLE t(c1 BIGINT NOT NULL) USING PARQUET;
-- The view should be invalidated, cannot upcast from BIGINT to INT
SELECT * FROM v;

-- The view still describes as v(c1 INT);
DESCRIBE EXTENDED v;

-- 2. Test true mode. In this mode Spark tolerates any supported CAST, not just up cast
SET spark.sql.legacy.viewSchemaBindingMode = true;
SET spark.sql.legacy.viewSchemaCompensation = false;

-- To verify ANSI_MODE is enforced even if ANSI_MODE is turned off.
SET spark.sql.ansi.enabled = false;

-- 2.a In BINDING views get invalidated if the type can't cast
DROP TABLE IF EXISTS t;
CREATE TABLE t(c1 INT NOT NULL) USING PARQUET;
CREATE OR REPLACE VIEW v AS SELECT * FROM t;
SELECT * FROM v;
-- Baseline: v(c1 INT);
DESCRIBE EXTENDED v;
SHOW CREATE TABLE v;

-- Widen the column c1 in t
DROP TABLE t;
CREATE TABLE t(c1 BIGINT NOT NULL) USING PARQUET;
INSERT INTO t VALUES (1);

-- This fails
SELECT * FROM v;
-- The view still describes as v(c1 BIGINT)
DESCRIBE EXTENDED v;
SHOW CREATE TABLE v;

-- 2.b Switch to default COMPENSATION
SET spark.sql.legacy.viewSchemaCompensation = true;

DROP TABLE IF EXISTS t;
CREATE TABLE t(c1 INT NOT NULL) USING PARQUET;
CREATE OR REPLACE VIEW v AS SELECT * FROM t;
SELECT * FROM v;
-- Baseline: v(c1 INT);
DESCRIBE EXTENDED v;
SHOW CREATE TABLE v;

-- Widen the column c1 in t
DROP TABLE t;
CREATE TABLE t(c1 BIGINT NOT NULL) USING PARQUET;
INSERT INTO t VALUES (1);

-- This now succeeds
SELECT * FROM v;
-- The view still describes as v(c1 BIGINT)
DESCRIBE EXTENDED v;
SHOW CREATE TABLE v;

-- 2.c In COMPENSATION views ignore added columns and change the type
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

-- 2.d Still can't drop a column, though
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

-- 2.e Attempt to rename a column
DROP TABLE IF EXISTS t;
CREATE TABLE t(c3 INT NOT NULL, c2 INT) USING PARQUET;
SELECT * FROM v;
DESCRIBE EXTENDED v;

-- 99 Cleanup
DROP VIEW IF EXISTS v;
DROP TABLE IF EXISTS t;
