-- SPARK-56853: SQL Standard PATH golden file coverage.
-- Covers the SET PATH grammar, CURRENT_PATH() reflection, path-driven
-- routine/relation resolution, and the most common static error conditions.

--SET spark.sql.path.enabled=true

-- Default path (no SET PATH issued, no DEFAULT_PATH conf): the spark-builtin
-- default ordering with current_schema in the catalog slot.
SELECT current_path();

-- A literal SET PATH that pins both a user schema and system.builtin.
SET PATH = spark_catalog.default, system.builtin;
SELECT current_path();

-- DEFAULT_PATH restores the spark-builtin default ordering for the session.
SET PATH = DEFAULT_PATH;
SELECT current_path();

-- SYSTEM_PATH expands to the two system entries in the default order.
SET PATH = SYSTEM_PATH;
SELECT current_path();

-- The PATH keyword reuses the live path; a new entry can be appended.
SET PATH = spark_catalog.default, system.builtin;
SET PATH = PATH, system.session;
SELECT current_path();

-- current_schema / current_database expand to the live USE SCHEMA.
SET PATH = current_schema, system.builtin;
SELECT current_path();
SET PATH = current_database, system.builtin;
SELECT current_path();

-- ANSI keyword form (no parens) returns the same string as current_path().
SET PATH = spark_catalog.default, system.builtin;
SELECT CURRENT_PATH = current_path() AS same;

-- Routine resolution follows the path.
CREATE SCHEMA sql_path_routines;
CREATE FUNCTION sql_path_routines.pick() RETURNS INT RETURN 7;
SET PATH = spark_catalog.sql_path_routines, spark_catalog.default, system.builtin;
SELECT pick();
SET PATH = DEFAULT_PATH;
DROP FUNCTION sql_path_routines.pick;
DROP SCHEMA sql_path_routines;

-- Relation resolution follows the path (first-match wins).
CREATE SCHEMA sql_path_relations;
CREATE TABLE sql_path_relations.tbl USING parquet AS SELECT 42 AS id;
SET PATH = spark_catalog.sql_path_relations, spark_catalog.default, system.builtin;
SELECT id FROM tbl;
SET PATH = DEFAULT_PATH;
DROP TABLE sql_path_relations.tbl;
DROP SCHEMA sql_path_relations;

-- Static error cases ---------------------------------------------------------

-- Static duplicate literal at SET PATH time.
SET PATH = spark_catalog.default, spark_catalog.default;

-- DEFAULT_PATH already contains system.builtin; listing it again is a duplicate.
SET PATH = DEFAULT_PATH, system.builtin;

-- Single-part name in SET PATH is not a valid qualified schema reference.
SET PATH = my_schema_no_catalog;

-- current_path() takes no arguments.
SELECT current_path(1);

-- PATH disabled --------------------------------------------------------------
-- Flip the feature flag inline so the disabled behavior is exercised in the
-- same golden run.

SET spark.sql.path.enabled=false;

-- current_path() is still resolvable (it is a regular builtin).
SELECT current_path() IS NOT NULL AS has_path;

-- SET PATH itself is rejected with UNSUPPORTED_FEATURE.SET_PATH_WHEN_DISABLED.
SET PATH = spark_catalog.default;
