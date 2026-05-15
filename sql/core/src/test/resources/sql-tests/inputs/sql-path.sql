-- ============================================================================
-- SQL Standard PATH golden coverage
-- ============================================================================
--
-- This file is the readable, SQL-level reference for what the PATH feature
-- does. It is the primary place to look up "how does SET PATH behave when
-- I write ..." before reaching for the Scala unit suites. Tests that need
-- features not expressible in pure SQL (multi-threaded execution, session
-- cloning, view-metadata inspection, Connect/PySpark plumbing) live in the
-- matching Scala / Python suites.
--
-- Table of Contents
-- -----------------
--   1. Default path observability (no SET PATH issued)
--   2. SET PATH grammar
--      2.1 Literal schema entries; case preservation; backtick quoting
--      2.2 DEFAULT_PATH shortcut
--      2.3 SYSTEM_PATH shortcut
--      2.4 PATH keyword (append to live path)
--      2.5 current_schema / current_database shortcuts
--   3. CURRENT_PATH() builtin
--      3.1 ANSI no-parens form equals current_path()
--      3.2 Argument-count validation
--   4. Static error conditions at SET PATH
--      4.1 Literal duplicate
--      4.2 DEFAULT_PATH expansion duplicate
--      4.3 SYSTEM_PATH expansion duplicate
--      4.4 current_database vs current_schema cross-alias duplicate
--      4.5 Single-part schema reference rejected
--   5. Routine resolution via PATH
--      5.1 Persistent scalar function follows PATH
--      5.2 Persistent table function follows PATH
--      5.3 First-match ordering across two schemas on PATH
--      5.4 Unqualified miss when schema is not on PATH
--   6. Relation resolution via PATH
--      6.1 Table resolved via PATH; first-match ordering
--      6.2 Unqualified miss when schema is not on PATH
--   7. Persisted view frozen-path behavior
--      7.1 View body resolves via creation-time PATH (not invoker PATH)
--      7.2 current_schema / current_path in view body use invoker context
--   8. SQL function frozen-path behavior
--      8.1 Scalar function body resolves via creation-time PATH
--      8.2 Table function body resolves via creation-time PATH
--      8.3 current_schema / current_path in function body use invoker context
--   9. DEFAULT_PATH conf (spark.sql.defaultPath)
--      9.1 Explicit SET PATH overrides the conf
--      9.2 SET PATH = DEFAULT_PATH expands to the conf value
--      9.3 Invalid conf value rejected
--  10. PATH disabled
--      10.1 current_path() still resolves (regular builtin)
--      10.2 SET PATH itself is rejected
-- ============================================================================

--SET spark.sql.path.enabled=true


-- ============================================================================
-- 1. Default path observability (no SET PATH issued)
-- ============================================================================

-- The session was opened with PATH enabled and no `SET PATH` issued, so the
-- effective path is the spark-builtin default ordering with current_schema in
-- the catalog slot.
SELECT current_path();


-- ============================================================================
-- 2. SET PATH grammar
-- ============================================================================

-- 2.1 Literal schema entries; case preservation; backtick quoting -------------

SET PATH = spark_catalog.default, system.builtin;
SELECT current_path();

-- Case is preserved exactly as typed.
SET PATH = Spark_Catalog.Default, System.Builtin;
SELECT current_path();

-- Backtick-quoted identifiers that contain dots round-trip with quoting.
SET PATH = spark_catalog.`sch.b`, system.builtin;
SELECT current_path();

-- Multi-level namespace (3+ parts) is accepted by the grammar. The stored entry
-- is verified at the Scala layer (SetPathSuite) because the session catalog
-- only supports single-part namespaces, so calling current_path() while a
-- multi-level entry is on the path would surface that catalog limitation
-- rather than the PATH grammar property under test here.

SET PATH = DEFAULT_PATH;


-- 2.2 DEFAULT_PATH shortcut ---------------------------------------------------

SET PATH = DEFAULT_PATH;
SELECT current_path();


-- 2.3 SYSTEM_PATH shortcut ----------------------------------------------------

SET PATH = SYSTEM_PATH;
SELECT current_path();


-- 2.4 PATH keyword (append to live path) --------------------------------------

SET PATH = spark_catalog.default, system.builtin;
SET PATH = PATH, system.session;
SELECT current_path();


-- 2.5 current_schema / current_database shortcuts -----------------------------

USE spark_catalog.default;
SET PATH = current_schema, system.builtin;
SELECT current_path();

-- current_database is a SQL alias for current_schema.
SET PATH = current_database, system.builtin;
SELECT current_path();

SET PATH = DEFAULT_PATH;


-- ============================================================================
-- 3. CURRENT_PATH() builtin
-- ============================================================================

-- 3.1 ANSI no-parens form equals current_path() ------------------------------

SET PATH = spark_catalog.default, system.builtin;
SELECT CURRENT_PATH = current_path() AS ansi_form_matches;


-- 3.2 Argument-count validation ----------------------------------------------

SELECT current_path(1);

SET PATH = DEFAULT_PATH;


-- ============================================================================
-- 4. Static error conditions at SET PATH
-- ============================================================================

-- 4.1 Literal duplicate -------------------------------------------------------

SET PATH = spark_catalog.default, spark_catalog.default;

-- Case-insensitive duplicate is still flagged.
SET PATH = spark_catalog.DEFAULT, spark_catalog.default;


-- 4.2 DEFAULT_PATH expansion duplicate ----------------------------------------

-- DEFAULT_PATH already contains system.builtin; listing it again is a duplicate
-- after expansion.
SET PATH = DEFAULT_PATH, system.builtin;


-- 4.3 SYSTEM_PATH expansion duplicate -----------------------------------------

SET PATH = SYSTEM_PATH, SYSTEM_PATH;


-- 4.4 current_database vs current_schema cross-alias duplicate ----------------

SET PATH = current_database, current_schema;


-- 4.5 Single-part schema reference rejected -----------------------------------

SET PATH = my_schema_no_catalog;


-- ============================================================================
-- 5. Routine resolution via PATH
-- ============================================================================

-- 5.1 Persistent scalar function follows PATH ---------------------------------

CREATE SCHEMA sql_path_routines;
CREATE FUNCTION sql_path_routines.pick() RETURNS INT RETURN 7;
SET PATH = spark_catalog.sql_path_routines, spark_catalog.default, system.builtin;
SELECT pick();
SET PATH = DEFAULT_PATH;


-- 5.2 Persistent table function follows PATH ----------------------------------

CREATE FUNCTION sql_path_routines.pick_tvf()
RETURNS TABLE(val INT)
RETURN SELECT 7 AS val;
SET PATH = spark_catalog.sql_path_routines, spark_catalog.default, system.builtin;
SELECT * FROM pick_tvf();
SET PATH = DEFAULT_PATH;


-- 5.3 First-match ordering across two schemas on PATH ------------------------

CREATE SCHEMA sql_path_routines_b;
CREATE FUNCTION sql_path_routines_b.pick() RETURNS INT RETURN 11;

SET PATH = spark_catalog.sql_path_routines, spark_catalog.sql_path_routines_b, system.builtin;
SELECT pick() AS from_first_schema;
SET PATH = spark_catalog.sql_path_routines_b, spark_catalog.sql_path_routines, system.builtin;
SELECT pick() AS from_first_schema;
SET PATH = DEFAULT_PATH;


-- 5.4 Unqualified miss when schema is not on PATH -----------------------------

SET PATH = spark_catalog.default, system.builtin;
SELECT pick();

-- Cleanup section 5.
SET PATH = DEFAULT_PATH;
DROP FUNCTION sql_path_routines.pick;
DROP FUNCTION sql_path_routines.pick_tvf;
DROP FUNCTION sql_path_routines_b.pick;
DROP SCHEMA sql_path_routines;
DROP SCHEMA sql_path_routines_b;


-- ============================================================================
-- 6. Relation resolution via PATH
-- ============================================================================

CREATE SCHEMA sql_path_relations_a;
CREATE SCHEMA sql_path_relations_b;
CREATE TABLE sql_path_relations_a.tbl USING parquet AS SELECT 1 AS id;
CREATE TABLE sql_path_relations_b.tbl USING parquet AS SELECT 2 AS id;

-- 6.1 First-match ordering ----------------------------------------------------

SET PATH = spark_catalog.sql_path_relations_a, spark_catalog.sql_path_relations_b, system.builtin;
SELECT id FROM tbl AS from_first_schema;
SET PATH = spark_catalog.sql_path_relations_b, spark_catalog.sql_path_relations_a, system.builtin;
SELECT id FROM tbl AS from_first_schema;


-- 6.2 Unqualified miss when schema is not on PATH -----------------------------

SET PATH = spark_catalog.default, system.builtin;
SELECT id FROM tbl;

-- Cleanup section 6.
SET PATH = DEFAULT_PATH;
DROP TABLE sql_path_relations_a.tbl;
DROP TABLE sql_path_relations_b.tbl;
DROP SCHEMA sql_path_relations_a;
DROP SCHEMA sql_path_relations_b;


-- ============================================================================
-- 7. Persisted view frozen-path behavior
-- ============================================================================

CREATE SCHEMA sql_path_views_a;
CREATE SCHEMA sql_path_views_b;
CREATE TABLE sql_path_views_a.frozen_t USING parquet AS SELECT 1 AS id;
CREATE TABLE sql_path_views_b.frozen_t USING parquet AS SELECT 2 AS id;

-- 7.1 View body resolves via creation-time PATH (not invoker PATH) ------------

SET PATH = spark_catalog.sql_path_views_a, system.builtin;
CREATE VIEW default.v_path_frozen AS SELECT id FROM frozen_t;

-- Flip the live PATH; the view body's unqualified `frozen_t` must still
-- resolve through the schema captured at CREATE VIEW (sql_path_views_a, id=1).
-- A bare query against `frozen_t` from the session follows the LIVE PATH and
-- returns the other table's row (id=2).
SET PATH = spark_catalog.sql_path_views_b, system.builtin;
SELECT id FROM frozen_t AS bare_lookup_uses_live_path;
SELECT id FROM default.v_path_frozen AS view_body_uses_frozen_path;


-- 7.2 current_schema / current_path in view body use invoker context ----------

USE spark_catalog.sql_path_views_a;
CREATE VIEW sql_path_views_a.v_ctx AS
SELECT current_schema() AS cs, current_path() AS cp;

USE spark_catalog.sql_path_views_b;
SET PATH = DEFAULT_PATH;
-- The view body re-evaluates current_schema() / current_path() on every
-- invocation against the INVOKER's context, not the creator's. The result
-- here must reflect sql_path_views_b (the invoker), not sql_path_views_a
-- (the creator's schema at CREATE VIEW).
SELECT cs, cp FROM sql_path_views_a.v_ctx;

-- Cleanup section 7.
USE spark_catalog.default;
SET PATH = DEFAULT_PATH;
DROP VIEW default.v_path_frozen;
DROP VIEW sql_path_views_a.v_ctx;
DROP TABLE sql_path_views_a.frozen_t;
DROP TABLE sql_path_views_b.frozen_t;
DROP SCHEMA sql_path_views_a;
DROP SCHEMA sql_path_views_b;


-- ============================================================================
-- 8. SQL function frozen-path behavior
-- ============================================================================

CREATE SCHEMA sql_path_fn_a;
CREATE SCHEMA sql_path_fn_b;
CREATE TABLE sql_path_fn_a.frozen_t USING parquet AS SELECT 10 AS id;
CREATE TABLE sql_path_fn_b.frozen_t USING parquet AS SELECT 20 AS id;

-- 8.1 Scalar function body resolves via creation-time PATH --------------------

SET PATH = spark_catalog.sql_path_fn_a, system.builtin;
CREATE FUNCTION default.frozen_fn()
RETURNS INT
RETURN (SELECT MAX(id) FROM frozen_t);

SET PATH = spark_catalog.sql_path_fn_b, system.builtin;
SELECT MAX(id) FROM frozen_t AS bare_lookup_uses_live_path;
SELECT default.frozen_fn() AS scalar_body_uses_frozen_path;


-- 8.2 Table function body resolves via creation-time PATH ---------------------

SET PATH = spark_catalog.sql_path_fn_a, system.builtin;
CREATE FUNCTION default.frozen_tvf()
RETURNS TABLE(id INT)
RETURN SELECT MAX(id) AS id FROM frozen_t;

SET PATH = spark_catalog.sql_path_fn_b, system.builtin;
SELECT * FROM default.frozen_tvf() AS table_body_uses_frozen_path;


-- 8.3 current_schema / current_path in function body use invoker context -----

USE spark_catalog.sql_path_fn_a;
CREATE FUNCTION sql_path_fn_a.f_ctx()
RETURNS STRING
RETURN concat(current_schema(), '::', current_path());

USE spark_catalog.sql_path_fn_b;
SET PATH = DEFAULT_PATH;
-- Like 7.2: current_schema() / current_path() in a SQL function body bind to
-- the INVOKER's context, not the creator's.
SELECT sql_path_fn_a.f_ctx() AS invoker_context;

-- Cleanup section 8.
USE spark_catalog.default;
SET PATH = DEFAULT_PATH;
DROP FUNCTION default.frozen_fn;
DROP FUNCTION default.frozen_tvf;
DROP FUNCTION sql_path_fn_a.f_ctx;
DROP TABLE sql_path_fn_a.frozen_t;
DROP TABLE sql_path_fn_b.frozen_t;
DROP SCHEMA sql_path_fn_a;
DROP SCHEMA sql_path_fn_b;


-- ============================================================================
-- 9. DEFAULT_PATH conf (spark.sql.defaultPath)
-- ============================================================================
--
-- The conf's RHS is captured as a raw string by the SQL `SET key = value`
-- form; keywords like `current_schema` and shortcut tokens like `SYSTEM_PATH`
-- must be written WITHOUT backticks so the conf's SET-PATH-grammar validator
-- recognizes them as path tokens rather than 1-part quoted identifiers.

-- 9.1 Explicit SET PATH overrides the conf ------------------------------------

SET spark.sql.defaultPath = system.session, system.builtin;
SET PATH = system.builtin, system.session;
SELECT current_path() AS explicit_set_path_wins_over_conf;
SET PATH = DEFAULT_PATH;
RESET spark.sql.defaultPath;


-- 9.2 SET PATH = DEFAULT_PATH expands to the conf value -----------------------

SET spark.sql.defaultPath = system.session, system.builtin, current_schema;
USE spark_catalog.default;
SET PATH = DEFAULT_PATH;
SELECT current_path() AS default_path_expands_to_conf;
RESET spark.sql.defaultPath;
SET PATH = DEFAULT_PATH;


-- 9.3 Invalid conf value rejected at SET time ---------------------------------

SET spark.sql.defaultPath = this is not a path;

-- The PATH keyword is not allowed in the conf value (it would create a cycle).
SET spark.sql.defaultPath = PATH, system.builtin;


-- ============================================================================
-- 10. PATH disabled
-- ============================================================================

SET spark.sql.path.enabled = false;


-- 10.1 current_path() still resolves (regular builtin) ------------------------

SELECT current_path() IS NOT NULL AS has_path;


-- 10.2 SET PATH itself is rejected --------------------------------------------

SET PATH = spark_catalog.default;
