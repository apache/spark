--SET spark.sql.catalog.testcat=org.apache.spark.sql.connector.catalog.InMemoryTableCatalog

-- DESCRIBE target resolution for tables, persistent views and temporary views.
CREATE TABLE describe_target_table (id INT COMMENT 'identifier', data STRING)
USING PARQUET
COMMENT 'describe target table'
TBLPROPERTIES ('describe.target.key' = 'value');

CREATE VIEW describe_target_view (id COMMENT 'view identifier', data)
COMMENT 'describe target view'
AS SELECT id, data FROM describe_target_table;

CREATE TEMPORARY VIEW describe_target_temp_view AS SELECT id, data FROM describe_target_table;
CREATE GLOBAL TEMPORARY VIEW describe_target_global_temp_view AS SELECT 1 AS id, 'a' AS data;

-- Catalog qualification and case-insensitive lookup reach the same table.
DESCRIBE default.describe_target_table;
DESCRIBE spark_catalog.default.describe_target_table;
DESCRIBE `default`.`describe_target_table`;
DESCRIBE DESCRIBE_TARGET_TABLE;
DESCRIBE TABLE EXTENDED spark_catalog.default.describe_target_table AS JSON;

-- Persistent, local temporary and global temporary view metadata.
DESCRIBE TABLE EXTENDED describe_target_view AS JSON;
DESCRIBE TABLE EXTENDED describe_target_temp_view AS JSON;
DESCRIBE global_temp.describe_target_global_temp_view;
DESCRIBE EXTENDED global_temp.describe_target_global_temp_view;
DESCRIBE TABLE EXTENDED global_temp.describe_target_global_temp_view AS JSON;

-- A persistent schema does not qualify a local temporary view, and a global temporary
-- view requires its global_temp qualifier.
DESCRIBE default.describe_target_temp_view;
DESCRIBE global_temp.describe_target_temp_view;
DESCRIBE describe_target_global_temp_view;

-- Missing tables, schemas, catalogs and an unsupported multipart namespace.
DESCRIBE describe_target_missing;
DESCRIBE EXTENDED default.describe_target_missing;
DESCRIBE TABLE EXTENDED describe_target_missing AS JSON;
DESCRIBE describe_target_missing_schema.describe_target_table;
DESCRIBE describe_target_missing_catalog.default.describe_target_table;
DESCRIBE global_temp.describe_target_missing;
DESCRIBE TABLE spark_catalog.default.describe_target_table.extra;
DESCRIBE TABLE EXTENDED spark_catalog.default.describe_target_table.extra AS JSON;

-- A local temporary view shadows a table only for an unqualified name.
CREATE TEMPORARY VIEW describe_target_table AS SELECT 'shadow' AS shadow_column;
DESCRIBE describe_target_table;
DESCRIBE TABLE EXTENDED describe_target_table AS JSON;
DESCRIBE default.describe_target_table;
DESCRIBE spark_catalog.default.describe_target_table;
DROP VIEW describe_target_table;

-- The same precedence applies when the persistent object is a view.
CREATE TEMPORARY VIEW describe_target_view AS SELECT 'shadow' AS shadow_column;
DESCRIBE describe_target_view;
DESCRIBE default.describe_target_view;
DROP VIEW describe_target_view;

-- Unqualified names follow the current schema; local temporary views stay visible.
CREATE SCHEMA describe_target_schema;
CREATE TABLE describe_target_schema.describe_target_table (schema_id BIGINT) USING PARQUET;
USE describe_target_schema;
DESCRIBE describe_target_table;
DESCRIBE default.describe_target_table;
DESCRIBE describe_target_temp_view;
DESCRIBE describe_target_view;
USE default;
DROP TABLE describe_target_schema.describe_target_table;
DROP SCHEMA describe_target_schema;

-- Case-sensitive lookup applies to tables and both kinds of temporary view.
SET spark.sql.caseSensitive=true;
DESCRIBE describe_target_table;
DESCRIBE DESCRIBE_TARGET_TABLE;
DESCRIBE TABLE EXTENDED DESCRIBE_TARGET_TABLE AS JSON;
DESCRIBE DESCRIBE_TARGET_TEMP_VIEW;
DESCRIBE global_temp.DESCRIBE_TARGET_GLOBAL_TEMP_VIEW;
RESET spark.sql.caseSensitive;

-- Table-level DESCRIBE uses stored view metadata, while column DESCRIBE resolves
-- the view query and therefore fails after its base table is dropped.
CREATE TABLE describe_target_dropped_base (id INT) USING PARQUET;
CREATE VIEW describe_target_dropped_view AS SELECT id FROM describe_target_dropped_base;
DROP TABLE describe_target_dropped_base;
DESCRIBE describe_target_dropped_view;
DESCRIBE EXTENDED describe_target_dropped_view;
DESCRIBE TABLE EXTENDED describe_target_dropped_view AS JSON;
DESCRIBE describe_target_dropped_view id;
DROP VIEW describe_target_dropped_view;

-- Dynamic SQL preserves target lookup and missing-target errors.
EXECUTE IMMEDIATE 'DESCRIBE TABLE describe_target_table';
-- JDBC sessions add owner metadata to JSON. Exercise dynamic JSON execution in a script;
-- the direct AS JSON cases above compare the rendered metadata.
--QUERY-DELIMITER-START
BEGIN
  EXECUTE IMMEDIATE 'DESCRIBE TABLE EXTENDED describe_target_table AS JSON';
END;
--QUERY-DELIMITER-END
EXECUTE IMMEDIATE 'DESCRIBE describe_target_temp_view';
EXECUTE IMMEDIATE 'DESCRIBE TABLE describe_target_missing';

-- IDENTIFIER evaluates a session variable for tables, views and missing targets.
DECLARE describe_target_name STRING DEFAULT 'describe_target_table';
DESCRIBE IDENTIFIER(describe_target_name);
DESCRIBE TABLE EXTENDED IDENTIFIER(describe_target_name) AS JSON;
SET VARIABLE describe_target_name = 'describe_target_temp_view';
DESCRIBE IDENTIFIER(describe_target_name);
SET VARIABLE describe_target_name = 'describe_target_missing';
DESCRIBE IDENTIFIER(describe_target_name);
DROP TEMPORARY VARIABLE describe_target_name;

-- DESCRIBE statements in SQL scripts, including a missing target.
--QUERY-DELIMITER-START
BEGIN
  DESCRIBE TABLE describe_target_table;
END;
--QUERY-DELIMITER-END

--QUERY-DELIMITER-START
BEGIN
  DESCRIBE TABLE EXTENDED describe_target_view AS JSON;
END;
--QUERY-DELIMITER-END

--QUERY-DELIMITER-START
BEGIN
  DESCRIBE TABLE describe_target_missing;
END;
--QUERY-DELIMITER-END

-- A V2 table can be described, but AS JSON requires a supported table type.
CREATE NAMESPACE testcat.describe_target_namespace;
CREATE TABLE testcat.describe_target_namespace.describe_target_table (id INT) USING foo;
DESCRIBE TABLE testcat.describe_target_namespace.describe_target_table;
DESCRIBE TABLE EXTENDED testcat.describe_target_namespace.describe_target_table AS JSON;
DROP TABLE testcat.describe_target_namespace.describe_target_table;
DROP NAMESPACE testcat.describe_target_namespace;

DROP VIEW global_temp.describe_target_global_temp_view;
DROP VIEW describe_target_temp_view;
DROP VIEW describe_target_view;
DROP TABLE describe_target_table;
