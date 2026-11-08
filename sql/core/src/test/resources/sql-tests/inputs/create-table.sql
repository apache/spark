--SET spark.sql.sources.default=json
--SET spark.sql.catalog.testcat=org.apache.spark.sql.connector.catalog.InMemoryTableCatalog

-- CREATE TABLE behavior in the session catalog and a V2 catalog.
CREATE NAMESPACE testcat.ns;
CREATE TABLE ct_v1 (c INT, s STRING COMMENT 'x') USING parquet;
CREATE TABLE testcat.ns.ct_v2 (c INT, s STRING COMMENT 'x') USING parquet;

-- Omitting USING selects the configured default provider.
CREATE TABLE ct_default_provider (c INT);
SHOW CREATE TABLE ct_default_provider;

-- IF NOT EXISTS preserves the existing schema, even when a different schema is supplied.
CREATE TABLE IF NOT EXISTS ct_v1 (c INT) USING parquet;
CREATE TABLE IF NOT EXISTS testcat.ns.ct_v2 (c INT) USING parquet;
DESCRIBE ct_v1;
DESCRIBE testcat.ns.ct_v2;

-- Creating an existing table without IF NOT EXISTS fails in both catalogs.
CREATE TABLE ct_v1 (c INT) USING parquet;
CREATE TABLE testcat.ns.ct_v2 (c INT) USING parquet;

-- Duplicate column names, including case variants.
CREATE TABLE ct_dup (a INT, a STRING) USING parquet;
CREATE TABLE ct_dup (a INT, A STRING) USING parquet;

-- Nested types and nullability.
CREATE TABLE ct_nested (
  st STRUCT<b: INT, c: STRING>,
  m MAP<STRING, INT>,
  arr ARRAY<INT>,
  arr_st ARRAY<STRUCT<x: INT, y: STRING>>
) USING parquet;
DESCRIBE ct_nested;
-- With spark.sql.fileSource.insert.enforceNotNull=false (the default), V1 file-source tables
-- store a nullable schema, so SHOW CREATE TABLE renders c INT NOT NULL as c INT.
CREATE TABLE ct_not_null (c INT NOT NULL, s STRING) USING parquet;
SHOW CREATE TABLE ct_not_null;

-- Day-time interval columns in V1 and V2 tables.
CREATE TABLE ct_interval (i INTERVAL DAY) USING parquet;
CREATE TABLE testcat.ns.ct_interval (i INTERVAL DAY) USING parquet;
DESCRIBE ct_interval;
DESCRIBE testcat.ns.ct_interval;

-- Qualified names: a valid schema and a missing schema.
CREATE TABLE default.ct_qualified (c INT) USING parquet;
CREATE TABLE ct_no_such_schema.ct_x (c INT) USING parquet;

-- A named parameter supplies the CREATE TABLE identifier in dynamic SQL.
EXECUTE IMMEDIATE 'CREATE TABLE IDENTIFIER(:n) (c INT) USING parquet' USING 'ct_exec' AS n;
DESCRIBE ct_exec;

-- Unknown data source provider and reserved table property.
CREATE TABLE ct_bad_provider (c INT) USING no_such_provider;
CREATE TABLE ct_reserved (c INT) USING parquet TBLPROPERTIES ('provider' = 'parquet');

-- A table without an explicit collation inherits its schema's default collation.
CREATE SCHEMA ct_coll_schema DEFAULT COLLATION UTF8_LCASE;
CREATE TABLE ct_coll_schema.ct_coll (s STRING) USING parquet;
SHOW CREATE TABLE ct_coll_schema.ct_coll;

-- Session-catalog REPLACE with a non-V2 provider such as parquet is unsupported, even
-- when the table is missing. In a V2 catalog, REPLACE requires an existing table.
REPLACE TABLE ct_repl_missing (c INT) USING parquet;
REPLACE TABLE testcat.ns.ct_repl_missing (c INT) USING parquet;
REPLACE TABLE testcat.ns.ct_v2 (c INT, d INT) USING parquet;
DESCRIBE testcat.ns.ct_v2;
REPLACE TABLE ct_v1 (c INT) USING parquet;

-- Session-catalog parquet statements fail because parquet is not a V2 provider.
-- In a V2 catalog, CREATE OR REPLACE creates a missing table or replaces an existing one.
CREATE OR REPLACE TABLE ct_cor_missing (c INT) USING parquet;
CREATE OR REPLACE TABLE testcat.ns.ct_cor_missing (c INT) USING parquet;
DESCRIBE testcat.ns.ct_cor_missing;
CREATE OR REPLACE TABLE testcat.ns.ct_v2 (c INT, s STRING) USING parquet;
DESCRIBE testcat.ns.ct_v2;
CREATE TABLE ct_cor_parquet (c INT) USING parquet;
CREATE OR REPLACE TABLE ct_cor_parquet (c INT, d INT) USING parquet;

-- Cleanup.
DROP TABLE ct_v1;
DROP TABLE testcat.ns.ct_v2;
DROP TABLE ct_default_provider;
DROP TABLE ct_nested;
DROP TABLE ct_not_null;
DROP TABLE ct_interval;
DROP TABLE testcat.ns.ct_interval;
DROP TABLE default.ct_qualified;
DROP TABLE ct_exec;
DROP TABLE testcat.ns.ct_cor_missing;
DROP TABLE ct_cor_parquet;
DROP SCHEMA ct_coll_schema CASCADE;
DROP NAMESPACE testcat.ns;
