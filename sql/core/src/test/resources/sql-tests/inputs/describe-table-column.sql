-- Column DESCRIBE resolves qualifiers, quoted names and view output columns.
CREATE TABLE describe_column_table (
  id INT COMMENT 'identifier',
  data STRING,
  s STRUCT<a: INT, b: STRING>,
  `dotted.name` INT,
  part INT)
USING PARQUET
PARTITIONED BY (part);

CREATE VIEW describe_column_view (id COMMENT 'view identifier', data, s)
AS SELECT id, data, s FROM describe_column_table;
CREATE TEMPORARY VIEW describe_column_temp_view AS SELECT id, data, s FROM describe_column_table;
CREATE GLOBAL TEMPORARY VIEW describe_column_global_temp_view
AS SELECT 1 AS id, named_struct('a', 1) AS s;

-- Comments, complex types, partition columns and quoted literal dots.
DESCRIBE describe_column_table id;
DESC describe_column_table data;
DESCRIBE TABLE describe_column_table s;
DESCRIBE describe_column_table part;
DESCRIBE describe_column_table ID;
DESCRIBE describe_column_table `dotted.name`;
DESCRIBE EXTENDED describe_column_table id;

-- Qualifiers must identify the target relation.
DESCRIBE describe_column_table describe_column_table.id;
DESCRIBE describe_column_table default.describe_column_table.id;
DESCRIBE describe_column_table spark_catalog.default.describe_column_table.id;
DESCRIBE default.describe_column_table describe_column_table.id;
DESCRIBE describe_column_table DESCRIBE_COLUMN_TABLE.id;
DESCRIBE describe_column_table other_table.id;

-- Nested fields are unsupported; an unquoted dotted column is a multipart name.
DESCRIBE describe_column_table s.a;
DESCRIBE describe_column_table describe_column_table.s.a;
DESCRIBE describe_column_table dotted.name;
DESCRIBE describe_column_table missing_column;
DESCRIBE describe_column_table _metadata;

-- Persistent and temporary views expose their output columns and comments.
DESCRIBE describe_column_view id;
DESCRIBE EXTENDED describe_column_view id;
DESCRIBE describe_column_view describe_column_view.data;
DESCRIBE describe_column_view s.a;
DESCRIBE describe_column_view missing_column;

DESCRIBE describe_column_temp_view id;
DESCRIBE EXTENDED describe_column_temp_view id;
DESCRIBE describe_column_temp_view describe_column_temp_view.data;
DESCRIBE describe_column_temp_view s.a;
DESCRIBE describe_column_temp_view missing_column;
DESCRIBE default.describe_column_temp_view id;

DESCRIBE global_temp.describe_column_global_temp_view id;
DESCRIBE global_temp.describe_column_global_temp_view describe_column_global_temp_view.id;
DESCRIBE global_temp.describe_column_global_temp_view
  global_temp.describe_column_global_temp_view.id;
DESCRIBE global_temp.describe_column_global_temp_view s.a;
DESCRIBE global_temp.describe_column_global_temp_view missing_column;

-- Target identifiers and missing targets are resolved before the column.
DESCRIBE IDENTIFIER('describe_column_table') id;
DESCRIBE describe_column_missing id;
DESCRIBE describe_column_missing_schema.describe_column_table id;
DESCRIBE spark_catalog.default.describe_column_table.extra id;

-- Case sensitivity affects column names and relation qualifiers.
SET spark.sql.caseSensitive=true;
DESCRIBE describe_column_table id;
DESCRIBE describe_column_table ID;
DESCRIBE describe_column_table DESCRIBE_COLUMN_TABLE.id;
DESCRIBE describe_column_view ID;
RESET spark.sql.caseSensitive;

-- Dynamic SQL and variable-backed target identifiers retain column semantics.
EXECUTE IMMEDIATE 'DESCRIBE describe_column_table id';
EXECUTE IMMEDIATE 'DESCRIBE EXTENDED describe_column_temp_view data';
EXECUTE IMMEDIATE 'DESCRIBE describe_column_table missing_column';
DECLARE describe_column_name STRING DEFAULT 'describe_column_table';
DESCRIBE IDENTIFIER(describe_column_name) id;
DESCRIBE IDENTIFIER(describe_column_name) s.a;
DROP TEMPORARY VARIABLE describe_column_name;

-- A local temporary view shadows the table for unqualified column DESCRIBE.
CREATE TEMPORARY VIEW describe_column_table AS SELECT 'shadow' AS shadow_column;
DESCRIBE describe_column_table shadow_column;
DESCRIBE describe_column_table id;
DESCRIBE default.describe_column_table id;
DROP VIEW describe_column_table;

DROP VIEW global_temp.describe_column_global_temp_view;
DROP VIEW describe_column_temp_view;
DROP VIEW describe_column_view;
DROP TABLE describe_column_table;
