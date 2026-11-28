-- Partition DESCRIBE validates specs after resolving the table or view target.
CREATE TABLE describe_partition_table (id INT, part INT, sub STRING)
USING PARQUET
PARTITIONED BY (part, sub);
INSERT INTO describe_partition_table VALUES (1, 10, 'x');

CREATE TABLE describe_partition_unpartitioned (id INT) USING PARQUET;
CREATE VIEW describe_partition_view AS SELECT * FROM describe_partition_table;
CREATE TEMPORARY VIEW describe_partition_temp_view AS SELECT * FROM describe_partition_table;

-- Partition key order, target qualification and IDENTIFIER preserve the partition.
DESCRIBE TABLE describe_partition_table PARTITION (sub = 'x', part = 10);
DESCRIBE default.describe_partition_table PARTITION (part = 10, sub = 'x');
DESCRIBE IDENTIFIER('describe_partition_table') PARTITION (part = 10, sub = 'x');

-- String values are cast to partition column types, including leading zeros.
DESCRIBE EXTENDED describe_partition_table PARTITION (part = '10', sub = 'x');
DESCRIBE EXTENDED describe_partition_table PARTITION (part = '010', sub = 'x');
DESCRIBE describe_partition_table PARTITION (part = 'abc', sub = 'x');

-- Invalid specs distinguish key mismatches, absent partitions and parser errors.
DESCRIBE describe_partition_table PARTITION (part = 10, sub = 'x', id = 1);
DESCRIBE describe_partition_table PARTITION (unknown = 10, sub = 'x');
DESCRIBE describe_partition_table PARTITION (part, sub = 'x');
DESCRIBE describe_partition_table PARTITION (part = NULL, sub = 'x');
DESCRIBE describe_partition_table PARTITION (part = 10, part = 10, sub = 'x');
DESCRIBE describe_partition_unpartitioned PARTITION (id = 1);

-- Views reject partition DESCRIBE, including its AS JSON form.
DESCRIBE describe_partition_view PARTITION (unknown = 1);
DESCRIBE TABLE EXTENDED describe_partition_view PARTITION (part = 10, sub = 'x') AS JSON;
DESCRIBE TABLE EXTENDED describe_partition_temp_view PARTITION (part = 10, sub = 'x') AS JSON;

SET spark.sql.caseSensitive=true;
DESCRIBE describe_partition_table PARTITION (part = 10, sub = 'x');
DESCRIBE describe_partition_table PARTITION (PART = 10, SUB = 'x');
RESET spark.sql.caseSensitive;

-- A shadowing temporary view rejects partitions; qualification reaches the table.
CREATE TEMPORARY VIEW describe_partition_table AS SELECT 1 AS id, 10 AS part, 'x' AS sub;
DESCRIBE describe_partition_table PARTITION (part = 10, sub = 'x');
DESCRIBE spark_catalog.default.describe_partition_table PARTITION (part = 10, sub = 'x');
DROP VIEW describe_partition_table;

CREATE GLOBAL TEMPORARY VIEW describe_partition_global_temp_view AS SELECT 1 AS part;
DESCRIBE global_temp.describe_partition_global_temp_view PARTITION (part = 1);
DROP VIEW global_temp.describe_partition_global_temp_view;

-- Dynamic execution and a variable-backed identifier use the same partition spec.
EXECUTE IMMEDIATE 'DESCRIBE describe_partition_table PARTITION (part = 10, sub = \'x\')';
DECLARE describe_partition_name STRING DEFAULT 'describe_partition_table';
DESCRIBE IDENTIFIER(describe_partition_name) PARTITION (part = 10, sub = 'x');
DROP TEMPORARY VARIABLE describe_partition_name;

-- Missing targets and a column combined with a partition spec.
DESCRIBE describe_partition_missing_schema.describe_partition_table PARTITION (part = 10);
DESCRIBE describe_partition_missing PARTITION (part = 10);
DESCRIBE describe_partition_table PARTITION (part = 10, sub = 'x') id;

DROP VIEW describe_partition_temp_view;
DROP VIEW describe_partition_view;
DROP TABLE describe_partition_unpartitioned;
DROP TABLE describe_partition_table;
