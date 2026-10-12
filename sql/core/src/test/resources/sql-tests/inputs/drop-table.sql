-- DROP TABLE on an existing table.
CREATE TABLE drop_table_t1 (c INT) USING parquet;
DROP TABLE drop_table_t1;

-- DROP TABLE IF EXISTS on an existing and on a missing table.
CREATE TABLE drop_table_t2 (c INT) USING parquet;
DROP TABLE IF EXISTS drop_table_t2;
DROP TABLE IF EXISTS drop_table_missing;

-- DROP TABLE on a missing table.
DROP TABLE drop_table_missing;

-- DROP TABLE with a qualified name.
CREATE TABLE drop_table_t3 (c INT) USING parquet;
DROP TABLE default.drop_table_t3;

-- DROP TABLE with PURGE.
CREATE TABLE drop_table_t4 (c INT) USING parquet;
DROP TABLE drop_table_t4 PURGE;

-- DROP TABLE on a temporary view.
CREATE TEMPORARY VIEW drop_table_v1 AS SELECT 1 AS c;
DROP TABLE drop_table_v1;
DROP TABLE IF EXISTS drop_table_v1;

-- DROP TABLE with an IDENTIFIER clause.
CREATE TABLE drop_table_t6 (c INT) USING parquet;
DROP TABLE IDENTIFIER('drop_table_t6');

-- DROP TABLE via a v1 command.
SET spark.sql.legacy.useV1Command=true;
CREATE TABLE drop_table_t7 (c INT) USING parquet;
DROP TABLE drop_table_t7;
DROP TABLE IF EXISTS drop_table_missing;
CREATE TABLE drop_table_t8 (c INT) USING parquet;
DROP TABLE drop_table_t8 PURGE;
SET spark.sql.legacy.useV1Command=false;
