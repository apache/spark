set hive.metastore.warehouse.dir=invalid_scheme://${system:test.tmp.dir};

-- Tests that CTAS queries in non-default databases use the location of the database
-- not the hive.metastore.warehouse.dir for intermediate files (FileSinkOperator output).
-- If hive.metastore.warehouse.dir were used this would fail because the scheme is invalid.

CREATE DATABASE db1
LOCATION 'pfile://${system:test.tmp.dir}/db1';

USE db1;
EXPLAIN CREATE TABLE table_db1 AS SELECT * FROM default.src;
CREATE TABLE table_db1 AS SELECT * FROM default.src;

DESCRIBE FORMATTED table_db1;
