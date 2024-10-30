CREATE DATABASE IF NOT EXISTS sql_on_files;
-- Parquet
CREATE TABLE sql_on_files.test_parquet USING PARQUET AS SELECT 1;
SELECT * FROM parquet.``;
SELECT * FROM parquet.`file:tmp`;

SELECT * FROM parquet.`/file/not/found`;
SELECT * FROM parquet.`${spark.sql.warehouse.dir}/sql_on_files.db/test_parquet`;
DROP TABLE sql_on_files.test_parquet;

-- ORC
CREATE TABLE sql_on_files.test_orc USING ORC AS SELECT 1;
SELECT * FROM orc.``;
SELECT * FROM orc.`file:tmp`;
SELECT * FROM orc.`/file/not/found`;
SELECT * FROM orc.`${spark.sql.warehouse.dir}/sql_on_files.db/test_orc`;
DROP TABLE sql_on_files.test_orc;

-- CSV
CREATE TABLE sql_on_files.test_csv USING CSV AS SELECT 1;
SELECT * FROM csv.``;
SELECT * FROM csv.`file:tmp`;
SELECT * FROM csv.`/file/not/found`;
SELECT * FROM csv.`${spark.sql.warehouse.dir}/sql_on_files.db/test_csv`;
DROP TABLE sql_on_files.test_csv;

-- JSON
CREATE TABLE sql_on_files.test_json USING JSON AS SELECT 1;
SELECT * FROM json.``;
SELECT * FROM json.`file:tmp`;
SELECT * FROM json.`/file/not/found`;
SELECT * FROM json.`${spark.sql.warehouse.dir}/sql_on_files.db/test_json`;
DROP TABLE sql_on_files.test_json;

DROP DATABASE sql_on_files;
