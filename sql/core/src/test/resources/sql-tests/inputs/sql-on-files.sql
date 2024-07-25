-- Parquet
CREATE TABLE TEST_PARQUET USING PARQUET AS SELECT 1;
SELECT * FROM parquet.``;
SELECT * FROM parquet.`/file/not/found`;
SELECT * FROM parquet.`${spark.sql.warehouse.dir}/test_parquet`;
DROP TABLE TEST_PARQUET;

-- ORC
CREATE TABLE TEST_ORC USING ORC AS SELECT 1;
SELECT * FROM orc.``;
SELECT * FROM orc.`/file/not/found`;
SELECT * FROM orc.`${spark.sql.warehouse.dir}/test_orc`;
DROP TABLE TEST_ORC;

-- CSV
CREATE TABLE TEST_CSV USING CSV AS SELECT 1;
SELECT * FROM csv.``;
SELECT * FROM csv.`/file/not/found`;
SELECT * FROM csv.`${spark.sql.warehouse.dir}/test_csv`;
DROP TABLE TEST_CSV;

-- JSON
CREATE TABLE TEST_JSON USING JSON AS SELECT 1;
SELECT * FROM json.``;
SELECT * FROM json.`/file/not/found`;
SELECT * FROM json.`${spark.sql.warehouse.dir}/test_json`;
DROP TABLE TEST_JSON;
