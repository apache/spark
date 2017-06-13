-- Check name duplication in a regular case
CREATE TABLE t (c STRING, c INT) USING parquet;

-- Check multiple name duplication
CREATE TABLE t (c0 STRING, c1 INT, c1 DOUBLE, c0 INT) USING parquet;

-- Catch case-insensitive name duplication
SET spark.sql.caseSensitive=false;
CREATE TABLE t (ab STRING, cd INT, ef DOUBLE, Ab INT) USING parquet;
