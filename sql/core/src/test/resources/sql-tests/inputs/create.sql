-- Catch case-sensitive name duplication
SET spark.sql.caseSensitive=true;

CREATE TABLE t (c0 STRING, c1 INT, c1 DOUBLE, c0 INT) USING parquet;

-- Catch case-insensitive name duplication
SET spark.sql.caseSensitive=false;

CREATE TABLE t (c0 STRING, c1 INT, c1 DOUBLE, c0 INT) USING parquet;

CREATE TABLE t (ab STRING, cd INT, ef DOUBLE, Ab INT) USING parquet;
