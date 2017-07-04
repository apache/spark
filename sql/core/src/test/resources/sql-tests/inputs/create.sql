-- Catch case-sensitive name duplication
SET spark.sql.caseSensitive=true;

CREATE TABLE t(c0 STRING, c1 INT, c1 DOUBLE, c0 INT) USING parquet;

CREATE TABLE t(c0 INT) USING parquet PARTITIONED BY (c0, c0);

CREATE TABLE t(c0 INT) USING parquet CLUSTERED BY (c0, c0) INTO 2 BUCKETS;

CREATE TABLE t(c0 INT, c1 INT) USING parquet CLUSTERED BY (c0) SORTED BY (c1, c1) INTO 2 BUCKETS;

-- Catch case-insensitive name duplication
SET spark.sql.caseSensitive=false;

CREATE TABLE t(c0 STRING, c1 INT, c1 DOUBLE, c0 INT) USING parquet;

CREATE TABLE t(ab STRING, cd INT, ef DOUBLE, Ab INT) USING parquet;

CREATE TABLE t(ab INT, cd INT) USING parquet PARTITIONED BY (Ab, aB);

CREATE TABLE t(ab INT) USING parquet CLUSTERED BY (Ab, aB) INTO 2 BUCKETS;

CREATE TABLE t(ab INT, cd INT) USING parquet CLUSTERED BY (ab) SORTED BY (cD, Cd) INTO 2 BUCKETS;
