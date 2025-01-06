CREATE TABLE t (a STRING, b INT, c STRING, d STRING) USING parquet
  OPTIONS (a '1', b '2', password 'password')
  PARTITIONED BY (c, d) CLUSTERED BY (a) SORTED BY (b ASC) INTO 2 BUCKETS
  COMMENT 'table_comment'
  TBLPROPERTIES (t 'test', password 'password');

CREATE TEMPORARY VIEW temp_v AS SELECT * FROM t;

CREATE TEMPORARY VIEW temp_Data_Source_View
  USING org.apache.spark.sql.sources.DDLScanSource
  OPTIONS (
    From '1',
    To '10',
    Table 'test1');

CREATE VIEW v AS SELECT * FROM t;

ALTER TABLE t SET TBLPROPERTIES (e = '3');

ALTER TABLE t ADD PARTITION (c='Us', d=1);

DESCRIBE t;

DESCRIBE EXTENDED t AS JSON;

-- AnalysisException: describe table as json must be extended
DESCRIBE t AS JSON;

-- AnalysisException: describe col as json unsupported
DESC FORMATTED t a AS JSON;

DESC default.t;

DESC TABLE t;

DESC FORMATTED t;

DESC EXTENDED t;

ALTER TABLE t UNSET TBLPROPERTIES (e);

DESC EXTENDED t;

ALTER TABLE t UNSET TBLPROPERTIES (comment);

DESC EXTENDED t;

DESC t PARTITION (c='Us', d=1);

DESC EXTENDED t PARTITION (c='Us', d=1) AS JSON;

DESC EXTENDED t PARTITION (c='Us', d=1);

DESC FORMATTED t PARTITION (c='Us', d=1);

DESC EXTENDED t PARTITION (C='Us', D=1);

-- NoSuchPartitionException: Partition not found in table
DESC t PARTITION (c='Us', d=2);

-- AnalysisException: Partition spec is invalid
DESC t PARTITION (c='Us');

-- ParseException: PARTITION specification is incomplete
DESC t PARTITION (c='Us', d);

-- DESC Temp View

DESC temp_v;

DESC TABLE temp_v;

DESC FORMATTED temp_v;

DESC EXTENDED temp_v;

DESC temp_Data_Source_View;

-- AnalysisException DESC PARTITION is not allowed on a temporary view
DESC temp_v PARTITION (c='Us', d=1);

-- DESC Persistent View

DESC v;

DESC TABLE v;

DESC FORMATTED v;

DESC EXTENDED v;

-- AnalysisException DESC PARTITION is not allowed on a view
DESC v PARTITION (c='Us', d=1);

-- Explain Describe Table
EXPLAIN DESC t;
EXPLAIN DESC EXTENDED t;
EXPLAIN EXTENDED DESC t;
EXPLAIN DESCRIBE t b;
EXPLAIN DESCRIBE t PARTITION (c='Us', d=2);
EXPLAIN DESCRIBE EXTENDED t PARTITION (c='Us', d=2) AS JSON;

-- DROP TEST TABLES/VIEWS
DROP TABLE t;

DROP VIEW temp_v;

DROP VIEW temp_Data_Source_View;

DROP VIEW v;

-- Show column default values
CREATE TABLE d (a STRING DEFAULT 'default-value', b INT DEFAULT 42) USING parquet COMMENT 'table_comment';

DESC d;

DESC EXTENDED d;

DESC TABLE EXTENDED d;

DESC FORMATTED d;

-- Show column default values with newlines in the string
CREATE TABLE e (a STRING DEFAULT CONCAT('a\n b\n ', 'c\n d'), b INT DEFAULT 42) USING parquet COMMENT 'table_comment';

DESC e;

DESC EXTENDED e;

DESC TABLE EXTENDED e;

DESC FORMATTED e;

