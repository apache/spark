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

DESC FORMATTED t AS JSON;

DESC EXTENDED t;

DESC EXTENDED t AS JSON;

ALTER TABLE t UNSET TBLPROPERTIES (e);

DESC EXTENDED t;

ALTER TABLE t UNSET TBLPROPERTIES (comment);

DESC EXTENDED t;

DESC t PARTITION (c='Us', d=1);

DESC EXTENDED t PARTITION (c='Us', d=1) AS JSON;

DESC EXTENDED t PARTITION (c='Us', d=1);

DESC FORMATTED t PARTITION (c='Us', d=1);

DESC EXTENDED t PARTITION (C='Us', D=1);

DESC EXTENDED t PARTITION (C='Us', D=1) AS JSON;

-- NoSuchPartitionException: Partition not found in table
DESC t PARTITION (c='Us', d=2);

-- AnalysisException: Partition spec is invalid
DESC t PARTITION (c='Us');

-- ParseException: PARTITION specification is incomplete
DESC t PARTITION (c='Us', d);

-- DESC Temp View

DESC temp_v;

DESC EXTENDED temp_v AS JSON;

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

DESC FORMATTED v AS JSON;

DESC EXTENDED v;

-- AnalysisException DESC PARTITION is not allowed on a view
DESC v PARTITION (c='Us', d=1);

-- Explain Describe Table
EXPLAIN DESC t;
EXPLAIN DESC EXTENDED t AS JSON;
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

DESC TABLE EXTENDED d AS JSON;

DESC FORMATTED d;

-- Show column default values with newlines in the string
CREATE TABLE e (a STRING DEFAULT CONCAT('a\n b\n ', 'c\n d'), b INT DEFAULT 42) USING parquet COMMENT 'table_comment';

DESC e;

DESC EXTENDED e AS JSON;

DESC EXTENDED e;

DESC TABLE EXTENDED e;

DESC FORMATTED e;

DESC TABLE FORMATTED e AS JSON;

-- test DESCRIBE with clustering info
CREATE TABLE t2 (
    a STRING,
    b INT,
    c STRING,
    d STRING
)
USING parquet
OPTIONS (
    a '1',
    b '2',
    password 'password'
)
PARTITIONED BY (c, d)
CLUSTERED BY (a) SORTED BY (b ASC) INTO 2 BUCKETS
COMMENT 'table_comment'
TBLPROPERTIES (
    t 'test',
    password 'password'
);

DESC t2;

DESC FORMATTED t2 as json;

CREATE TABLE c (
  id STRING,
  nested_struct STRUCT<
    name: STRING,
    age: INT,
    contact: STRUCT<
      email: STRING,
      phone_numbers: ARRAY<STRING>,
      addresses: ARRAY<STRUCT<
        street: STRING,
        city: STRING,
        zip: INT
      >>
    >
  >,
  preferences MAP<STRING, ARRAY<STRING>>
) USING parquet
  OPTIONS (option1 'value1', option2 'value2')
  PARTITIONED BY (id)
  COMMENT 'A table with nested complex types'
  TBLPROPERTIES ('property1' = 'value1', 'password' = 'password');


DESC FORMATTED c AS JSON;

CREATE TABLE special_types_table (
  id STRING,
  salary DECIMAL(10, 2),
  short_description VARCHAR(255),
  char_code CHAR(10),
  timestamp_with_time_zone TIMESTAMP,
  timestamp_without_time_zone TIMESTAMP_NTZ,
  interval_year_to_month INTERVAL YEAR TO MONTH,
  interval_day_to_hour INTERVAL DAY TO HOUR,
  nested_struct STRUCT<
    detail: STRING,
    metrics: STRUCT<
      precision: DECIMAL(5, 2),
      scale: CHAR(5)
    >,
    time_info: STRUCT<
      timestamp_ltz: TIMESTAMP,
      timestamp_ntz: TIMESTAMP_NTZ
    >
  >,
  preferences MAP<STRING, ARRAY<VARCHAR(50)>>
) USING parquet
  OPTIONS ('compression' = 'snappy')
  COMMENT 'Table testing all special types'
  TBLPROPERTIES ('test_property' = 'test_value');

DESC FORMATTED special_types_table AS JSON;


