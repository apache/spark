-- simple
CREATE TABLE tbl (a INT, b STRING, c INT) USING parquet;

SHOW CREATE TABLE tbl;
DROP TABLE tbl;


-- options
CREATE TABLE tbl (a INT, b STRING, c INT) USING parquet
OPTIONS ('a' 1);

SHOW CREATE TABLE tbl;
DROP TABLE tbl;


-- path option
CREATE TABLE tbl (a INT, b STRING, c INT) USING parquet
OPTIONS ('path' '/path/to/table');

SHOW CREATE TABLE tbl;
DROP TABLE tbl;


-- location
CREATE TABLE tbl (a INT, b STRING, c INT) USING parquet
LOCATION '/path/to/table';

SHOW CREATE TABLE tbl;
DROP TABLE tbl;


-- partition by
CREATE TABLE tbl (a INT, b STRING, c INT) USING parquet
PARTITIONED BY (a);

SHOW CREATE TABLE tbl;
DROP TABLE tbl;


-- clustered by
CREATE TABLE tbl (a INT, b STRING, c INT) USING parquet
CLUSTERED BY (a) SORTED BY (b ASC) INTO 2 BUCKETS;

SHOW CREATE TABLE tbl;
DROP TABLE tbl;


-- comment
CREATE TABLE tbl (a INT, b STRING, c INT) USING parquet
COMMENT 'This is a comment';

SHOW CREATE TABLE tbl;
DROP TABLE tbl;


-- tblproperties
CREATE TABLE tbl (a INT, b STRING, c INT) USING parquet
TBLPROPERTIES ('a' = '1');

SHOW CREATE TABLE tbl;
DROP TABLE tbl;

-- float alias real and decimal alias numeric
CREATE TABLE tbl (a REAL, b NUMERIC, c NUMERIC(10), d NUMERIC(10,1)) USING parquet;
SHOW CREATE TABLE tbl;
DROP TABLE tbl;


-- show create table for view
CREATE TABLE tbl (a INT, b STRING, c INT) USING parquet;

-- simple
CREATE VIEW view_SPARK_30302 (aaa, bbb)
AS SELECT a, b FROM tbl;

SHOW CREATE TABLE view_SPARK_30302 AS SERDE;

SHOW CREATE TABLE view_SPARK_30302;

DROP VIEW view_SPARK_30302;


-- comment
CREATE VIEW view_SPARK_30302 (aaa COMMENT 'comment with \'quoted text\' for aaa', bbb)
COMMENT 'This is a comment with \'quoted text\' for view'
AS SELECT a, b FROM tbl;

SHOW CREATE TABLE view_SPARK_30302 AS SERDE;

SHOW CREATE TABLE view_SPARK_30302;

DROP VIEW view_SPARK_30302;


-- tblproperties
CREATE VIEW view_SPARK_30302 (aaa, bbb)
TBLPROPERTIES ('a' = '1', 'b' = '2')
AS SELECT a, b FROM tbl;

SHOW CREATE TABLE view_SPARK_30302 AS SERDE;

SHOW CREATE TABLE view_SPARK_30302;

DROP VIEW view_SPARK_30302;

DROP TABLE tbl;
