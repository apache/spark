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
