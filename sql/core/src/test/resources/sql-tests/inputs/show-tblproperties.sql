-- create a table with properties
CREATE TABLE tbl (a INT, b STRING, c INT) USING parquet
TBLPROPERTIES('p1'='v1', 'p2'='v2');

SHOW TBLPROPERTIES tbl;
SHOW TBLPROPERTIES tbl("p1");
SHOW TBLPROPERTIES tbl("p3");

DROP TABLE tbl;

-- create a view with properties
CREATE VIEW view TBLPROPERTIES('p1'='v1', 'p2'='v2') AS SELECT 1 AS c1;

SHOW TBLPROPERTIES view;
SHOW TBLPROPERTIES view("p1");
SHOW TBLPROPERTIES view("p3");

DROP VIEW view;

-- create a temporary view
CREATE TEMPORARY VIEW tv AS SELECT 1 AS c1;

-- Properties for a temporary view should be empty
SHOW TBLPROPERTIES tv;

DROP VIEW tv;
