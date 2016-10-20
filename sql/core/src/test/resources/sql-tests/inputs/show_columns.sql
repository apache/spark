CREATE DATABASE showdb;

USE showdb;

CREATE TABLE showcolumn1 (col1 int, `col 2` int);
CREATE TABLE showcolumn2 (price int, qty int) partitioned by (year int, month int);
CREATE TEMPORARY VIEW showColumn3 (col3 int, `col 4` int) USING parquet;
CREATE GLOBAL TEMP VIEW showColumn4 AS SELECT 1 as col1, 'abc' as `col 5`;


-- only table name
SHOW COLUMNS IN showcolumn1;

-- qualified table name
SHOW COLUMNS IN showdb.showcolumn1;

-- table name and database name
SHOW COLUMNS IN showcolumn1 FROM showdb;

-- partitioned table
SHOW COLUMNS IN showcolumn2 IN showdb;

-- Non-existent table. Raise an error in this case
SHOW COLUMNS IN badtable FROM showdb;

-- database in table identifier and database name in different case
SHOW COLUMNS IN showdb.showcolumn1 from SHOWDB;

-- different database name in table identifier and database name.
-- Raise an error in this case.
SHOW COLUMNS IN showdb.showcolumn1 FROM baddb;

-- show column on temporary view
SHOW COLUMNS IN showcolumn3;

-- error temp view can't be qualified with a database
SHOW COLUMNS IN showdb.showcolumn3;

-- error temp view can't be qualified with a database
SHOW COLUMNS IN showcolumn3 FROM showdb;

-- error global temp view needs to be qualified
SHOW COLUMNS IN showcolumn4;

-- global temp view qualified with database
SHOW COLUMNS IN global_temp.showcolumn4;

-- global temp view qualified with database
SHOW COLUMNS IN showcolumn4 FROM global_temp;

DROP TABLE showcolumn1;
DROP TABLE showColumn2;
DROP VIEW  showcolumn3;
DROP VIEW  global_temp.showcolumn4;

use default;

DROP DATABASE showdb;
