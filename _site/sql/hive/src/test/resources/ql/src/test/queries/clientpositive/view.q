CREATE DATABASE db1;
USE db1;

CREATE TABLE table1 (key STRING, value STRING)
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/kv1.txt'
OVERWRITE INTO TABLE table1;

CREATE TABLE table2 (key STRING, value STRING)
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/kv1.txt'
OVERWRITE INTO TABLE table2;

-- relative reference, no alias
CREATE VIEW v1 AS SELECT * FROM table1;

-- relative reference, aliased
CREATE VIEW v2 AS SELECT t1.* FROM table1 t1;

-- relative reference, multiple tables
CREATE VIEW v3 AS SELECT t1.*, t2.key k FROM table1 t1 JOIN table2 t2 ON t1.key = t2.key;

-- absolute reference, no alias
CREATE VIEW v4 AS SELECT * FROM db1.table1;

-- absolute reference, aliased
CREATE VIEW v5 AS SELECT t1.* FROM db1.table1 t1;

-- absolute reference, multiple tables
CREATE VIEW v6 AS SELECT t1.*, t2.key k FROM db1.table1 t1 JOIN db1.table2 t2 ON t1.key = t2.key;

-- relative reference, explicit column
CREATE VIEW v7 AS SELECT key from table1;

-- absolute reference, explicit column
CREATE VIEW v8 AS SELECT key from db1.table1;

CREATE DATABASE db2;
USE db2;

SELECT * FROM db1.v1;
SELECT * FROM db1.v2;
SELECT * FROM db1.v3;
SELECT * FROM db1.v4;
SELECT * FROM db1.v5;
SELECT * FROM db1.v6;
SELECT * FROM db1.v7;
SELECT * FROM db1.v8;

